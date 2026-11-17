// SPDX-FileCopyrightText: Szilárd Hajba
// SPDX-License-Identifier: MPL-2.0

//! FUSE frontend. One `Filesystem` impl serves every [`Mode`]:
//! - `off`: metadata from `RemoteTree` (in-memory, 10 s listing TTL), reads as ranged
//!   downloads;
//! - `on-demand`: metadata from `DbTree` (the metadata DB the `FeedPoller` keeps current),
//!   files hydrated into the `BlobCache` on `open` and read from there, so a cached file
//!   opens offline (an uncached one fails with `EIO`);
//! - `pinned`: served exactly like `on-demand`; the caller keeps every file hydrated
//!   (`core::pin`).
//!
//! Writes go through a local staging file (`write/`) in both modes. `off` uploads it on
//! close; `on-demand` journals every mutation (`Journal`) and updates the DB row at once, and
//! the caller's `SyncWorker` replays the journal.
//! Callbacks that may hit the network spawn a tokio task and reply from it; purely cached or
//! local answers reply inline.
#![forbid(unsafe_code)]

mod inode;
mod read;
#[cfg(test)]
mod test_ctx;
mod write;

use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::io;
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

use cloudillo_desktop_core::api::{Parent, VersionView};
use cloudillo_desktop_core::{
	BlobCache, Child, ChildKind, ClientError, Db, DbTree, FeedPoller, Journal, RemoteTree, Session,
	Uploader, ignore, journal, tree,
};
use fuser::{
	BackgroundSession, BsdFileFlags, Config, Errno, FileAttr, FileHandle, FileType, Filesystem,
	FopenFlags, Generation, INodeNo, LockOwner, MountOption, OpenAccMode, OpenFlags, RenameFlags,
	ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen,
	ReplyStatfs, ReplyWrite, ReplyXattr, Request, TimeOrNow, WriteFlags,
};

use crate::inode::{Inodes, ROOT_INO};
use crate::read::ReadAhead;
use crate::write::Staged;

/// Kernel attribute/entry cache lifetime (the tree's own listing TTL is 10 s).
const TTL: Duration = Duration::from_secs(1);
/// `O_TRUNC` (no `libc` dep; fuser exposes no constant for it).
// the asm-generic value (x86, arm, riscv); differs on alpha/sparc.
const O_TRUNC: i32 = 0o1000;

/// Virtual xattrs on regular files: the version list (read), restore in place / as a copy
/// (write the version's `file_id`).
const XATTR_VERSIONS: &str = "user.cloudillo.versions";
const XATTR_RESTORE: &str = "user.cloudillo.restore";
const XATTR_RESTORE_COPY: &str = "user.cloudillo.restore_copy";

/// Options for [`mount`].
#[derive(Debug, Clone)]
pub struct MountOptions {
	/// Staging dir for files being written (created 0700 if missing). A staged file is deleted
	/// only after its upload succeeded; failed ones stay here.
	pub staging: PathBuf,
	/// Shared with the caller, which awaits `idle()` before unmounting. A sealing uploader
	/// (encrypted staging) requires [`Mode::Off`].
	pub uploader: Arc<Uploader>,
	pub mode: Mode,
}

/// Cache mode of a mount.
#[derive(Debug, Clone)]
pub enum Mode {
	/// No local metadata or content.
	Off,
	/// Tree from the metadata DB, content hydrated into the blob cache on open.
	OnDemand(OnDemand),
	/// As `OnDemand`; the caller hydrates the whole mount (`pin::hydrate_pinned`).
	Pinned(OnDemand),
}

/// Handles of an `on-demand` mount; the caller runs `poller` (`FeedPoller::run`).
#[derive(Debug, Clone)]
pub struct OnDemand {
	pub db: Arc<Db>,
	pub cache: Arc<BlobCache>,
	/// Triggered after every local mutation.
	pub poller: Arc<FeedPoller>,
	/// Every mutation is journaled here; the caller runs its `SyncWorker`.
	pub journal: Arc<Journal>,
	/// Which new/changed files stay local instead of syncing.
	pub policy: Arc<ignore::Policy>,
}

impl OnDemand {
	/// Cached path of content `cid` of `entry`: its staged copy while journaled, else the
	/// blob cache (hydrating on a miss). An `ack` racing the staged copy away has already
	/// cached the landed content, so the fallback resolves `cid` and usually hits.
	async fn blob(&self, s: &Session, entry: &str, cid: &str) -> Result<PathBuf, ClientError> {
		if let Some(p) = self.journal.pending_blob(cid)
			&& let Ok(path) = self.cache.insert(cid, &p)
		{
			return Ok(path);
		}
		let entry = self.journal.resolve(entry)?;
		self.cache.hydrate(s, &entry, &self.journal.resolve(cid)?).await
	}
}

/// Metadata source of the mount's [`Mode`].
#[derive(Debug)]
enum Tree {
	Remote(RemoteTree),
	Db(DbTree, OnDemand),
}

impl Tree {
	fn parent_of(&self, node: &str) -> Parent {
		match self {
			Self::Remote(t) => t.parent_of(node),
			Self::Db(t, _) => t.parent_of(node),
		}
	}

	fn get(&self, id: &str) -> Option<Child> {
		match self {
			Self::Remote(t) => t.get(id),
			Self::Db(t, _) => t.get(id).unwrap_or_else(|e| {
				tracing::warn!("metadata db: {e}");
				None
			}),
		}
	}

	async fn children(&self, node: &str) -> cloudillo_desktop_core::Result<Vec<Child>> {
		match self {
			Self::Remote(t) => t.children(node).await,
			Self::Db(t, _) => t.children(node),
		}
	}

	/// After a mutation in `node`: `off` drops the cached listing, `on-demand` asks the feed
	/// for an immediate poll.
	fn invalidate(&self, node: &str) {
		match self {
			Self::Remote(t) => t.invalidate(node),
			Self::Db(_, od) => od.poller.trigger(),
		}
	}

	fn on_demand(&self) -> Option<&OnDemand> {
		match self {
			Self::Db(_, od) => Some(od),
			Self::Remote(_) => None,
		}
	}
}

/// Mount `session`'s paired target at `mountpoint` in the background; dropping the returned
/// session unmounts. Must be called inside a tokio runtime (its handle runs the network tasks).
pub fn mount(
	session: Arc<Session>,
	mountpoint: &Path,
	opts: MountOptions,
) -> io::Result<(BackgroundSession, Writes)> {
	let rt = tokio::runtime::Handle::try_current().map_err(io::Error::other)?;
	if opts.uploader.seal().is_some() && !matches!(opts.mode, Mode::Off) {
		return Err(io::Error::other("sealed staging needs Mode::Off"));
	}
	std::fs::DirBuilder::new().recursive(true).mode(0o700).create(&opts.staging)?;
	let meta = std::fs::metadata(mountpoint)?;
	let owner = Owner {
		uid: meta.uid(),
		gid: meta.gid(),
		read_only: session.credentials().key.access == "R",
		mounted: SystemTime::now(),
	};
	let (tree, inodes) = match opts.mode {
		Mode::Off => (Tree::Remote(RemoteTree::new(Arc::clone(&session))), Inodes::default()),
		Mode::OnDemand(od) | Mode::Pinned(od) => {
			let db_tree = DbTree::new(&session, Arc::clone(&od.db)).map_err(io::Error::other)?;
			let inodes = Inodes::new(Some(Arc::clone(&od.db)));
			(Tree::Db(db_tree, od), inodes)
		}
	};
	let ctx = Ctx {
		tree,
		session,
		inodes,
		uploader: opts.uploader,
		staging: opts.staging,
		staged: Mutex::default(),
		stage_lock: tokio::sync::Mutex::default(),
		seq: AtomicU64::new(1),
		dead: AtomicBool::new(false),
		owner,
		handles: Mutex::default(),
		next_fh: AtomicU64::new(1),
	};
	let ctx = Arc::new(ctx);
	let mut cfg = Config::default();
	cfg.mount_options =
		vec![MountOption::FSName("cloudillo".to_owned()), MountOption::DefaultPermissions];
	// auto_unmount releases the mountpoint after a crash; it needs allow_other in fusermount,
	// which RootAndOwner keeps limited to the owner (and root) in-process.
	let mut auto = cfg.clone();
	auto.mount_options.push(MountOption::AutoUnmount);
	auto.acl = fuser::SessionACL::RootAndOwner;
	let fs = || CloudilloFs { ctx: Arc::clone(&ctx), rt: rt.clone() };
	let bg = fuser::spawn_mount(fs(), mountpoint, &auto).or_else(|e| {
		tracing::info!(
			"auto_unmount unavailable ({e}; needs user_allow_other in /etc/fuse.conf); after a \
			 crash run fusermount -u {}",
			mountpoint.display()
		);
		fuser::spawn_mount(fs(), mountpoint, &cfg)
	})?;
	Ok((bg, Writes(ctx)))
}

/// Write state of a running mount, for the caller's shutdown.
#[derive(Clone)]
pub struct Writes(Arc<Ctx>);

impl Writes {
	/// Local names of files whose last upload failed.
	// names only; add the parent path if users can't find them.
	#[must_use]
	pub fn failed(&self) -> Vec<String> {
		write::failed(&self.0).into_iter().map(|(_, n)| n).collect()
	}

	/// Retry every failed upload once.
	pub async fn retry(&self) {
		write::retry(&self.0).await;
	}

	/// After unmount: journal / upload the writes still debounced or held, then wait for them.
	pub async fn finish(&self) {
		write::finish(&self.0).await;
	}

	/// Writes not journaled / uploaded yet (debounced, held, or still open).
	#[must_use]
	pub fn pending(&self) -> usize {
		write::pending(&self.0)
	}
}

/// One open file handle (`fh`). Write state lives per inode in `Ctx::staged`.
#[derive(Debug)]
pub(crate) struct Handle {
	/// Node id at open time (entry id, or `#new:<n>` for a file not uploaded yet).
	pub node: String,
	/// Launcher JSON, served from memory instead of downloaded.
	pub launcher: Option<Vec<u8>>,
	pub ahead: ReadAhead,
	/// Opened for writing: counted in its inode's `Staged::writers`.
	pub write: bool,
	/// `on-demand`: the hydrated blob and its content id (kept from eviction while open).
	pub blob: Option<(PathBuf, String)>,
}

/// Ownership and mode inputs for attributes; `Copy` so spawned tasks can carry it.
#[derive(Debug, Clone, Copy)]
struct Owner {
	uid: u32,
	gid: u32,
	/// Paired with access `R`: every file is 0444 and every mutation `EROFS`.
	read_only: bool,
	/// mtime of the mount root.
	mounted: SystemTime,
}

impl Owner {
	/// `child == None` is the mount root.
	fn attr(self, ino: u64, child: Option<&Child>) -> FileAttr {
		match child {
			None => self.raw(ino, FileType::Directory, 0o755, 0, self.mounted),
			Some(c) => match c.kind {
				ChildKind::Folder | ChildKind::RoomFolder => {
					self.raw(ino, FileType::Directory, 0o755, 0, c.mtime)
				}
				ChildKind::Launcher { .. } => {
					self.raw(ino, FileType::RegularFile, 0o444, c.size, c.mtime)
				}
				ChildKind::File => self.file(ino, c.size, c.mtime),
			},
		}
	}

	fn file(self, ino: u64, size: u64, mtime: SystemTime) -> FileAttr {
		let perm = if self.read_only { 0o444 } else { 0o644 };
		self.raw(ino, FileType::RegularFile, perm, size, mtime)
	}

	fn raw(self, ino: u64, kind: FileType, perm: u16, size: u64, mtime: SystemTime) -> FileAttr {
		let dir = matches!(kind, FileType::Directory);
		FileAttr {
			ino: INodeNo(ino),
			size,
			blocks: size.div_ceil(512),
			atime: mtime,
			mtime,
			ctime: mtime,
			crtime: mtime,
			kind,
			perm,
			nlink: if dir { 2 } else { 1 },
			uid: self.uid,
			gid: self.gid,
			rdev: 0,
			blksize: 4096,
			flags: 0,
		}
	}
}

fn file_type(kind: &ChildKind) -> FileType {
	match kind {
		ChildKind::Folder | ChildKind::RoomFolder => FileType::Directory,
		ChildKind::File | ChildKind::Launcher { .. } => FileType::RegularFile,
	}
}

/// Error of a callback: a direct errno, or a `ClientError` still to be mapped by
/// [`Ctx::errno`]. `?` converts from all three sources.
#[derive(Debug)]
pub(crate) enum Fail {
	Errno(Errno),
	Client(ClientError),
}

impl From<Errno> for Fail {
	fn from(e: Errno) -> Self {
		Self::Errno(e)
	}
}

impl From<ClientError> for Fail {
	fn from(e: ClientError) -> Self {
		Self::Client(e)
	}
}

impl From<io::Error> for Fail {
	fn from(e: io::Error) -> Self {
		Self::Client(ClientError::Io(e))
	}
}

/// State shared by the callbacks and their spawned tasks.
#[derive(Debug)]
pub(crate) struct Ctx {
	session: Arc<Session>,
	tree: Tree,
	inodes: Inodes,
	uploader: Arc<Uploader>,
	staging: PathBuf,
	/// Per-inode write state (see `write/`); never held across `.await`.
	staged: Mutex<HashMap<u64, Staged>>,
	/// Serializes staging downloads.
	stage_lock: tokio::sync::Mutex<()>,
	/// Staging file name prefix.
	seq: AtomicU64,
	/// The key was rejected by the probe too: no more writes (`EACCES`) until re-paired.
	dead: AtomicBool,
	owner: Owner,
	handles: Mutex<HashMap<u64, Arc<Handle>>>,
	next_fh: AtomicU64,
}

impl Ctx {
	fn handles(&self) -> MutexGuard<'_, HashMap<u64, Arc<Handle>>> {
		cloudillo_desktop_core::lock(&self.handles)
	}

	fn new_handle(&self, handle: Handle) -> FileHandle {
		let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
		self.handles().insert(fh, Arc::new(handle));
		FileHandle(fh)
	}

	/// `on-demand`: apply a local mutation to the metadata DB and ask the feed for an
	/// immediate poll. DB errors are logged only: the next feed round repairs the row.
	/// No-op in `off` mode.
	fn record(&self, op: &str, f: impl FnOnce(&OnDemand) -> cloudillo_desktop_core::Result<()>) {
		let Some(od) = self.tree.on_demand() else {
			return;
		};
		if let Err(e) = f(od) {
			tracing::warn!("{op}: metadata db: {e}");
		}
		od.poller.trigger();
	}

	/// `on-demand`: shrink the blob cache to its limit, keeping blobs of open handles.
	fn evict(&self) {
		let Some(od) = self.tree.on_demand() else {
			return;
		};
		let open: HashSet<String> = self
			.handles()
			.values()
			.filter_map(|h| h.blob.as_ref().map(|b| b.1.clone()))
			.collect();
		if let Err(e) = od.cache.evict_to_fit(&open) {
			tracing::warn!("blob cache eviction: {e}");
		}
	}

	/// The one `ClientError` -> errno mapping:
	/// `NotFound` -> ENOENT, `TooLarge` -> EFBIG, `QuotaExceeded` -> EDQUOT,
	/// 403 `…CROSSROOM` -> EXDEV, any other 403 -> probe `GET /api/files?limit=1` on the mount
	/// root (403 again: dead key, writes stop) then EACCES, 401 -> EACCES, anything else -> EIO.
	async fn errno(&self, op: &str, fail: Fail) -> Errno {
		let err = match fail {
			Fail::Errno(e) => return e,
			Fail::Client(e) => e,
		};
		match &err {
			ClientError::NotFound => Errno::ENOENT,
			ClientError::TooLarge => {
				tracing::warn!("{op}: too large");
				Errno::EFBIG
			}
			ClientError::QuotaExceeded => {
				tracing::warn!("{op}: quota exceeded");
				Errno::EDQUOT
			}
			ClientError::Forbidden { code } if code.ends_with("CROSSROOM") => Errno::EXDEV,
			ClientError::Forbidden { .. } => {
				if matches!(self.session.key_dead(&self.tree.parent_of(tree::ROOT)).await, Ok(true))
				{
					self.dead.store(true, Ordering::Relaxed);
					tracing::error!(
						"{op}: the API key was rejected: re-pair needed. Writes are disabled; \
						 staged files are kept in {}",
						self.staging.display()
					);
				} else {
					tracing::warn!("{op}: {err}");
				}
				Errno::EACCES
			}
			ClientError::Unauthorized => {
				tracing::warn!("{op}: {err}");
				Errno::EACCES
			}
			_ => {
				tracing::warn!("{op}: {err}");
				Errno::EIO
			}
		}
	}

	/// Gate for every mutation: access `R` -> EROFS, dead key -> EACCES.
	fn writable(&self) -> Result<(), Errno> {
		if self.owner.read_only {
			Err(Errno::EROFS)
		} else if self.dead.load(Ordering::Relaxed) {
			Err(Errno::EACCES)
		} else {
			Ok(())
		}
	}

	/// Node id of a directory inode; ENOTDIR for files, ENOENT for unknown inodes.
	fn dir_node(&self, ino: u64) -> Result<String, Errno> {
		let id = self.node(ino).ok_or(Errno::ENOENT)?;
		if id == tree::ROOT {
			return Ok(id);
		}
		match self.tree.get(&id).map(|c| c.kind) {
			Some(ChildKind::Folder | ChildKind::RoomFolder) => Ok(id),
			Some(_) => Err(Errno::ENOTDIR),
			None => Err(Errno::ENOENT),
		}
	}

	/// Attributes from the staging file if `ino` has one, else from the tree.
	fn attr_of(&self, ino: u64) -> Option<FileAttr> {
		if let Some(a) = write::staged_attr(self, ino) {
			return Some(a);
		}
		let id = self.node(ino)?;
		if id == tree::ROOT {
			return Some(self.owner.attr(ino, None));
		}
		self.tree.get(&id).map(|c| self.owner.attr(ino, Some(&c)))
	}

	/// The server-side file behind `ino` for the version xattrs: ENOENT unknown inode,
	/// `not_file` for folders / the root, EAGAIN while not uploaded yet (`local:` id).
	fn version_target(&self, ino: u64, not_file: Errno) -> Result<Child, Errno> {
		let id = self.node(ino).ok_or(Errno::ENOENT)?;
		let child = self.tree.get(&id).filter(|c| c.kind == ChildKind::File).ok_or(not_file)?;
		if id.starts_with(journal::LOCAL_PREFIX) {
			return Err(Errno::EAGAIN);
		}
		Ok(child)
	}

	/// Node id of `ino`. `on-demand`: a `local:<seq>` id whose create the sync worker has
	/// acknowledged meanwhile is rebound to the real entry id.
	fn node(&self, ino: u64) -> Option<String> {
		let id = self.inodes.id(ino)?;
		if let Some(od) = self.tree.on_demand()
			&& id.starts_with(journal::LOCAL_PREFIX)
		{
			match od.journal.resolve(&id) {
				Ok(real) if real != id => {
					self.inodes.rebind(ino, &real);
					return Some(real);
				}
				Ok(_) => {}
				Err(e) => tracing::warn!("journal: {e}"),
			}
		}
		Some(id)
	}

	/// `user.cloudillo.versions` of `ino` ([`version_lines`]).
	async fn versions_xattr(&self, ino: u64) -> Result<String, Errno> {
		let child = self.version_target(ino, Errno::ENODATA)?;
		let id = child.entry_id.as_str();
		match self.session.call(|api| async move { api.list_versions(id).await }).await {
			Ok(list) => Ok(version_lines(&list)),
			Err(e) => Err(self.errno("getxattr", e.into()).await),
		}
	}

	/// Restore version `file_id` of `ino` in place, or `as_copy` as a new sibling. In place
	/// never overtakes a local edit not landed yet (EAGAIN), nor a changed remote (412: EAGAIN).
	async fn restore_xattr(&self, ino: u64, as_copy: bool, file_id: String) -> Result<(), Errno> {
		self.writable()?;
		let child = self.version_target(ino, Errno::ENOTSUP)?;
		let id = child.entry_id.as_str();
		if !as_copy {
			if write::is_staged(self, ino) {
				return Err(Errno::EAGAIN);
			}
			if let Some(od) = self.tree.on_demand() {
				if let Err(f) = write::fire_held(self, od, Some(id)) {
					return Err(self.errno("setxattr", f).await);
				}
				match od.journal.has_pending(id) {
					Ok(false) => {}
					Ok(true) => return Err(Errno::EAGAIN),
					Err(e) => return Err(self.errno("setxattr", e.into()).await),
				}
			}
		}
		let parent = self
			.inodes
			.parent(ino)
			.and_then(|p| self.node(p))
			.unwrap_or_else(|| tree::ROOT.to_owned());
		let (fid, current) = (file_id.as_str(), child.content_id.as_deref());
		let res = if as_copy {
			self.session
				.call(|api| async move { api.duplicate_from_version(id, fid).await })
				.await
		} else {
			self.session
				.call(|api| async move { api.restore_version(id, fid, current).await })
				.await
		};
		match res {
			Ok(_) => {
				self.tree.invalidate(&parent);
				Ok(())
			}
			Err(ClientError::PreconditionFailed) => Err(Errno::EAGAIN),
			Err(e) => Err(self.errno("setxattr", e.into()).await),
		}
	}
}

pub struct CloudilloFs {
	ctx: Arc<Ctx>,
	rt: tokio::runtime::Handle,
}

/// `user.cloudillo.versions` value: `{file_id}\t{replaced_at}\t{size|-}\t{replaced_by|-}` per
/// line; empty without versions.
fn version_lines(list: &[VersionView]) -> String {
	// server text must not break the line / field structure
	let clean =
		|s: &str| -> String { s.chars().map(|c| if c.is_control() { ' ' } else { c }).collect() };
	let line = |v: &VersionView| {
		let size = v.size.map_or_else(|| "-".to_owned(), |s| s.to_string());
		let by = clean(v.replaced_by.as_deref().unwrap_or("-"));
		format!("{}\t{}\t{size}\t{by}", clean(&v.file_id), clean(&v.replaced_at))
	};
	list.iter().map(line).collect::<Vec<_>>().join("\n")
}

/// xattr size probe: `size == 0` asks for the length (`Some`), a too small buffer is ERANGE,
/// `None` = send the data.
fn fit(len: usize, size: u32) -> Result<Option<u32>, Errno> {
	let n = u32::try_from(len).unwrap_or(u32::MAX);
	match size {
		0 => Ok(Some(n)),
		s if s < n => Err(Errno::ERANGE),
		_ => Ok(None),
	}
}

/// Non-UTF-8 names cannot be sent to the server.
fn utf8(name: &OsStr) -> Result<String, Errno> {
	name.to_str().map(str::to_owned).ok_or(Errno::EINVAL)
}

impl Filesystem for CloudilloFs {
	fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
		let dir = match self.ctx.dir_node(parent.0) {
			Ok(d) => d,
			Err(e) => return reply.error(e),
		};
		let Some(name) = name.to_str().map(str::to_owned) else {
			return reply.error(Errno::ENOENT);
		};
		if let Some(attr) =
			write::staged_lookup(&self.ctx, parent.0, &name).and_then(|ino| self.ctx.attr_of(ino))
		{
			return reply.entry(&TTL, &attr, Generation(0));
		}
		let ctx = Arc::clone(&self.ctx);
		// spawned even on a fresh listing (RemoteTree has no sync cached-listing
		// accessor); add one to tree.rs if the task hop ever shows up in latency.
		self.rt.spawn(async move {
			match ctx.tree.children(&dir).await {
				Ok(list) => match list.into_iter().find(|c| c.local_name == name) {
					Some(c) => {
						let ino = ctx.inodes.ino_for(&c.entry_id, parent.0);
						let attr =
							ctx.attr_of(ino).unwrap_or_else(|| ctx.owner.attr(ino, Some(&c)));
						reply.entry(&TTL, &attr, Generation(0));
					}
					None => reply.error(Errno::ENOENT),
				},
				Err(e) => reply.error(ctx.errno("lookup", e.into()).await),
			}
		});
	}

	fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
		match self.ctx.attr_of(ino.0) {
			Some(a) => reply.attr(&TTL, &a),
			None => reply.error(Errno::ENOENT),
		}
	}

	/// Only `size` (truncate) is applied; mode/owner/time changes are accepted and dropped.
	fn setattr(
		&self,
		_req: &Request,
		ino: INodeNo,
		_mode: Option<u32>,
		_uid: Option<u32>,
		_gid: Option<u32>,
		size: Option<u64>,
		_atime: Option<TimeOrNow>,
		_mtime: Option<TimeOrNow>,
		_ctime: Option<SystemTime>,
		_fh: Option<FileHandle>,
		_crtime: Option<SystemTime>,
		_chgtime: Option<SystemTime>,
		_bkuptime: Option<SystemTime>,
		_flags: Option<BsdFileFlags>,
		reply: ReplyAttr,
	) {
		let Some(size) = size else {
			return match self.ctx.attr_of(ino.0) {
				Some(a) => reply.attr(&TTL, &a),
				None => reply.error(Errno::ENOENT),
			};
		};
		if let Err(e) = self.ctx.writable() {
			return reply.error(e);
		}
		let ctx = Arc::clone(&self.ctx);
		self.rt.spawn(async move {
			match write::truncate(&ctx, ino.0, size).await {
				Ok(a) => reply.attr(&TTL, &a),
				Err(f) => reply.error(ctx.errno("truncate", f).await),
			}
		});
	}

	fn mkdir(
		&self,
		_req: &Request,
		parent: INodeNo,
		name: &OsStr,
		_mode: u32,
		_umask: u32,
		reply: ReplyEntry,
	) {
		let name = match self.ctx.writable().and_then(|()| utf8(name)) {
			Ok(n) => n,
			Err(e) => return reply.error(e),
		};
		let ctx = Arc::clone(&self.ctx);
		self.rt.spawn(async move {
			match write::mkdir(&ctx, parent.0, &name).await {
				Ok(a) => reply.entry(&TTL, &a, Generation(0)),
				Err(f) => reply.error(ctx.errno("mkdir", f).await),
			}
		});
	}

	fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
		let name = match self.ctx.writable().and_then(|()| utf8(name)) {
			Ok(n) => n,
			Err(e) => return reply.error(e),
		};
		let ctx = Arc::clone(&self.ctx);
		self.rt.spawn(async move {
			match write::unlink(&ctx, parent.0, &name).await {
				Ok(()) => reply.ok(),
				Err(f) => reply.error(ctx.errno("unlink", f).await),
			}
		});
	}

	fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
		let name = match self.ctx.writable().and_then(|()| utf8(name)) {
			Ok(n) => n,
			Err(e) => return reply.error(e),
		};
		let ctx = Arc::clone(&self.ctx);
		self.rt.spawn(async move {
			match write::rmdir(&ctx, parent.0, &name).await {
				Ok(()) => reply.ok(),
				Err(f) => reply.error(ctx.errno("rmdir", f).await),
			}
		});
	}

	fn rename(
		&self,
		_req: &Request,
		parent: INodeNo,
		name: &OsStr,
		newparent: INodeNo,
		newname: &OsStr,
		flags: RenameFlags,
		reply: ReplyEmpty,
	) {
		let names = self.ctx.writable().and_then(|()| Ok((utf8(name)?, utf8(newname)?)));
		let (name, newname) = match names {
			Ok(n) => n,
			Err(e) => return reply.error(e),
		};
		let ctx = Arc::clone(&self.ctx);
		self.rt.spawn(async move {
			match write::rename(&ctx, (parent.0, &name), (newparent.0, &newname), flags).await {
				Ok(()) => reply.ok(),
				Err(f) => reply.error(ctx.errno("rename", f).await),
			}
		});
	}

	fn readdir(
		&self,
		_req: &Request,
		ino: INodeNo,
		_fh: FileHandle,
		offset: u64,
		mut reply: ReplyDirectory,
	) {
		let dir = match self.ctx.dir_node(ino.0) {
			Ok(d) => d,
			Err(e) => return reply.error(e),
		};
		let up = self.ctx.inodes.parent(ino.0).unwrap_or(ROOT_INO);
		let ctx = Arc::clone(&self.ctx);
		self.rt.spawn(async move {
			let list = match ctx.tree.children(&dir).await {
				Ok(l) => l,
				Err(e) => return reply.error(ctx.errno("readdir", e.into()).await),
			};
			let dots = [
				(ino.0, FileType::Directory, ".".to_owned()),
				(up, FileType::Directory, "..".to_owned()),
			];
			let remote = list.into_iter().map(|c| {
				(ctx.inodes.ino_for(&c.entry_id, ino.0), file_type(&c.kind), c.local_name)
			});
			// New files not uploaded yet come last.
			let staged = write::staged_new_in(&ctx, ino.0)
				.into_iter()
				.map(|(n, name)| (n, FileType::RegularFile, name));
			let skip = usize::try_from(offset).unwrap_or(usize::MAX);
			for (i, (n, kind, name)) in
				dots.into_iter().chain(remote).chain(staged).enumerate().skip(skip)
			{
				// The offset handed back is that of the next entry.
				if reply.add(INodeNo(n), i as u64 + 1, kind, name) {
					break;
				}
			}
			reply.ok();
		});
	}

	fn create(
		&self,
		_req: &Request,
		parent: INodeNo,
		name: &OsStr,
		_mode: u32,
		_umask: u32,
		_flags: i32,
		reply: ReplyCreate,
	) {
		let created = self
			.ctx
			.writable()
			.and_then(|()| self.ctx.dir_node(parent.0))
			.and_then(|_| utf8(name))
			.and_then(|name| write::create(&self.ctx, parent.0, &name));
		match created {
			Ok((node, attr)) => {
				let handle = Handle {
					node,
					launcher: None,
					ahead: ReadAhead::default(),
					write: true,
					blob: None,
				};
				let fh = self.ctx.new_handle(handle);
				reply.created(&TTL, &attr, Generation(0), fh, FopenFlags::empty());
			}
			Err(e) => reply.error(e),
		}
	}

	fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
		let ctx = &self.ctx;
		let Some(node) = ctx.node(ino.0) else {
			return reply.error(Errno::ENOENT);
		};
		let launcher = if write::is_staged(ctx, ino.0) {
			None
		} else {
			match ctx.tree.get(&node).map(|c| c.kind) {
				Some(ChildKind::File) => None,
				Some(ChildKind::Launcher { json }) => Some(json),
				Some(_) => return reply.error(Errno::EISDIR),
				None if node == tree::ROOT => return reply.error(Errno::EISDIR),
				None => return reply.error(Errno::ENOENT),
			}
		};
		let write = !matches!(flags.acc_mode(), OpenAccMode::O_RDONLY);
		let trunc = write && flags.0 & O_TRUNC != 0;
		if write {
			if launcher.is_some() {
				return reply.error(Errno::EACCES);
			}
			if let Err(e) = ctx.writable() {
				return reply.error(e);
			}
		}
		// `on-demand`: a file whose content is not staged is hydrated first. Not for `O_WRONLY`:
		// without `atomic_o_trunc` the kernel sends `O_TRUNC` as a later `setattr(0)`, so
		// `> file` offline must not need the old content; a first write stages it if needed.
		let wronly = matches!(flags.acc_mode(), OpenAccMode::O_WRONLY);
		let cid = match ctx.tree.on_demand() {
			Some(_)
				if launcher.is_none() && !trunc && !wronly && !write::has_staging(ctx, ino.0) =>
			{
				ctx.tree.get(&node).and_then(|c| c.content_id)
			}
			_ => None,
		};
		let Some(cid) = cid else {
			if write && let Err(e) = write::open_writer(ctx, ino.0, &node, trunc) {
				return reply.error(e);
			}
			let handle = Handle { node, launcher, ahead: ReadAhead::default(), write, blob: None };
			return reply.opened(ctx.new_handle(handle), FopenFlags::empty());
		};
		let ctx = Arc::clone(&self.ctx);
		self.rt.spawn(async move {
			let Some(od) = ctx.tree.on_demand() else {
				return reply.error(Errno::EIO);
			};
			// A cached blob needs no network; an uncached one offline maps to EIO. Content
			// still in the journal is served from its staged copy.
			let path = match od.blob(&ctx.session, &node, &cid).await {
				Ok(p) => p,
				Err(e) => return reply.error(ctx.errno("open", e.into()).await),
			};
			if write && let Err(e) = write::open_writer(&ctx, ino.0, &node, false) {
				return reply.error(e);
			}
			let blob = Some((path, cid));
			let ahead = ReadAhead::default();
			let fh = ctx.new_handle(Handle { node, launcher: None, ahead, write, blob });
			ctx.evict();
			reply.opened(fh, FopenFlags::empty());
		});
	}

	fn read(
		&self,
		_req: &Request,
		ino: INodeNo,
		fh: FileHandle,
		offset: u64,
		size: u32,
		_flags: OpenFlags,
		_lock_owner: Option<LockOwner>,
		reply: ReplyData,
	) {
		let Some(h) = self.ctx.handles().get(&fh.0).cloned() else {
			return reply.error(Errno::EBADF);
		};
		if let Some(json) = &h.launcher {
			let from = usize::try_from(offset).unwrap_or(usize::MAX).min(json.len());
			let to =
				from.saturating_add(usize::try_from(size).unwrap_or(usize::MAX)).min(json.len());
			return reply.data(json.get(from..to).unwrap_or_default());
		}
		match write::read_staged(&self.ctx, ino.0, offset, size) {
			Some(Ok(data)) => return reply.data(&data),
			Some(Err(e)) => return reply.error(write::eio("read", &e)),
			None => {}
		}
		if let Some((path, _)) = &h.blob {
			return match read::read_at(path, offset, size) {
				Ok(data) => reply.data(&data),
				Err(e) => {
					tracing::warn!("read: cached blob {}: {e}", path.display());
					reply.error(Errno::EIO);
				}
			};
		}
		let ctx = Arc::clone(&self.ctx);
		self.rt.spawn(async move {
			match h.ahead.read(&ctx.session, &h.node, offset, size).await {
				Ok(data) => reply.data(&data),
				Err(e) => reply.error(ctx.errno("read", e.into()).await),
			}
		});
	}

	fn write(
		&self,
		_req: &Request,
		ino: INodeNo,
		fh: FileHandle,
		offset: u64,
		data: &[u8],
		_write_flags: WriteFlags,
		_flags: OpenFlags,
		_lock_owner: Option<LockOwner>,
		reply: ReplyWrite,
	) {
		if !self.ctx.handles().get(&fh.0).is_some_and(|h| h.write) {
			return reply.error(Errno::EBADF);
		}
		match write::write_staged(&self.ctx, ino.0, offset, data) {
			Some(Ok(n)) => return reply.written(n),
			Some(Err(e)) => return reply.error(write::eio("write", &e)),
			None => {}
		}
		// First write of an existing file: download it into a staging file first.
		let (ctx, data) = (Arc::clone(&self.ctx), data.to_vec());
		self.rt.spawn(async move {
			if let Err(f) = write::stage(&ctx, ino.0).await {
				return reply.error(ctx.errno("write", f).await);
			}
			match write::write_staged(&ctx, ino.0, offset, &data) {
				Some(Ok(n)) => reply.written(n),
				Some(Err(e)) => reply.error(write::eio("write", &e)),
				None => reply.error(Errno::EIO),
			}
		});
	}

	/// Uploads happen on the last writer's `release` (or `fsync`), not on every `close`.
	fn flush(
		&self,
		_req: &Request,
		_ino: INodeNo,
		_fh: FileHandle,
		_lock_owner: LockOwner,
		reply: ReplyEmpty,
	) {
		reply.ok();
	}

	/// Uploads the staged content now (the file stays open).
	fn fsync(
		&self,
		_req: &Request,
		ino: INodeNo,
		fh: FileHandle,
		_datasync: bool,
		reply: ReplyEmpty,
	) {
		if !self.ctx.handles().get(&fh.0).is_some_and(|h| h.write) {
			return reply.ok();
		}
		let ctx = Arc::clone(&self.ctx);
		self.rt.spawn(async move {
			match write::commit(&ctx, ino.0).await {
				Ok(()) => reply.ok(),
				Err(f) => reply.error(ctx.errno("fsync", f).await),
			}
		});
	}

	fn release(
		&self,
		_req: &Request,
		ino: INodeNo,
		fh: FileHandle,
		_flags: OpenFlags,
		_lock_owner: Option<LockOwner>,
		_flush: bool,
		reply: ReplyEmpty,
	) {
		let wrote = self.ctx.handles().remove(&fh.0).is_some_and(|h| h.write);
		// Decremented inline (not in the task), so a rename right after close() sees the
		// pending upload. Errors here never reach close(); they are logged.
		if !wrote || !write::release_writer(&self.ctx, ino.0) {
			return reply.ok();
		}
		let ctx = Arc::clone(&self.ctx);
		self.rt.spawn(async move {
			match write::commit(&ctx, ino.0).await {
				Ok(()) => reply.ok(),
				Err(f) => reply.error(ctx.errno("upload", f).await),
			}
		});
	}

	fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
		// No quota figures: report an empty filesystem.
		reply.statfs(0, 0, 0, 0, 0, 4096, 255, 4096);
	}

	fn getxattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, size: u32, reply: ReplyXattr) {
		if name != XATTR_VERSIONS {
			return reply.error(Errno::ENODATA);
		}
		let ctx = Arc::clone(&self.ctx);
		self.rt.spawn(async move {
			match ctx.versions_xattr(ino.0).await.and_then(|t| Ok((fit(t.len(), size)?, t))) {
				Ok((Some(n), _)) => reply.size(n),
				Ok((None, text)) => reply.data(text.as_bytes()),
				Err(e) => reply.error(e),
			}
		});
	}

	fn setxattr(
		&self,
		_req: &Request,
		ino: INodeNo,
		name: &OsStr,
		value: &[u8],
		_flags: i32,
		_position: u32,
		reply: ReplyEmpty,
	) {
		let as_copy = match name.to_str() {
			Some(XATTR_RESTORE) => false,
			Some(XATTR_RESTORE_COPY) => true,
			_ => return reply.error(Errno::ENOTSUP),
		};
		let Some(file_id) = std::str::from_utf8(value).ok().filter(|v| !v.is_empty()) else {
			return reply.error(Errno::EINVAL);
		};
		let file_id = file_id.to_owned();
		let ctx = Arc::clone(&self.ctx);
		self.rt.spawn(async move {
			match ctx.restore_xattr(ino.0, as_copy, file_id).await {
				Ok(()) => reply.ok(),
				Err(e) => reply.error(e),
			}
		});
	}
}

#[cfg(test)]
mod tests {
	use cloudillo_desktop_core::stub::Log;

	use super::*;
	use crate::test_ctx::{T, ctx, not_found, od, track};

	#[test]
	fn version_lines_format() {
		let v = |id: &str, size, by: Option<&str>| VersionView {
			file_id: id.to_owned(),
			replaced_at: "2026-11-16T10:00:00Z".to_owned(),
			replaced_by: by.map(str::to_owned),
			size,
		};
		let list = [
			v("f1", Some(12), Some("alice")),
			v("f2", None, None),
			v("f3", None, Some("a\tb\nc\x1b")),
		];
		assert_eq!(
			version_lines(&list),
			"f1\t2026-11-16T10:00:00Z\t12\talice\nf2\t2026-11-16T10:00:00Z\t-\t-\n\
			 f3\t2026-11-16T10:00:00Z\t-\ta b c "
		);
		assert_eq!(version_lines(&[]), "");
	}

	#[test]
	fn fit_probe() {
		assert_eq!(fit(5, 0), Ok(Some(5)));
		assert_eq!(fit(5, 4), Err(Errno::ERANGE));
		assert_eq!(fit(5, 5), Ok(None));
	}

	fn data(json: &str) -> (u16, String) {
		(200, format!(r#"{{"data":{json}}}"#))
	}

	fn restored(l: &str) -> (u16, String) {
		if l.starts_with("POST /api/files/E1/versions/v1/restore") {
			data(r#"{"id":"E1","fileName":"f"}"#)
		} else if l.starts_with("GET /api/files/E1/versions") {
			data(r#"[{"fileId":"v1","replacedAt":"2026-11-16T10:00:00Z","size":3}]"#)
		} else {
			(404, "{}".into())
		}
	}

	/// `ino` of the tracked root file `E1`.
	fn e1(t: &T) -> u64 {
		track(&t.ctx, "E1", "f");
		t.ctx.inodes.ino_for("E1", ROOT_INO)
	}

	fn logged(log: &Log, needle: &str) -> bool {
		log.lock().unwrap().iter().any(|h| h.contains(needle))
	}

	#[tokio::test]
	async fn blob_after_ack_uses_landed_content() {
		let (t, _) = ctx("x-blob-ack", not_found).await;
		let od = od(&t.ctx);
		let src = t.tmp.join("src");
		std::fs::write(&src, b"hi").unwrap();
		let op = journal::PendingOp::new(journal::OpKind::Create, "local:0", "n");
		let seq = od.journal.enqueue(&op, Some(&src)).unwrap();
		od.cache.insert("c9", &src).unwrap();
		od.journal.ack(seq, Some("E9"), Some("c9")).unwrap();
		let got = od.blob(&t.ctx.session, "E9", &journal::pending_cid(seq)).await.unwrap();
		assert_eq!(Some(got), od.cache.get("c9"));
	}

	#[tokio::test]
	async fn xattr_targets() {
		let (mut t, log) = ctx("x-targets", not_found).await;
		assert_eq!(t.ctx.versions_xattr(ROOT_INO).await, Err(Errno::ENODATA));
		let v1 = || "v1".to_owned();
		assert_eq!(t.ctx.restore_xattr(ROOT_INO, false, v1()).await, Err(Errno::ENOTSUP));
		track(&t.ctx, "local:5", "n");
		let ino = t.ctx.inodes.ino_for("local:5", ROOT_INO);
		assert_eq!(t.ctx.versions_xattr(ino).await, Err(Errno::EAGAIN));
		assert_eq!(t.ctx.restore_xattr(ino, true, v1()).await, Err(Errno::EAGAIN));
		t.ctx.owner.read_only = true;
		let ino = e1(&t);
		assert_eq!(t.ctx.restore_xattr(ino, false, v1()).await, Err(Errno::EROFS));
		assert!(log.lock().unwrap().is_empty());
	}

	#[tokio::test]
	async fn xattr_versions_and_restore() {
		let (t, log) = ctx("x-restore", restored).await;
		let ino = e1(&t);
		assert_eq!(
			t.ctx.versions_xattr(ino).await.as_deref(),
			Ok("v1\t2026-11-16T10:00:00Z\t3\t-")
		);
		assert_eq!(t.ctx.restore_xattr(ino, false, "v1".into()).await, Ok(()));
		assert!(logged(&log, "post /api/files/e1/versions/v1/restore"));
		assert!(logged(&log, "if-match: \"c-e1\""));
	}

	#[tokio::test]
	async fn xattr_restore_412_is_eagain() {
		let (t, _) = ctx("x-412", |_| (412, "{}".into())).await;
		let ino = e1(&t);
		assert_eq!(t.ctx.restore_xattr(ino, false, "v1".into()).await, Err(Errno::EAGAIN));
	}

	#[tokio::test]
	async fn xattr_restore_waits_for_local_edit() {
		let (t, log) = ctx("x-pending", restored).await;
		let ino = e1(&t);
		let op = journal::PendingOp::new(journal::OpKind::Replace, "E1", "f");
		od(&t.ctx).journal.enqueue(&op, None).unwrap();
		assert_eq!(t.ctx.restore_xattr(ino, false, "v1".into()).await, Err(Errno::EAGAIN));
		assert!(!logged(&log, "restore"));
	}
}
