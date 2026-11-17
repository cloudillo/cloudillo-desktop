// SPDX-FileCopyrightText: Szilárd Hajba
// SPDX-License-Identifier: MPL-2.0

//! Write path helpers; the `Filesystem` callbacks in `lib.rs` call these.
//!
//! Per inode being written, a [`Staged`] record in `Ctx::staged` holds a local staging file
//! `<staging>/<seq>-<local name>` (mode 0600). It is created on `create`, on `O_TRUNC` /
//! truncate-to-0 (empty), or on the first write (remote content downloaded first). The last
//! writer's `release` (or `fsync`) uploads it through the core `Uploader`: a new file is
//! created, an existing one replaced with `If-Match`. The staging file is deleted only after
//! its upload succeeded (or when it holds nothing of the user's).
//!
//! `on-demand`: nothing here calls the server. Commit copies the staging file into the
//! `Journal` (Create / Replace) and every namespace op is journaled too; the DB row is updated
//! at once, a new entry under the temporary id `local:<seq>`, new content under the token
//! `pending-<seq>` (served from the journal's staged copy until the sync worker lands it).
//! A Create / Replace is journaled only after `atomic::QUIET` without writes since the last
//! close (`fsync` does not wait). Files matching the mount's ignore patterns (new files only) or
//! above its size cap are never journaled: their staged record stays, kept local. An unlink or
//! rename-away of a tracked file is held `atomic::HOLD`; a new file appearing at the same
//! `(parent, name)` meanwhile (`create`, or rename of a not yet journaled file onto it, which
//! also covers a rename over a tracked file) becomes a Replace of that entry (atomic save).

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Write as _};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::MutexGuard;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime};

use cloudillo_desktop_core::ignore::Kept;
use cloudillo_desktop_core::{ChildKind, PendingOp, atomic};
use fuser::{Errno, FileAttr};

pub use commit::commit;
use commit::commit_now;
pub(crate) use commit::{failed, finish, pending, retry};
use hold::adopt_held;
pub(crate) use hold::fire_held;
pub use ns::{mkdir, rename, rmdir, unlink};

use crate::read::read_at;
use crate::{Ctx, Fail};

mod commit;
mod hold;
mod ns;

/// Node id prefix of a created file that has no entry yet.
const NEW_PREFIX: &str = "#new:";

/// Write state of one inode.
#[derive(Debug)]
#[allow(clippy::struct_excessive_bools)] // independent per-inode flags, not a state machine
pub(crate) struct Staged {
	/// Local staging file, once created.
	path: Option<PathBuf>,
	/// Entry id + the content id the staging is based on (`If-Match`); `None` = new file with
	/// no entry yet. A `None` base is fetched at commit (content staged without reading it).
	remote: Option<(String, Option<String>)>,
	/// Parent inode and local name (where a new file is created; names the staging file).
	parent: u64,
	name: String,
	writers: u32,
	/// Bumped on every change; `committed` = the value last uploaded.
	edits: u64,
	committed: u64,
	committing: bool,
	/// The last upload failed (a rename/unlink then stops waiting for it).
	failed: bool,
	/// Unlinked while open before its first upload: discarded on the last release. Also set on
	/// the hidden `held` / `orphan` records.
	unlinked: bool,
	/// Last change; the debounce commits [`atomic::QUIET`] after it.
	touched: Instant,
	/// A `commit` is waiting out the debounce.
	debouncing: bool,
	/// Kept local (ignore pattern / size cap): never journaled, so never dropped either.
	local: Option<Kept>,
	/// Not a write: a held unlink / rename-away at `(parent, name)` (see [`hold`]).
	held: Option<Held>,
	/// Not a write: the away-name of a rename-away that became a Replace. Local only and
	/// hidden; its unlink is dropped, never journaled as a Trash of the original entry.
	orphan: bool,
}

/// A held op; `away` = where a rename-away put the entry.
#[derive(Debug)]
struct Held {
	hold: atomic::Hold,
	op: PendingOp,
	away: Option<(u64, String)>,
}

impl Staged {
	fn new(remote: Option<(String, Option<String>)>, parent: u64, name: String) -> Self {
		Self {
			path: None,
			remote,
			parent,
			name,
			writers: 0,
			edits: 0,
			committed: 0,
			committing: false,
			failed: false,
			unlinked: false,
			touched: Instant::now(),
			debouncing: false,
			local: None,
			held: None,
			orphan: false,
		}
	}

	fn touch(&mut self) {
		self.edits += 1;
		self.touched = Instant::now();
	}
}

/// Map key of a `held` / `orphan` record: no inode, counted down from the top.
fn side_key(ctx: &Ctx) -> u64 {
	u64::MAX - ctx.seq.fetch_add(1, Ordering::Relaxed)
}

fn staged(ctx: &Ctx) -> MutexGuard<'_, HashMap<u64, Staged>> {
	cloudillo_desktop_core::lock(&ctx.staged)
}

pub fn eio(op: &str, e: &io::Error) -> Errno {
	tracing::warn!("{op}: staging file: {e}");
	Errno::EIO
}

/// A fresh, empty staging file for local name `name`.
fn new_staging(ctx: &Ctx, name: &str) -> io::Result<PathBuf> {
	let short: String = name.chars().take(100).collect();
	let path = ctx.staging.join(format!("{}-{short}", ctx.seq.fetch_add(1, Ordering::Relaxed)));
	OpenOptions::new().write(true).create_new(true).mode(0o600).open(&path)?;
	Ok(path)
}

// --- inline (local) queries -------------------------------------------------------------------

pub fn is_staged(ctx: &Ctx, ino: u64) -> bool {
	staged(ctx).contains_key(&ino)
}

/// Attributes of `ino` from its staging file, if it has one.
pub fn staged_attr(ctx: &Ctx, ino: u64) -> Option<FileAttr> {
	let path = staged(ctx).get(&ino)?.path.clone()?;
	let m = std::fs::metadata(path).ok()?;
	Some(ctx.owner.file(ino, m.len(), m.modified().unwrap_or_else(|_| SystemTime::now())))
}

/// Inode of a new (not yet uploaded) file `name` in `parent`.
pub fn staged_lookup(ctx: &Ctx, parent: u64, name: &str) -> Option<u64> {
	staged(ctx)
		.iter()
		.find(|(_, r)| r.remote.is_none() && !r.unlinked && r.parent == parent && r.name == name)
		.map(|(&ino, _)| ino)
}

/// New (not yet uploaded) files in `parent`, for `readdir`.
pub fn staged_new_in(ctx: &Ctx, parent: u64) -> Vec<(u64, String)> {
	staged(ctx)
		.iter()
		.filter(|(_, r)| r.remote.is_none() && !r.unlinked && r.parent == parent)
		.map(|(&ino, r)| (ino, r.name.clone()))
		.collect()
}

pub fn read_staged(ctx: &Ctx, ino: u64, off: u64, size: u32) -> Option<io::Result<Vec<u8>>> {
	let path = staged(ctx).get(&ino)?.path.clone()?;
	Some(read_at(&path, off, size).and_then(|mut data| {
		if let Some(s) = ctx.uploader.seal() {
			s.apply(&path, off, &mut data)?;
		}
		Ok(data)
	}))
}

/// The staging file has been created (reads come from it, not from the remote content).
pub fn has_staging(ctx: &Ctx, ino: u64) -> bool {
	staged(ctx).get(&ino).is_some_and(|r| r.path.is_some())
}

/// Write into the staging file; `None` if `ino` has none yet (call [`stage`] first).
pub fn write_staged(ctx: &Ctx, ino: u64, off: u64, data: &[u8]) -> Option<io::Result<u32>> {
	let mut st = staged(ctx);
	let rec = st.get_mut(&ino)?;
	let path = rec.path.as_ref()?;
	let res = OpenOptions::new()
		.write(true)
		.open(path)
		.and_then(|f| match ctx.uploader.seal() {
			None => f.write_all_at(data, off),
			Some(s) => {
				let len = f.metadata()?.len();
				if off > len {
					s.zero_fill(&f, path, len, off)?;
				}
				let mut buf = data.to_vec();
				s.apply(path, off, &mut buf)?;
				f.write_all_at(&buf, off)
			}
		});
	if res.is_ok() {
		rec.touch();
	}
	Some(res.map(|()| u32::try_from(data.len()).unwrap_or(u32::MAX)))
}

// --- open / create / release ------------------------------------------------------------------

/// `create`: a new empty staged file with one writer. Returns its node id and attributes.
pub fn create(ctx: &Ctx, parent: u64, name: &str) -> Result<(String, FileAttr), Errno> {
	let path = new_staging(ctx, name).map_err(|e| eio("create", &e))?;
	let node = format!("{NEW_PREFIX}{}", ctx.seq.fetch_add(1, Ordering::Relaxed));
	let ino = ctx.inodes.ino_for(&node, parent);
	let mut rec = Staged::new(None, parent, name.to_owned());
	rec.path = Some(path);
	rec.writers = 1;
	rec.edits = 1;
	if let Some(od) = ctx.tree.on_demand() {
		// atomic save: a tracked file just unlinked / renamed away here comes back as this one
		rec.remote = adopt_held(ctx, od, ino, parent, name);
	}
	staged(ctx).insert(ino, rec);
	Ok((node, ctx.owner.file(ino, 0, SystemTime::now())))
}

/// Count one more writer of `ino` (node id `node`). `trunc` empties the content without
/// downloading it. Copies nothing remote: that waits for the first write ([`stage`]).
pub fn open_writer(ctx: &Ctx, ino: u64, node: &str, trunc: bool) -> Result<(), Errno> {
	let mut st = staged(ctx);
	let rec = if let Some(rec) = st.get_mut(&ino) {
		rec
	} else {
		let child = ctx.tree.get(node).ok_or(Errno::ENOENT)?;
		match child.kind {
			ChildKind::File => {}
			ChildKind::Launcher { .. } => return Err(Errno::EACCES),
			ChildKind::Folder | ChildKind::RoomFolder => return Err(Errno::EISDIR),
		}
		let parent = ctx.inodes.parent(ino).ok_or(Errno::ENOENT)?;
		st.entry(ino).or_insert(Staged::new(
			Some((node.to_owned(), None)),
			parent,
			child.local_name,
		))
	};
	if trunc {
		if ctx.uploader.seal().is_some() && !rec.committing && rec.path.is_some() {
			// a fresh staging path = a fresh seal nonce: never reuse the keystream from offset 0
			remove_staging(rec.path.take());
			rec.path = Some(new_staging(ctx, &rec.name).map_err(|e| eio("truncate", &e))?);
		} else if let Some(p) = rec.path.clone() {
			// sealed + committing reuses the keystream (the upload reads this path);
			// needs a path swap once the commit ends
			OpenOptions::new()
				.write(true)
				.open(p)
				.and_then(|f| f.set_len(0))
				.map_err(|e| eio("truncate", &e))?;
		} else {
			rec.path = Some(new_staging(ctx, &rec.name).map_err(|e| eio("truncate", &e))?);
			// Content not read: the If-Match base is fetched at commit.
			if let Some(r) = rec.remote.as_mut() {
				r.1 = None;
			}
		}
		rec.touch();
	}
	rec.writers += 1;
	rec.failed = false;
	Ok(())
}

/// One writer of `ino` is gone. `true` = it was the last and there is something to upload:
/// call [`commit`]. Synchronous, so a rename/unlink arriving after `close()` already sees the
/// pending upload (see [`settle`]).
pub fn release_writer(ctx: &Ctx, ino: u64) -> bool {
	let mut st = staged(ctx);
	let Some(rec) = st.get_mut(&ino) else {
		return false;
	};
	rec.writers = rec.writers.saturating_sub(1);
	if rec.writers > 0 || rec.committing {
		// An in-flight commit re-checks for changes when it finishes.
		return false;
	}
	if !rec.unlinked && rec.edits != rec.committed {
		return true;
	}
	if !rec.unlinked && rec.local.is_some() {
		return false; // kept local: the staging file is the only copy
	}
	// Unlinked before its first upload, or an unchanged copy of the remote content.
	let path = st.remove(&ino).and_then(|r| r.path);
	drop(st);
	remove_staging(path);
	false
}

fn remove_staging(path: Option<PathBuf>) {
	if let Some(p) = path
		&& let Err(e) = std::fs::remove_file(&p)
	{
		tracing::warn!("removing {}: {e}", p.display());
	}
}

/// Copy entry content into a new staging file for `ino`, unless it has one. Downloads are
/// serialized; the `If-Match` base is read (after any in-flight replace landed) just before.
pub async fn stage(ctx: &Ctx, ino: u64) -> Result<(), Fail> {
	let _one = ctx.stage_lock.lock().await;
	let (id, name) = {
		let st = staged(ctx);
		let rec = st.get(&ino).ok_or(Errno::EBADF)?;
		if rec.path.is_some() {
			return Ok(());
		}
		let id = rec.remote.as_ref().map(|r| r.0.clone()).ok_or(Errno::EIO)?;
		(id, rec.name.clone())
	};
	let (id, base, path) = if let Some(od) = ctx.tree.on_demand() {
		// Base = the row's content (no network); copied from the journal or the blob cache.
		let id = od.journal.resolve(&id)?;
		let cid = od.db.get(&id)?.and_then(|r| r.content_id).ok_or(Errno::EIO)?;
		let src = od.blob(&ctx.session, &id, &cid).await?;
		let path = new_staging(ctx, &name)?;
		if let Err(e) = std::fs::copy(&src, &path) {
			std::fs::remove_file(&path).ok();
			return Err(e.into());
		}
		(id, Some(cid), path)
	} else {
		let base = ctx.uploader.base(&id).await?;
		let path = new_staging(ctx, &name)?;
		if let Err(e) = download(ctx, &id, &path).await {
			// A partial copy of remote content, nothing of the user's.
			std::fs::remove_file(&path).ok();
			return Err(e.into());
		}
		(id, base, path)
	};
	let mut st = staged(ctx);
	if let Some(rec) = st.get_mut(&ino).filter(|r| r.path.is_none()) {
		rec.path = Some(path);
		rec.remote = Some((id, base));
		return Ok(());
	}
	// Released meanwhile, or an `O_TRUNC` open staged it first: that one wins.
	drop(st);
	std::fs::remove_file(&path).ok();
	Ok(())
}

async fn download(ctx: &Ctx, id: &str, path: &Path) -> cloudillo_desktop_core::Result<()> {
	ctx.session
		.call(|api| async move {
			let mut dl = api.download(id, None).await?;
			// Truncates, so a retried attempt starts over.
			let mut f = File::create(path)?;
			let mut off = 0;
			while let Some(chunk) = dl.chunk().await? {
				match ctx.uploader.seal() {
					None => f.write_all(&chunk)?,
					Some(s) => {
						let mut buf = chunk.to_vec();
						s.apply(path, off, &mut buf)?;
						f.write_all(&buf)?;
					}
				}
				off += chunk.len() as u64;
			}
			Ok(())
		})
		.await
}

/// `setattr(size)`: truncate/extend, as a writer of its own (uploads at once if no one else
/// has the file open).
pub async fn truncate(ctx: &Ctx, ino: u64, size: u64) -> Result<FileAttr, Fail> {
	let node = ctx.node(ino).ok_or(Errno::ENOENT)?;
	open_writer(ctx, ino, &node, size == 0)?;
	let res = resize(ctx, ino, size).await;
	let done = if release_writer(ctx, ino) { commit_now(ctx, ino).await } else { Ok(()) };
	res?;
	done?;
	let mut a = ctx.attr_of(ino).ok_or(Errno::ENOENT)?;
	a.size = size;
	a.blocks = size.div_ceil(512);
	Ok(a)
}

async fn resize(ctx: &Ctx, ino: u64, size: u64) -> Result<(), Fail> {
	if size == 0 {
		return Ok(()); // `open_writer(trunc)` did it
	}
	stage(ctx, ino).await?;
	let mut st = staged(ctx);
	let rec = st.get_mut(&ino).ok_or(Errno::EBADF)?;
	let path = rec.path.as_ref().ok_or(Errno::EIO)?;
	let f = OpenOptions::new().write(true).open(path)?;
	match ctx.uploader.seal() {
		Some(s) if size > f.metadata()?.len() => {
			let len = f.metadata()?.len();
			s.zero_fill(&f, path, len, size)?;
		}
		_ => f.set_len(size)?,
	}
	rec.touch();
	Ok(())
}

/// Wait (at most 120 s) while new file `name` in `parent` is closed but not uploaded yet, so
/// a rename/unlink right after `close()` (the upload runs on `release`) finds the entry.
async fn settle(ctx: &Ctx, parent: u64, name: &str) {
	for _ in 0..1200 {
		let pending = staged(ctx).values().any(|r| {
			r.remote.is_none()
				&& r.writers == 0
				&& !r.failed && !r.unlinked
				&& !r.debouncing
				&& r.local.is_none()
				&& r.parent == parent
				&& r.name == name
		});
		if !pending {
			return;
		}
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
}

/// Drop the write state of `ino` without uploading it: at once, or on the last release /
/// the end of an in-flight commit.
fn discard_staged(ctx: &Ctx, ino: u64) -> io::Result<()> {
	let mut st = staged(ctx);
	let busy = st.get(&ino).is_some_and(|r| r.writers > 0 || r.committing);
	let path = if busy {
		if let Some(r) = st.get_mut(&ino) {
			r.unlinked = true;
		}
		None
	} else {
		st.remove(&ino).and_then(|r| r.path)
	};
	drop(st);
	if let Some(od) = ctx.tree.on_demand() {
		od.policy.mark(ino, None);
	}
	if let Some(p) = path {
		std::fs::remove_file(p)?;
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use cloudillo_desktop_core::journal::local_id;
	use cloudillo_desktop_core::seal::Seal;
	use cloudillo_desktop_core::{OpKind, Uploader, db, journal};
	use fuser::RenameFlags;

	use super::*;
	use crate::inode::ROOT_INO;
	use crate::test_ctx::{T, ctx, not_found, od, off, track};

	/// Create `name` at the root, write to it and close it (no commit yet). Returns its inode.
	fn write_new(ctx: &Ctx, name: &str) -> u64 {
		let (_, attr) = create(ctx, ROOT_INO, name).unwrap();
		let ino = attr.ino.0;
		write_staged(ctx, ino, 0, b"hi").unwrap().unwrap();
		assert!(release_writer(ctx, ino));
		ino
	}

	/// Every journaled op as `(kind, entry id, name)`, in replay order (drains the journal).
	fn drain(ctx: &Ctx) -> Vec<(OpKind, String, Option<String>)> {
		let j = &od(ctx).journal;
		let mut out = Vec::new();
		while let Some(op) = j.next_ready().unwrap() {
			j.ack(op.seq, None, None).unwrap();
			out.push((op.op, op.entry_id, op.name));
		}
		out
	}

	fn staging_empty(t: &T) -> bool {
		std::fs::read_dir(&t.ctx.staging).unwrap().next().is_none()
	}

	#[tokio::test]
	async fn commit_journals_create() {
		let (t, _) = ctx("w-create", not_found).await;
		let ino = write_new(&t.ctx, "a.txt");
		commit_now(&t.ctx, ino).await.unwrap();
		let ops = drain(&t.ctx);
		assert_eq!(ops.len(), 1);
		assert_eq!((ops[0].0, ops[0].2.as_deref()), (OpKind::Create, Some("a.txt")));
		let seq: i64 = ops[0].1.trim_start_matches(journal::LOCAL_PREFIX).parse().unwrap();
		let row = od(&t.ctx).db.get(&local_id(seq)).unwrap().unwrap();
		assert_eq!(row.status, db::LIVE);
		assert!(staging_empty(&t));
	}

	#[tokio::test]
	async fn ignored_new_file_kept_local() {
		let (t, _) = ctx("w-ignored", not_found).await;
		let ino = write_new(&t.ctx, "x~");
		commit_now(&t.ctx, ino).await.unwrap();
		assert!(drain(&t.ctx).is_empty());
		assert_eq!(od(&t.ctx).policy.kept(), (1, 0));
	}

	#[tokio::test]
	async fn rmdir_with_staged_child_is_enotempty() {
		let (t, _) = ctx("w-rmdir", not_found).await;
		let d = mkdir(&t.ctx, ROOT_INO, "d").await.unwrap().ino.0;
		create(&t.ctx, d, "x").unwrap();
		assert!(matches!(
			rmdir(&t.ctx, ROOT_INO, "d").await,
			Err(Fail::Errno(e)) if e == Errno::ENOTEMPTY
		));
	}

	#[tokio::test]
	async fn rename_onto_staged_new_file_replaces_it() {
		let (t, _) = ctx("w-rename-staged", not_found).await;
		for n in ["a", "b"] {
			let ino = write_new(&t.ctx, n);
			// as after `release`: a debounced commit is coming (else `settle` waits for it)
			staged(&t.ctx).get_mut(&ino).unwrap().debouncing = true;
		}
		rename(&t.ctx, (ROOT_INO, "a"), (ROOT_INO, "b"), RenameFlags::empty())
			.await
			.unwrap();
		let names: Vec<_> = staged_new_in(&t.ctx, ROOT_INO).into_iter().map(|x| x.1).collect();
		assert_eq!(names, ["b"]);
		finish(&t.ctx).await;
		let ops = drain(&t.ctx);
		assert_eq!(ops.len(), 1);
		assert_eq!((ops[0].0, ops[0].2.as_deref()), (OpKind::Create, Some("b")));
	}

	#[tokio::test]
	async fn sealed_trunc_gets_fresh_staging() {
		let (mut t, _) = off("w-seal-trunc", not_found).await;
		let seal = Arc::new(Seal::random().unwrap());
		t.ctx.uploader = Arc::new(Uploader::new(Arc::clone(&t.ctx.session), Some(seal)));
		let (node, attr) = create(&t.ctx, ROOT_INO, "f").unwrap();
		let ino = attr.ino.0;
		write_staged(&t.ctx, ino, 0, b"hi").unwrap().unwrap();
		let old = staged(&t.ctx)[&ino].path.clone().unwrap();
		open_writer(&t.ctx, ino, &node, true).unwrap();
		let new = staged(&t.ctx)[&ino].path.clone().unwrap();
		assert_ne!(new, old);
		assert!(!old.exists());
		assert!(read_staged(&t.ctx, ino, 0, 16).unwrap().unwrap().is_empty());
	}

	#[tokio::test]
	async fn atomic_save_becomes_replace() {
		let (t, _) = ctx("w-atomic", not_found).await;
		track(&t.ctx, "E1", "f");
		unlink(&t.ctx, ROOT_INO, "f").await.unwrap();
		let ino = write_new(&t.ctx, "f");
		commit_now(&t.ctx, ino).await.unwrap();
		finish(&t.ctx).await;
		let ops = drain(&t.ctx);
		assert_eq!(ops.len(), 1);
		assert_eq!((ops[0].0, ops[0].1.as_str()), (OpKind::Replace, "E1"));
	}

	#[tokio::test]
	async fn held_rename_replays_before_later_rename() {
		let (t, _) = ctx("w-rename", not_found).await;
		track(&t.ctx, "E1", "a");
		track(&t.ctx, "E2", "c");
		let none = RenameFlags::empty();
		rename(&t.ctx, (ROOT_INO, "a"), (ROOT_INO, "b"), none).await.unwrap();
		rename(&t.ctx, (ROOT_INO, "b"), (ROOT_INO, "c"), none).await.unwrap();
		let ops = drain(&t.ctx);
		let e1: Vec<_> = ops.iter().filter(|o| o.1 == "E1").map(|o| o.2.as_deref()).collect();
		assert_eq!(e1, [Some("b"), Some("c")]);
		assert!(ops.iter().any(|o| o.1 == "E2" && o.0 == OpKind::Trash));
	}

	#[tokio::test]
	async fn finish_flushes_debounce_and_holds() {
		let (t, _) = ctx("w-finish", not_found).await;
		track(&t.ctx, "E1", "f");
		write_new(&t.ctx, "n.txt");
		unlink(&t.ctx, ROOT_INO, "f").await.unwrap();
		assert_eq!(pending(&t.ctx), 2);
		finish(&t.ctx).await;
		assert!(staged(&t.ctx).is_empty());
		assert_eq!(pending(&t.ctx), 0);
		let ops = drain(&t.ctx);
		assert!(ops.iter().any(|o| o.0 == OpKind::Trash && o.1 == "E1"));
		assert!(ops.iter().any(|o| o.0 == OpKind::Create && o.2.as_deref() == Some("n.txt")));
		assert!(staging_empty(&t));
	}

	/// `off`: unlinked while its create was uploading: the landed entry is trashed again and
	/// the staging is gone.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn unlinked_while_uploading_trashes_landed() {
		let (tx, rx) = std::sync::mpsc::channel::<()>();
		let rx = std::sync::Mutex::new(rx);
		let (t, log) = off("w-unlinked", move |l| {
			if l.starts_with("POST /api/uploads/") {
				let _ = rx.lock().unwrap().recv_timeout(Duration::from_secs(5));
				(200, r#"{"data":{"id":"E9","contentId":"c9","fileName":"a.txt"}}"#.into())
			} else {
				(200, r#"{"data":[]}"#.into())
			}
		})
		.await;
		let ino = write_new(&t.ctx, "a.txt");
		let logged = |n: &str| log.lock().unwrap().iter().any(|h| h.contains(n));
		let drive = async {
			while !logged("post /api/uploads/") {
				tokio::time::sleep(Duration::from_millis(10)).await;
			}
			staged(&t.ctx).get_mut(&ino).unwrap().unlinked = true;
			tx.send(()).unwrap();
		};
		let (res, ()) = tokio::join!(commit_now(&t.ctx, ino), drive);
		res.unwrap();
		assert!(logged("delete /api/files/e9"));
		assert!(!is_staged(&t.ctx, ino));
		assert!(staging_empty(&t));
	}
}
