//! `cloudillo daemon`: owns every persistent mount (`mounts.toml`) and answers CLI requests on
//! the Unix socket ([`ipc::socket_path`]). [`request`] is the client side; it starts the daemon
//! on demand.

use std::collections::HashMap;
use std::fmt::{Display, Write as _};
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::sync::Arc;
use std::time::Duration;

use cloudillo_desktop_core::ipc::{self, Request, Response};
use cloudillo_desktop_core::{
	BlobCache, Child, Config, Db, DbTree, FeedPoller, Journal, MountEntry, RemoteTree, Session,
	SyncWorker, Uploader, cache, credentials, ignore, pin, sync, tree,
};
use cloudillo_desktop_fs_fuse::{Mode, MountOptions, OnDemand, Writes, mount};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;

use crate::Res;

/// Path lookup of a mount: the same source its FUSE side reads.
enum Lookup {
	Remote(RemoteTree),
	Db(DbTree),
}

impl Lookup {
	async fn children(&self, node: &str) -> cloudillo_desktop_core::Result<Vec<Child>> {
		match self {
			Self::Remote(t) => t.children(node).await,
			Self::Db(t) => t.children(node),
		}
	}
}

/// A running mount.
struct Live {
	uploader: Arc<Uploader>,
	/// fuser's `BackgroundSession`; dropping it unmounts.
	bg: Box<dyn Send>,
	writes: Writes,
	staging: PathBuf,
	/// Staging dirs of earlier runs still holding files that were never uploaded.
	leftover: Vec<PathBuf>,
	tree: Arc<Lookup>,
	/// `on-demand` / `pinned` only.
	sync: Option<Syncing>,
}

/// How often pinned files are re-checked: the feed polls every 60 s, so a feed change is
/// hydrated within one poll plus this.
// a timer, since `FeedPoller::run` has no post-round hook; add one if the lag matters.
const PIN_TICK: Duration = Duration::from_secs(15);

/// Feed poller + journal sync worker + pin hydration of an `on-demand` / `pinned` mount, and
/// their tasks (none while paused).
struct Syncing {
	poller: Arc<FeedPoller>,
	worker: Arc<SyncWorker>,
	pins: Arc<Pins>,
	/// Ignore patterns + size cap, registered for the FUSE side under the staging dir.
	policy: Arc<ignore::Policy>,
	/// Poller + pin hydration.
	tasks: Vec<JoinHandle<()>>,
	/// The sync worker: paused cooperatively (never aborted mid-op).
	worker_task: Option<JoinHandle<()>>,
}

/// What [`pin::hydrate_pinned`] needs. `whole` = a `pinned` mount.
struct Pins {
	db: Arc<Db>,
	cache: Arc<BlobCache>,
	session: Arc<Session>,
	whole: bool,
	/// Hydrate now (after a `pin`).
	wake: Notify,
}

impl Syncing {
	fn resume(&mut self) {
		if self.worker_task.is_some() {
			return;
		}
		self.worker.resume();
		let w = Arc::clone(&self.worker);
		self.worker_task = Some(tokio::spawn(async move { w.run().await }));
		if self.tasks.is_empty() {
			let p = Arc::clone(&self.poller);
			self.tasks.push(tokio::spawn(async move { p.run().await }));
			let pins = Arc::clone(&self.pins);
			self.tasks.push(tokio::spawn(async move {
				loop {
					// offline or a failed file: retried at the next tick (core does not log)
					let _ =
						pin::hydrate_pinned(&pins.db, &pins.cache, &pins.session, pins.whole).await;
					let _ = tokio::time::timeout(PIN_TICK, pins.wake.notified()).await;
				}
			}));
		}
	}

	/// Stops the tasks without waiting: returns the worker's task, which ends once its op
	/// in flight is done.
	fn halt(&mut self) -> Option<JoinHandle<()>> {
		for t in self.tasks.drain(..) {
			t.abort();
		}
		self.worker.pause();
		self.worker_task.take()
	}

	/// Returns once the worker finished the op in flight.
	async fn pause(&mut self) {
		if let Some(t) = self.halt() {
			let _ = t.await;
		}
	}
}

#[derive(Default)]
struct Daemon {
	cfg: Config,
	live: HashMap<String, Live>,
	/// Why a configured mount is not running (restore on start failed).
	errors: HashMap<String, String>,
}

// one lock for the whole registry, held across mount/unmount; per-mount locks if a
// slow mount ever stalls `status`.
type Shared = Arc<Mutex<Daemon>>;

fn err(e: impl Display) -> String {
	e.to_string()
}

fn cache_root() -> Result<PathBuf, String> {
	dirs::cache_dir()
		.map(|d| d.join("cloudillo"))
		.ok_or_else(|| "no cache dir ($XDG_CACHE_HOME / $HOME unset)".to_owned())
}

/// [`cache_root`], created / tightened to `0700`: everything under it (db, blobs, staging,
/// the fallback socket dir) is then private to this user.
fn private_cache_root() -> Result<PathBuf, String> {
	let root = cache_root()?;
	private_dir(&root)?;
	Ok(root)
}

/// Create `dir` and tighten it to `0700`, even when it already existed.
fn private_dir(dir: &Path) -> Result<(), String> {
	std::fs::DirBuilder::new()
		.recursive(true)
		.mode(0o700)
		.create(dir)
		.map_err(err)?;
	std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).map_err(err)
}

/// `$XDG_CACHE_HOME/cloudillo/<id>/`; ids are numeric, so never `staging` or a path escape.
fn cache_dir(id: &str) -> Result<PathBuf, String> {
	if id.is_empty() || !id.bytes().all(|b| b.is_ascii_digit()) {
		return Err(format!("bad mount id {id:?} in mounts.toml"));
	}
	Ok(cache_root()?.join(id))
}

fn check_mode(mode: &str) -> Result<(), String> {
	match mode {
		"off" | "on-demand" | "pinned" => Ok(()),
		m => Err(format!("unknown mode {m} (off | on-demand | pinned)")),
	}
}

async fn start(e: &MountEntry, cache_max_mb: Option<u64>) -> Result<Live, String> {
	check_mode(&e.mode)?;
	let root = private_cache_root()?;
	let dir = cache_dir(&e.id)?;
	let account = e.account.clone();
	let creds = tokio::task::spawn_blocking(move || credentials::load(&account))
		.await
		.map_err(err)?
		.map_err(err)?
		.ok_or_else(|| format!("{0} is not logged in; run `cloudillo login {0}`", e.account))?;
	let session = Arc::new(Session::new(creds).map_err(err)?);
	// per daemon process, so leftovers of an earlier run are never overwritten
	let staging = root.join("staging").join(&e.id).join(std::process::id().to_string());
	// earlier runs' dirs: empty ones go; the rest hold files a crash left un-uploaded
	// (nothing records their entry, so they are only reported)
	// a reused pid: move its dir aside, so the scan below handles it like any other
	if staging.exists() {
		let secs = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.map_or(0, |d| d.as_secs());
		let aside = staging.with_extension(secs.to_string());
		if let Err(e) = std::fs::rename(&staging, &aside) {
			tracing::warn!("cannot move stale staging dir {}: {e}", staging.display());
		}
	}
	let mut leftover = Vec::new();
	if let Some(Ok(dirs)) = staging.parent().map(std::fs::read_dir) {
		for d in dirs.flatten().map(|d| d.path()).filter(|d| *d != staging) {
			if std::fs::remove_dir(&d).is_err() {
				tracing::warn!("files not uploaded by an earlier run are kept in {}", d.display());
				leftover.push(d);
			}
		}
	}
	let uploader = Arc::new(Uploader::new(Arc::clone(&session), None));
	let (mode, lookup, sync) = if e.mode == "off" {
		(Mode::Off, Lookup::Remote(RemoteTree::new(Arc::clone(&session))), None)
	} else {
		std::fs::create_dir_all(&dir).map_err(err)?;
		let db = Arc::new(Db::open(&dir.join("meta.sqlite")).map_err(err)?);
		let whole = e.mode == "pinned";
		// a pinned mount keeps every file: nothing is evictable, so no limit
		let max = if whole {
			u64::MAX
		} else {
			cache_max_mb.unwrap_or(cache::DEFAULT_MAX_MB).saturating_mul(1024 * 1024)
		};
		let blobs = BlobCache::new(dir.join("blobs"), max, Arc::clone(&db)).map_err(err)?;
		let poller = Arc::new(FeedPoller::new(Arc::clone(&session), Arc::clone(&db)));
		let lookup = Lookup::Db(DbTree::new(&session, Arc::clone(&db)).map_err(err)?);
		// survives restarts: changes made offline (or before a crash) replay on the next start
		let journal = Arc::new(Journal::new(Arc::clone(&db), dir.join("staged")).map_err(err)?);
		let cache = Arc::new(blobs);
		let worker = Arc::new(SyncWorker::new(
			Arc::clone(&session),
			Arc::clone(&uploader),
			Arc::clone(&journal),
			Arc::clone(&cache),
			Arc::clone(&poller),
		));
		let pins = Arc::new(Pins {
			db: Arc::clone(&db),
			cache: Arc::clone(&cache),
			session: Arc::clone(&session),
			whole,
			wake: Notify::new(),
		});
		let policy = Arc::new(ignore::Policy::new(&e.ignore, e.max_file_size_mb));
		let od = OnDemand {
			db,
			cache,
			poller: Arc::clone(&poller),
			journal,
			policy: Arc::clone(&policy),
		};
		let mode = if whole { Mode::Pinned(od) } else { Mode::OnDemand(od) };
		(
			mode,
			lookup,
			Some(Syncing { poller, worker, pins, policy, tasks: Vec::new(), worker_task: None }),
		)
	};
	let opts = MountOptions { staging: staging.clone(), uploader: Arc::clone(&uploader), mode };
	let (bg, writes) = mount(Arc::clone(&session), &e.mountpoint, opts).map_err(|x| {
		let _ = std::fs::remove_dir(&staging);
		format!("mount {}: {x}", e.mountpoint.display())
	})?;
	let sync = sync.map(|mut s| {
		if !e.paused {
			s.resume();
		}
		s
	});
	Ok(Live { uploader, bg: Box::new(bg), writes, staging, leftover, tree: Arc::new(lookup), sync })
}

/// Unmount; without `discard` waits for uploads first. Returns a note if staging was kept.
async fn stop(mut l: Live, discard: bool) -> Option<String> {
	if let Some(s) = &mut l.sync {
		s.pause().await;
	}
	if !discard {
		l.uploader.idle().await;
	}
	// Dropping the session unmounts and joins the FUSE thread; keep that off the runtime.
	let bg = l.bg;
	let _ = tokio::task::spawn_blocking(move || drop(bg)).await;
	if !discard {
		// debounced / held writes: journaled or uploaded now, else they die with the process
		l.writes.finish().await;
		l.uploader.idle().await;
	}
	let res =
		if discard { std::fs::remove_dir_all(&l.staging) } else { std::fs::remove_dir(&l.staging) };
	if let Some(parent) = l.staging.parent() {
		let _ = std::fs::remove_dir(parent);
	}
	match res {
		Ok(()) => None,
		Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
		// non-empty = failed uploads: never discard them unasked
		Err(e) => Some(format!("staging kept, files not uploaded: {} ({e})", l.staging.display())),
	}
}

pub(crate) async fn busy(uploader: &Uploader) -> bool {
	tokio::time::timeout(Duration::ZERO, uploader.idle()).await.is_err()
}

/// Journaled changes of `on-demand` mount `m` the server has not acknowledged (its DB is
/// opened if the mount is not running). An unreadable journal is an error, never 0.
fn unsynced(m: &MountEntry, live: Option<&Live>) -> Result<u64, String> {
	let counts = if let Some(s) = live.and_then(|l| l.sync.as_ref()) {
		s.worker.journal().counts()
	} else {
		let dir = cache_dir(&m.id)?;
		let meta = dir.join("meta.sqlite");
		if !meta.is_file() {
			return Ok(0);
		}
		Db::open(&meta)
			.and_then(|db| Journal::new(Arc::new(db), dir.join("staged")))
			.and_then(|j| j.counts())
	};
	counts.map(|(n, _)| n).map_err(|e| format!("cannot read the sync journal: {e}"))
}

fn saved(cfg: &Config, msg: String) -> String {
	match cfg.save() {
		Ok(()) => msg,
		Err(e) => format!("{msg}; warning: mounts.toml not saved: {e}"),
	}
}

async fn mount_new(d: &Shared, account: String, mountpoint: PathBuf, mode: String) -> Response {
	check_mode(&mode)?;
	if !mountpoint.is_absolute() {
		return Err(format!("mountpoint {} must be absolute", mountpoint.display()));
	}
	let mut g = d.lock().await;
	if let Some(m) = g.cfg.mounts.iter().find(|m| m.mountpoint == mountpoint) {
		return Err(format!("{} is already mount {}", mountpoint.display(), m.id));
	}
	let entry = MountEntry {
		// never reuse an id whose cache dir survived (unsynced journal)
		id: g.cfg.next_id(|id| cache_dir(id).is_ok_and(|d| d.exists())),
		account,
		mountpoint,
		mode,
		paused: false,
		ignore: Vec::new(),
		max_file_size_mb: None,
	};
	let live = start(&entry, g.cfg.cache_max_mb).await?;
	g.live.insert(entry.id.clone(), live);
	let msg =
		format!("mounted {} at {} (id {})", entry.account, entry.mountpoint.display(), entry.id);
	g.cfg.mounts.push(entry);
	Ok(saved(&g.cfg, msg))
}

async fn unmount(d: &Shared, target: &str, discard: bool) -> Response {
	let mut g = d.lock().await;
	let idx = g
		.cfg
		.mounts
		.iter()
		.position(|m| m.id == target || m.mountpoint == Path::new(target))
		.ok_or_else(|| {
			format!("no mount {target} (a temporary mount ends with Ctrl-C in its terminal)")
		})?;
	let m = g.cfg.mounts.remove(idx);
	if let Some(live) = g.live.get(&m.id)
		&& !discard
		&& (live.writes.pending() > 0 || busy(&live.uploader).await)
	{
		g.cfg.mounts.insert(idx, m);
		return Err("uploads in flight or files still being saved; retry in a few seconds, or \
		            pass --discard"
			.to_owned());
	}
	// the cache dir below holds the journal: never drop unacknowledged changes unasked
	if m.mode != "off" && !discard {
		let blocker = match unsynced(&m, g.live.get(&m.id)) {
			Ok(0) => None,
			Ok(n) => Some(format!("{n} local change(s) not synced yet")),
			Err(e) => Some(e),
		};
		if let Some(why) = blocker {
			g.cfg.mounts.insert(idx, m);
			return Err(format!("{why}; retry once synced, or pass --discard to drop them"));
		}
	}
	let mut msg = format!("unmounted {}", m.id);
	if let Some(live) = g.live.remove(&m.id)
		&& let Some(note) = stop(live, discard).await
	{
		msg = format!("{msg}; {note}");
	}
	g.errors.remove(&m.id);
	if m.mode != "off"
		&& let Ok(dir) = cache_dir(&m.id)
	{
		// re-checked now that FUSE is gone: a write may have been journaled meanwhile
		if discard || unsynced(&m, None) == Ok(0) {
			let _ = std::fs::remove_dir_all(dir);
		} else {
			msg = format!("{msg}; unsynced changes kept in {}", dir.display());
		}
	}
	Ok(saved(&g.cfg, msg))
}

/// Pause / resume change-feed polling of an `on-demand` mount.
async fn set_paused(d: &Shared, id: &str, paused: bool) -> Response {
	let mut guard = d.lock().await;
	let g = &mut *guard;
	let m = g
		.cfg
		.mounts
		.iter_mut()
		.find(|m| m.id == id)
		.ok_or_else(|| format!("no mount {id}"))?;
	if m.mode == "off" {
		return Err(format!("mount {id} is `off`: it has no sync to pause or resume"));
	}
	m.paused = paused;
	if let Some(s) = g.live.get_mut(id).and_then(|l| l.sync.as_mut()) {
		if paused {
			// not awaited under the registry lock; a later run waits for this one's op
			drop(s.halt());
		} else {
			s.resume();
		}
	}
	Ok(saved(&g.cfg, format!("{} {id}", if paused { "paused" } else { "resumed" })))
}

fn status(d: &Daemon) -> String {
	if d.cfg.mounts.is_empty() {
		return "no mounts".to_owned();
	}
	let line = |m: &MountEntry| {
		let mut state = match (d.live.contains_key(&m.id), d.errors.get(&m.id)) {
			(true, _) if m.paused => "paused".to_owned(),
			(true, _) => "mounted".to_owned(),
			(false, Some(e)) => format!("not mounted: {e}"),
			(false, None) => "not mounted".to_owned(),
		};
		if let Some(s) = d.live.get(&m.id).and_then(|l| l.sync.as_ref()) {
			if s.worker.dead() {
				state.push_str(", re-pair needed");
			}
			if let Ok((n, failed)) = s.worker.journal().counts()
				&& n > 0
			{
				let _ = write!(state, ", {n} unsynced ({failed} failed)");
			}
			// rejected by the server (journal) + kept local before journaling (policy)
			let (big, quota) = sync::failures(&s.pins.db).unwrap_or_default();
			let (ignored, local_big) = s.policy.kept();
			let big = big + u64::try_from(local_big).unwrap_or(0);
			if big > 0 {
				let _ = write!(state, ", {big} not synced: too large");
			}
			if quota > 0 {
				let _ = write!(state, ", {quota} quota exceeded");
			}
			if ignored > 0 {
				let _ = write!(state, ", {ignored} ignored (kept local)");
			}
		}
		for dir in d.live.get(&m.id).map_or(&[][..], |l| &l.leftover) {
			let _ = write!(state, ", files not uploaded kept in {}", dir.display());
		}
		format!("{}\t{}\t{}\t{}\t{state}", m.id, m.mode, m.account, m.mountpoint.display())
	};
	d.cfg.mounts.iter().map(line).collect::<Vec<_>>().join("\n")
}

/// A path inside a running mount, resolved by local names from the mount root.
struct Target {
	pins: Option<Arc<Pins>>,
	/// `None` = the mount root.
	child: Option<Child>,
}

/// Walk `path` (absolute) from the mount root.
async fn find(d: &Shared, path: &Path) -> Result<Target, String> {
	let (lookup, pins, names) = {
		let g = d.lock().await;
		let (entry, rest) = g
			.cfg
			.mounts
			.iter()
			.find_map(|m| path.strip_prefix(&m.mountpoint).ok().map(|r| (m, r)))
			.ok_or_else(|| format!("{} is not inside a cloudillo mount", path.display()))?;
		let live = g
			.live
			.get(&entry.id)
			.ok_or_else(|| format!("mount {} is not mounted", entry.id))?;
		let names = rest
			.components()
			.map(|c| match c {
				Component::Normal(s) => s.to_str().map(str::to_owned),
				_ => None,
			})
			.collect::<Option<Vec<_>>>()
			.ok_or_else(|| format!("{}: unsupported path", path.display()))?;
		let pins = live.sync.as_ref().map(|s| Arc::clone(&s.pins));
		(Arc::clone(&live.tree), pins, names)
	};
	let mut node = tree::ROOT.to_owned();
	let mut found = None;
	for name in &names {
		let child = lookup
			.children(&node)
			.await
			.map_err(err)?
			.into_iter()
			.find(|c| &c.local_name == name)
			.ok_or_else(|| format!("{}: no such file", path.display()))?;
		node.clone_from(&child.entry_id);
		found = Some(child);
	}
	Ok(Target { pins, child: found })
}

/// `pin` / `unpin` a file or folder of an `on-demand` mount (read-only mounts too).
async fn set_pin(d: &Shared, path: &Path, on: bool) -> Response {
	let t = find(d, path).await?;
	let pins = t.pins.ok_or("pinning needs an `on-demand` mount")?;
	if pins.whole {
		return Err("this mount is `pinned`: all of it stays cached".to_owned());
	}
	let child = t.child.ok_or("pin a path inside the mount; `--mode pinned` pins all of it")?;
	if on {
		pin::pin(&pins.db, &child.entry_id).map_err(err)?;
		pins.wake.notify_one();
		return Ok(format!("pinned {}", path.display()));
	}
	Ok(if pin::unpin(&pins.db, &child.entry_id).map_err(err)? {
		format!("unpinned {}; still pinned through an enclosing folder", path.display())
	} else {
		format!("unpinned {}", path.display())
	})
}

async fn handle(d: &Shared, req: Request) -> Response {
	match req {
		Request::Mount { account, mountpoint, mode } => {
			mount_new(d, account, mountpoint, mode).await
		}
		Request::Unmount { target, discard } => unmount(d, &target, discard).await,
		Request::Status => Ok(status(&*d.lock().await)),
		Request::Pause { id } => set_paused(d, &id, true).await,
		Request::Resume { id } => set_paused(d, &id, false).await,
		Request::Pin { path } => set_pin(d, &path, true).await,
		Request::Unpin { path } => set_pin(d, &path, false).await,
	}
}

/// One request, one response per connection.
async fn serve(d: Shared, stream: UnixStream) {
	let (r, mut w) = stream.into_split();
	let mut line = String::new();
	if BufReader::new(r).read_line(&mut line).await.is_err() {
		return;
	}
	let resp = match ipc::decode::<Request>(&line) {
		Ok(req) => handle(&d, req).await,
		Err(e) => Err(format!("bad request: {e}")),
	};
	if let Ok(frame) = ipc::encode(&resp) {
		let _ = w.write_all(&frame).await;
	}
}

/// Start every configured mount not running yet; failures are kept for `status`.
async fn restore_all(d: Shared) {
	let mut g = d.lock().await;
	let mounts = g.cfg.mounts.clone();
	for m in &mounts {
		if g.live.contains_key(&m.id) {
			continue;
		}
		match start(m, g.cfg.cache_max_mb).await {
			Ok(l) => {
				tracing::info!("mounted {} at {}", m.id, m.mountpoint.display());
				g.live.insert(m.id.clone(), l);
			}
			Err(e) => {
				tracing::warn!("mount {}: {e}", m.id);
				g.errors.insert(m.id.clone(), e);
			}
		}
	}
}

/// `cloudillo daemon`: serve until SIGINT / SIGTERM, then flush uploads and unmount all.
pub async fn run() -> Res<ExitCode> {
	let sock = ipc::socket_path().ok_or("no runtime or cache directory")?;
	let dir = sock.parent().ok_or("bad socket path")?;
	private_dir(dir)?;
	// Held for the daemon's lifetime: a second daemon must not mount over the first.
	let lock = std::fs::OpenOptions::new()
		.create(true)
		.truncate(false)
		.write(true)
		.open(dir.join("daemon.lock"))?;
	if lock.try_lock().is_err() {
		return Err("a cloudillo daemon is already running".into());
	}
	let _ = std::fs::remove_file(&sock);
	let listener = UnixListener::bind(&sock)?;
	std::fs::set_permissions(&sock, std::fs::Permissions::from_mode(0o600))?;

	let d: Shared = Arc::new(Mutex::new(Daemon { cfg: Config::load()?, ..Daemon::default() }));
	tokio::spawn(restore_all(Arc::clone(&d)));

	let mut term = signal(SignalKind::terminate())?;
	loop {
		tokio::select! {
			conn = listener.accept() => match conn {
				Ok((stream, _)) => {
					tokio::spawn(serve(Arc::clone(&d), stream));
				}
				Err(e) => tracing::warn!("accept: {e}"),
			},
			r = tokio::signal::ctrl_c() => {
				r?;
				break;
			}
			_ = term.recv() => break,
		}
	}

	tracing::info!("shutting down: flushing uploads, unmounting");
	let _ = std::fs::remove_file(&sock);
	let lives: Vec<Live> = d.lock().await.live.drain().map(|(_, l)| l).collect();
	let mut code = ExitCode::SUCCESS;
	for l in lives {
		if let Some(note) = stop(l, false).await {
			eprintln!("{note}");
			code = ExitCode::FAILURE;
		}
	}
	Ok(code)
}

/// Connect to the daemon, starting it detached if no socket answers, and retry for 5 s.
async fn connect(sock: &Path) -> Res<UnixStream> {
	if let Ok(s) = UnixStream::connect(sock).await {
		return Ok(s);
	}
	Command::new(std::env::current_exe()?)
		.arg("daemon")
		.stdin(Stdio::null())
		.stdout(Stdio::null())
		.stderr(Stdio::null())
		.process_group(0)
		.spawn()?;
	for _ in 0..50 {
		tokio::time::sleep(Duration::from_millis(100)).await;
		if let Ok(s) = UnixStream::connect(sock).await {
			return Ok(s);
		}
	}
	Err("cloudillo daemon did not come up within 5 s (run `cloudillo daemon` to see why)".into())
}

/// Send one request and return the daemon's answer text.
pub async fn request(req: &Request) -> Res<String> {
	let sock = ipc::socket_path().ok_or("no runtime or cache directory")?;
	let (r, mut w) = connect(&sock).await?.into_split();
	w.write_all(&ipc::encode(req)?).await?;
	let mut line = String::new();
	BufReader::new(r).read_line(&mut line).await?;
	if line.is_empty() {
		return Err("the daemon closed the connection".into());
	}
	Ok(ipc::decode::<Response>(&line)??)
}
