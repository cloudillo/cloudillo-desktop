//! Sync worker of one mount: replays the [`Journal`] in `seq` order, one op at a time.
//! Network / 5xx errors back off (1 s doubling to 5 min) and retry; any other rejection marks
//! the op failed (kept, shown in `status`). A key the probe confirms dead stops the worker
//! with the journal intact ("re-pair needed"). Every ack triggers an immediate feed poll.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::Notify;

use crate::api::Parent;
use crate::cache::BlobCache;
use crate::conflict::{self, Action, LocalChange, RemoteChange};
use crate::db::{self, Db};
use crate::error::{ClientError, Result};
use crate::feed::FeedPoller;
use crate::journal::{Journal, OpKind, PENDING_PREFIX, PendingOp};
use crate::session::Session;
use crate::tree::{self, Layout};
use crate::upload::{Replaced, Uploader};

const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(300);
/// Re-check the journal this often even without a wake-up.
const IDLE: Duration = Duration::from_secs(60);

/// Failed ops the server rejected as `(too large, quota exceeded)`, for `status`. Read from
/// `pending_ops.last_error`, which holds the error's display text.
pub fn failures(db: &Db) -> Result<(u64, u64)> {
	let (big, quota): (i64, i64) = db
		.conn()
		.query_row(
			"SELECT COALESCE(SUM(last_error = ?1), 0), COALESCE(SUM(last_error = ?2), 0)
			 FROM pending_ops WHERE failed = 1",
			[ClientError::TooLarge.to_string(), ClientError::QuotaExceeded.to_string()],
			|r| Ok((r.get(0)?, r.get(1)?)),
		)
		.map_err(db::sql)?;
	Ok((big.try_into().unwrap_or(0), quota.try_into().unwrap_or(0)))
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
	Retry,
	Fail,
	Dead,
}

// a crash / kill between the server's create and the ack replays the create on the
// next start, leaving a duplicate (a pause waits for the op); an idempotency key would need
// server support.
#[derive(Debug)]
pub struct SyncWorker {
	session: Arc<Session>,
	uploader: Arc<Uploader>,
	journal: Arc<Journal>,
	cache: Arc<BlobCache>,
	poller: Arc<FeedPoller>,
	layout: Layout,
	dead: AtomicBool,
	paused: AtomicBool,
	/// Cuts a [`Self::run`] wait short on [`Self::pause`].
	halt: Notify,
	/// Held by a [`Self::run`]: a new run waits for an old one to finish its op.
	running: tokio::sync::Mutex<()>,
}

impl SyncWorker {
	pub fn new(
		session: Arc<Session>,
		uploader: Arc<Uploader>,
		journal: Arc<Journal>,
		cache: Arc<BlobCache>,
		poller: Arc<FeedPoller>,
	) -> Self {
		let layout = Layout::new(&session);
		Self {
			session,
			uploader,
			journal,
			cache,
			poller,
			layout,
			dead: AtomicBool::new(false),
			paused: AtomicBool::new(false),
			halt: Notify::new(),
			running: tokio::sync::Mutex::new(()),
		}
	}

	pub fn journal(&self) -> &Arc<Journal> {
		&self.journal
	}

	/// The probe confirmed the key dead: nothing is replayed until re-paired.
	pub fn dead(&self) -> bool {
		self.dead.load(Ordering::Relaxed)
	}

	/// [`Self::run`] returns once the op in flight (if any) is done; never mid-op.
	pub fn pause(&self) {
		self.paused.store(true, Ordering::SeqCst);
		// notify_one keeps a permit, so a run not waiting yet still sees it
		self.halt.notify_one();
	}

	/// Undo [`Self::pause`] before running again.
	pub fn resume(&self) {
		self.paused.store(false, Ordering::SeqCst);
	}

	/// Wait for `f`, or less on [`Self::pause`].
	async fn nap(&self, f: impl Future) {
		tokio::select! {
			_ = f => {}
			() = self.halt.notified() => {}
		}
	}

	/// Replay until the key is found dead or [`Self::pause`]d (then returns). Runs never
	/// overlap: a second one starts once the first returned.
	pub async fn run(&self) {
		let _run = self.running.lock().await;
		let mut backoff = BACKOFF_MIN;
		while !self.dead() && !self.paused.load(Ordering::SeqCst) {
			let op = match self.journal.next_ready() {
				Ok(Some(op)) => op,
				Ok(None) => {
					self.nap(tokio::time::timeout(IDLE, self.journal.notified())).await;
					continue;
				}
				Err(_) => {
					self.nap(tokio::time::sleep(BACKOFF_MAX)).await;
					continue;
				}
			};
			let err = match self.replay(&op).await {
				Ok(()) => {
					backoff = BACKOFF_MIN;
					self.poller.trigger();
					continue;
				}
				Err(e) => e,
			};
			// core does not log: the error is kept as the op's `last_error` for `status`
			let msg = err.to_string();
			// a journal write failing here is dropped; the op is retried anyway
			let outcome = self.classify(&err).await;
			let r = match outcome {
				Outcome::Retry => self.journal.retry_later(op.seq, &msg),
				Outcome::Fail => self.journal.fail(op.seq, &msg),
				Outcome::Dead => {
					self.dead.store(true, Ordering::Relaxed);
					self.journal.retry_later(op.seq, &msg)
				}
			};
			// a journal write that failed leaves the op ready: back off instead of spinning
			if outcome == Outcome::Retry || r.is_err() {
				self.nap(tokio::time::sleep(backoff)).await;
				backoff = (backoff * 2).min(BACKOFF_MAX);
			}
		}
	}

	/// Retry vs fail; a 403 other than cross-room probes the mount root, and a second 403
	/// means the key is dead.
	async fn classify(&self, err: &ClientError) -> Outcome {
		match err {
			ClientError::Network(_) => Outcome::Retry,
			ClientError::Http { status, .. } if *status >= 500 || matches!(status, 408 | 429) => {
				Outcome::Retry
			}
			ClientError::Forbidden { code } if !code.ends_with("CROSSROOM") => {
				match self.session.key_dead(&self.layout.parent_of(tree::ROOT)).await {
					Ok(true) => Outcome::Dead,
					Err(ClientError::Network(_)) => Outcome::Retry,
					_ => Outcome::Fail,
				}
			}
			_ => Outcome::Fail,
		}
	}

	async fn replay(&self, op: &PendingOp) -> Result<()> {
		let id = op.entry_id.as_str();
		let name = op.name.as_deref().unwrap_or_default();
		let parent = op.parent_id.as_deref().map(|p| self.layout.parent_of(p));
		let j = &self.journal;
		match op.op {
			OpKind::Create => {
				let (Some(parent), Some(path)) = (&parent, &op.staged_blob) else {
					return Err(incomplete(op));
				};
				let fv = self.uploader.create(parent, name, path).await?;
				// a provisional `@…` id would leave later Replaces without a usable base
				let cid = match fv.content_id.as_deref() {
					Some(p) if p.starts_with('@') => self.uploader.wait(&fv.id, "", p).await,
					c => c.map(str::to_owned),
				};
				// core does not log: a failed write-through only costs a later download
				let _ = self.cache.put(cid.as_deref(), path);
				j.ack(op.seq, Some(&fv.id), cid.as_deref())
			}
			OpKind::Replace => {
				let (Some(parent), Some(path)) = (&parent, &op.staged_blob) else {
					return Err(incomplete(op));
				};
				self.replay_replace(op, parent, name, path).await
			}
			OpKind::Mkdir => {
				let parent = parent.as_ref().ok_or_else(|| incomplete(op))?;
				let fv =
					self.session.call(|api| async move { api.mkdir(parent, name).await }).await?;
				j.ack(op.seq, Some(&fv.id), None)
			}
			OpKind::Rename | OpKind::Move => {
				let nm = op.name.as_deref();
				let np = if op.op == OpKind::Move { parent.as_ref() } else { None };
				self.session.call(|api| async move { api.patch(id, nm, np).await }).await?;
				j.ack(op.seq, None, None)
			}
			OpKind::Trash => {
				match self.session.call(|api| async move { api.trash(id).await }).await {
					Ok(()) | Err(ClientError::NotFound) => j.ack(op.seq, None, None),
					Err(e) => Err(e),
				}
			}
		}
	}

	async fn replay_replace(
		&self,
		op: &PendingOp,
		parent: &Parent,
		name: &str,
		path: &Path,
	) -> Result<()> {
		let id = op.entry_id.as_str();
		let j = &self.journal;
		// a replace lands even in the trash: check first (local modified vs remote trashed)
		let remote = self.session.call(|api| async move { api.get_entry(id).await }).await?;
		if remote.parent_id.as_deref() == Some(conflict::TRASH_PARENT) {
			for a in conflict::decide(LocalChange::Modified, RemoteChange::Trashed) {
				if a == Action::Restore {
					let to = match parent {
						Parent::Folder(p) => Some(p.as_str()),
						Parent::DriveRoot { .. } => None,
					};
					self.session
						.call(|api| async move { api.restore_trashed(id, to).await })
						.await?;
				}
				// Action::Upload: the replace below
			}
		}
		// A chained base whose op acked without a final content id is unknown: the server's
		// current content may be a remote edit, so never overwrite it; save a sibling instead.
		let known_base = op
			.base_content_id
			.as_deref()
			.filter(|b| !b.starts_with(PENDING_PREFIX) && !b.starts_with('@'));
		let replaced = match known_base {
			Some(b) => self.uploader.replace(id, b, parent, name, path).await?,
			None => Replaced::Conflict(self.uploader.sibling(parent, name, path).await?),
		};
		match replaced {
			Replaced::Landed(c) => {
				let cid = c.as_deref().filter(|c| !c.starts_with('@'));
				let _ = self.cache.put(cid, path); // write-through only
				j.ack(op.seq, None, cid)
			}
			// 412: local modified vs remote modified
			Replaced::Conflict(sibling) => {
				for a in conflict::decide(LocalChange::Modified, RemoteChange::Modified) {
					match a {
						// already uploaded as `sibling`; the feed brings the row
						Action::UploadAsConflictSibling => {
							let _ = self.cache.put(sibling.content_id.as_deref(), path);
						}
						// the local content is safe in the sibling: done with the op
						Action::Pull => {
							j.ack(op.seq, None, None)?;
							// a failed pull leaves the `pending-` token, whose
							// reads download the entry's current content anyway
							if let Ok(fv) = self
								.session
								.call(|api| async move { api.get_entry(id).await })
								.await
							{
								let _ = self.poller.pull(fv);
							}
						}
						_ => {}
					}
				}
				Ok(())
			}
		}
	}
}

fn incomplete(op: &PendingOp) -> ClientError {
	ClientError::Decode(format!("journal op {} incomplete", op.seq))
}

#[cfg(test)]
mod tests {
	use std::path::PathBuf;
	use std::time::UNIX_EPOCH;

	use super::*;
	use crate::db::{EntryKind, EntryRow, LIVE};
	use crate::journal::{local_id, pending_cid};
	use crate::stub::{Log, stub};

	struct T {
		w: Arc<SyncWorker>,
		db: Arc<Db>,
		log: Log,
		src: PathBuf,
		tmp: PathBuf,
	}

	impl Drop for T {
		fn drop(&mut self) {
			let _ = std::fs::remove_dir_all(&self.tmp);
		}
	}

	impl T {
		async fn new(
			tag: &str,
			handler: impl Fn(&str) -> (u16, String) + Send + Sync + 'static,
		) -> Self {
			let (session, log) = stub(handler).await;
			let tmp = std::env::temp_dir().join(format!("cl-sync-{tag}-{}", std::process::id()));
			let db = Arc::new(Db::open_in_memory().unwrap());
			let journal = Arc::new(Journal::new(Arc::clone(&db), tmp.join("staged")).unwrap());
			let cache =
				Arc::new(BlobCache::new(tmp.join("blobs"), 1 << 20, Arc::clone(&db)).unwrap());
			let poller = Arc::new(FeedPoller::new(Arc::clone(&session), Arc::clone(&db)));
			let uploader = Arc::new(Uploader::new(Arc::clone(&session), None));
			let w = Arc::new(SyncWorker::new(session, uploader, journal, cache, poller));
			let src = tmp.join("src");
			std::fs::write(&src, b"hi").unwrap();
			Self { w, db, log, src, tmp }
		}

		/// Enqueue `op` (staged from `src` if it carries content) and a row for its entry
		/// with content `cid` (`None` = the op's `pending-<seq>`). Returns the entry id.
		fn enqueue(&self, mut op: PendingOp, content: bool, cid: Option<&str>) -> String {
			op.parent_id = Some(tree::ROOT.into());
			op.name.get_or_insert_with(|| "f".into());
			let seq = self.w.journal.enqueue(&op, content.then_some(self.src.as_path())).unwrap();
			let id = if op.op == OpKind::Create { local_id(seq) } else { op.entry_id.clone() };
			self.db
				.upsert(&row(&id, cid.map_or_else(|| pending_cid(seq), str::to_owned)))
				.unwrap();
			id
		}

		async fn replay_next(&self) -> Result<()> {
			let op = self.w.journal.next_ready()?.ok_or(ClientError::NotFound)?;
			self.w.replay(&op).await
		}

		fn logged(&self, needle: &str) -> bool {
			self.log.lock().unwrap().iter().any(|h| h.contains(needle))
		}
	}

	fn row(id: &str, cid: String) -> EntryRow {
		EntryRow {
			ino: 0,
			entry_id: id.into(),
			parent_id: tree::ROOT.into(),
			drive: None,
			remote_name: "f".into(),
			local_name: "f".into(),
			kind: EntryKind::File,
			content_id: Some(cid),
			size: 2,
			mtime: UNIX_EPOCH,
			created_at: String::new(),
			updated_at: 0,
			status: LIVE.into(),
			hydrated: false,
		}
	}

	fn data(json: &str) -> (u16, String) {
		(200, format!(r#"{{"data":{json}}}"#))
	}

	fn entry(cid: &str) -> (u16, String) {
		data(&format!(r#"{{"id":"E1","contentId":"{cid}","parentId":"__root__","fileName":"f"}}"#))
	}

	#[tokio::test]
	async fn replay_create_acks_real_id() {
		let t = T::new("create", |l| {
			if l.starts_with("POST /api/uploads/standard") {
				data(r#"{"id":"E1","contentId":"c1","fileName":"f"}"#)
			} else {
				(500, String::new())
			}
		})
		.await;
		t.enqueue(PendingOp::new(OpKind::Create, "", "f"), true, None);
		t.replay_next().await.unwrap();
		assert_eq!(t.w.journal.counts().unwrap(), (0, 0));
		assert_eq!(t.db.get("E1").unwrap().unwrap().content_id.as_deref(), Some("c1"));
		assert!(t.w.cache.get("c1").is_some());
	}

	fn replace(base: &str) -> PendingOp {
		let mut op = PendingOp::new(OpKind::Replace, "E1", "f");
		op.base_content_id = Some(base.into());
		op
	}

	#[tokio::test]
	async fn replay_replace_lands() {
		let put = std::sync::atomic::AtomicBool::new(false);
		let t = T::new("replace", move |l| {
			if l.starts_with("PUT /api/files/E1/content") {
				put.store(true, Ordering::SeqCst);
				data(r#"{"entryId":"E1","pendingFileId":"@c1"}"#)
			} else if l.starts_with("GET /api/files/E1/metadata") {
				entry(if put.load(Ordering::SeqCst) { "c1" } else { "c0" })
			} else {
				(500, String::new())
			}
		})
		.await;
		t.enqueue(replace("c0"), true, None);
		t.replay_next().await.unwrap();
		assert!(t.logged("if-match: \"c0\""));
		assert_eq!(t.w.journal.counts().unwrap(), (0, 0));
		assert_eq!(t.db.get("E1").unwrap().unwrap().content_id.as_deref(), Some("c1"));
	}

	fn conflicting(l: &str) -> (u16, String) {
		if l.starts_with("PUT ") {
			(412, String::new())
		} else if l.starts_with("POST /api/uploads/standard") {
			data(r#"{"id":"S1","contentId":"s1","fileName":"f2"}"#)
		} else if l.starts_with("GET /api/files/E1/metadata") {
			entry("c9")
		} else {
			(500, String::new())
		}
	}

	#[tokio::test]
	async fn replay_replace_412_saves_sibling() {
		let t = T::new("conflict", conflicting).await;
		t.enqueue(replace("c0"), true, None);
		t.replay_next().await.unwrap();
		assert!(
			t.log
				.lock()
				.unwrap()
				.iter()
				.any(|h| h.starts_with("post ") && h.contains("conflict"))
		);
		assert_eq!(t.w.journal.counts().unwrap(), (0, 0));
	}

	#[tokio::test]
	async fn replay_replace_unknown_base_never_puts() {
		let t = T::new("unknown-base", conflicting).await;
		t.enqueue(replace("pending-9"), true, None);
		t.replay_next().await.unwrap();
		assert!(!t.logged("put "));
		assert!(t.logged("conflict"));
		assert_eq!(t.w.journal.counts().unwrap(), (0, 0));
	}

	#[tokio::test]
	async fn replay_trash_404_acks() {
		let t = T::new("trash", |_| (404, String::new())).await;
		t.enqueue(PendingOp::new(OpKind::Trash, "E1", "f"), false, Some("c0"));
		t.replay_next().await.unwrap();
		assert_eq!(t.w.journal.counts().unwrap(), (0, 0));
	}

	fn forbidden(code: &str) -> ClientError {
		ClientError::Forbidden { code: code.into() }
	}

	#[tokio::test]
	async fn classify_outcomes() {
		let t = T::new("classify", |_| (403, String::new())).await;
		let http = ClientError::Http { status: 503, code: String::new(), message: String::new() };
		assert_eq!(t.w.classify(&http).await, Outcome::Retry);
		assert_eq!(t.w.classify(&forbidden("E-FILE-CROSSROOM")).await, Outcome::Fail);
		assert!(t.log.lock().unwrap().is_empty());
		assert_eq!(t.w.classify(&forbidden("E-AUTH-FORBIDDEN")).await, Outcome::Dead);
		assert!(t.logged("get /api/files?limit=1"));

		let t = T::new("classify-ok", |_| data("[]")).await;
		assert_eq!(t.w.classify(&forbidden("E-AUTH-FORBIDDEN")).await, Outcome::Fail);
	}

	#[tokio::test]
	async fn pause_stops_run() {
		let t = T::new("pause", |_| (500, String::new())).await;
		let w = Arc::clone(&t.w);
		let run = tokio::spawn(async move { w.run().await });
		tokio::task::yield_now().await;
		t.w.pause();
		assert!(tokio::time::timeout(Duration::from_secs(1), run).await.is_ok());
	}

	/// Pause + resume + a new run while the first is mid-op: the op is replayed once.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn runs_never_overlap() {
		let (tx, rx) = std::sync::mpsc::channel::<()>();
		let rx = std::sync::Mutex::new(rx);
		let t = T::new("overlap", move |l| {
			if l.starts_with("DELETE /api/files/E1") {
				let _ = rx.lock().unwrap().recv_timeout(Duration::from_secs(2));
				(200, "{}".into())
			} else {
				(500, String::new())
			}
		})
		.await;
		t.enqueue(PendingOp::new(OpKind::Trash, "E1", "f"), false, Some("c0"));
		let w = Arc::clone(&t.w);
		let run1 = tokio::spawn(async move { w.run().await });
		while !t.logged("delete /api/files/e1") {
			tokio::time::sleep(Duration::from_millis(10)).await;
		}
		t.w.pause();
		t.w.resume();
		let w = Arc::clone(&t.w);
		let run2 = tokio::spawn(async move { w.run().await });
		tokio::time::sleep(Duration::from_millis(50)).await;
		tx.send(()).unwrap();
		while t.w.journal.counts().unwrap() != (0, 0) {
			tokio::time::sleep(Duration::from_millis(10)).await;
		}
		t.w.pause();
		for run in [run1, run2] {
			assert!(tokio::time::timeout(Duration::from_secs(5), run).await.is_ok());
		}
		let deletes = t.log.lock().unwrap().iter().filter(|h| h.starts_with("delete ")).count();
		assert_eq!(deletes, 1);
	}
}
