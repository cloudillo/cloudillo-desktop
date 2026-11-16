//! Pending-op journal (`pending_ops` in the mount's `meta.sqlite`): every mutation made through
//! FUSE in `on-demand` mode, replayed in `seq` order by the [`crate::sync::SyncWorker`].
//!
//! Staged content lives in `<dir>/<seq>` until the op is acknowledged. A locally created
//! entry gets the temporary id `local:<seq>`, staged content the content token `pending-<seq>`;
//! [`Journal::ack`] rewrites both to the real ids (rows, queued ops) and keeps an alias so ids
//! still held in memory resolve ([`Journal::resolve`]).

use std::io;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{OptionalExtension, Row, params};
use tokio::sync::Notify;

use crate::db::{Db, drive_col, sql};
use crate::error::Result;

/// Prefix of a temporary entry id: `local:<seq>` (the creating op's seq).
pub const LOCAL_PREFIX: &str = "local:";
/// Prefix of a temporary content id: `pending-<seq>` (content staged by op `seq`).
pub const PENDING_PREFIX: &str = "pending-";

pub fn local_id(seq: i64) -> String {
	format!("{LOCAL_PREFIX}{seq}")
}

pub fn pending_cid(seq: i64) -> String {
	format!("{PENDING_PREFIX}{seq}")
}

/// [`Journal::resolve`] on a connection the caller already holds.
fn resolve_in(conn: &rusqlite::Connection, id: &str) -> Result<String> {
	if !id.starts_with(LOCAL_PREFIX) && !id.starts_with(PENDING_PREFIX) {
		return Ok(id.to_owned());
	}
	let real: Option<String> = conn
		.query_row("SELECT real_id FROM id_alias WHERE local_id = ?1", [id], |r| r.get(0))
		.optional()
		.map_err(sql)?;
	Ok(real.unwrap_or_else(|| id.to_owned()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
	/// New file: `parent_id`, `name`, `staged_blob`.
	Create,
	/// New content of `entry_id`: `staged_blob`, `base_content_id` (`If-Match`); `parent_id`
	/// and `name` place a conflict sibling.
	Replace,
	/// New folder: `parent_id`, `name`.
	Mkdir,
	/// Same folder, new `name`.
	Rename,
	/// New `parent_id`, and `name` if it changed too.
	Move,
	/// To the trash.
	Trash,
}

impl OpKind {
	fn encode(self) -> &'static str {
		match self {
			Self::Create => "create",
			Self::Replace => "replace",
			Self::Mkdir => "mkdir",
			Self::Rename => "rename",
			Self::Move => "move",
			Self::Trash => "trash",
		}
	}

	/// Keep in sync with [`KNOWN_OPS`].
	fn decode(s: &str) -> Option<Self> {
		Some(match s {
			"create" => Self::Create,
			"replace" => Self::Replace,
			"mkdir" => Self::Mkdir,
			"rename" => Self::Rename,
			"move" => Self::Move,
			"trash" => Self::Trash,
			_ => return None,
		})
	}

	/// Creates an entry: the journal assigns its `local:<seq>` id.
	fn creates(self) -> bool {
		matches!(self, Self::Create | Self::Mkdir)
	}
}

/// One journaled op. [`Journal::enqueue`] ignores `seq`, `created_at`, `attempts`,
/// `last_error`, `failed` and `staged_blob` (and `entry_id` for Create/Mkdir).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingOp {
	pub seq: i64,
	pub op: OpKind,
	pub entry_id: String,
	/// Local name, for messages only.
	pub local_path_hint: String,
	/// Node id (entry id or [`crate::tree::ROOT`] / room node sentinel).
	pub parent_id: Option<String>,
	/// `None` = main drive.
	pub drive: Option<String>,
	/// Remote name.
	pub name: Option<String>,
	pub staged_blob: Option<PathBuf>,
	pub base_content_id: Option<String>,
	/// Unix seconds.
	pub created_at: i64,
	pub attempts: i64,
	pub last_error: Option<String>,
	/// Rejected by the server; kept, surfaced in `status`, never retried automatically.
	pub failed: bool,
}

impl PendingOp {
	pub fn new(op: OpKind, entry_id: &str, local_path_hint: &str) -> Self {
		Self {
			seq: 0,
			op,
			entry_id: entry_id.to_owned(),
			local_path_hint: local_path_hint.to_owned(),
			parent_id: None,
			drive: None,
			name: None,
			staged_blob: None,
			base_content_id: None,
			created_at: 0,
			attempts: 0,
			last_error: None,
			failed: false,
		}
	}
}

/// SQL list of every op [`OpKind::decode`] knows; keep in sync.
const KNOWN_OPS: &str = "('create','replace','mkdir','rename','move','trash')";

fn op_row(r: &Row<'_>) -> rusqlite::Result<PendingOp> {
	let op: String = r.get("op")?;
	let drive: String = r.get("drive")?;
	let Some(op) = OpKind::decode(&op) else {
		return Err(rusqlite::Error::FromSqlConversionFailure(
			0,
			rusqlite::types::Type::Text,
			Box::new(io::Error::other(format!("unknown journal op {op}"))),
		));
	};
	Ok(PendingOp {
		seq: r.get("seq")?,
		op,
		entry_id: r.get("entry_id")?,
		local_path_hint: r.get("local_path_hint")?,
		parent_id: r.get("parent_id")?,
		drive: (!drive.is_empty()).then_some(drive),
		name: r.get("name")?,
		staged_blob: r.get::<_, Option<String>>("staged_blob")?.map(PathBuf::from),
		base_content_id: r.get("base_content_id")?,
		created_at: r.get("created_at")?,
		attempts: r.get("attempts")?,
		last_error: r.get("last_error")?,
		failed: r.get("failed")?,
	})
}

fn now_secs() -> i64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

/// The journal of one mount; share as `Arc<Journal>` (FUSE enqueues, the worker replays).
#[derive(Debug)]
pub struct Journal {
	db: Arc<Db>,
	/// `…/<mount-id>/staged/`.
	dir: PathBuf,
	wake: Notify,
	part_seq: AtomicU64,
}

impl Journal {
	/// `dir` (staged content) is created 0700 if missing; leftover `.part` copies removed.
	pub fn new(db: Arc<Db>, dir: PathBuf) -> Result<Self> {
		std::fs::DirBuilder::new().recursive(true).mode(0o700).create(&dir)?;
		for e in std::fs::read_dir(&dir)?.flatten() {
			if e.file_name().to_string_lossy().ends_with(".part") {
				let _ = std::fs::remove_file(e.path());
			}
		}
		Ok(Self { db, dir, wake: Notify::new(), part_seq: AtomicU64::new(0) })
	}

	/// Resolves once an op was enqueued since the last call (coalescing).
	pub async fn notified(&self) {
		self.wake.notified().await;
	}

	/// Append `op`; `content` is copied (and synced) into `<dir>/<seq>` first. Ids and the
	/// base still in their temporary form are resolved. Returns the op's `seq`; a Create /
	/// Mkdir entry is [`local_id`]`(seq)`.
	pub fn enqueue(&self, op: &PendingOp, content: Option<&Path>) -> Result<i64> {
		let part = match content {
			Some(src) => {
				let n = self.part_seq.fetch_add(1, Ordering::Relaxed);
				let part = self.dir.join(format!("{}-{n}.part", std::process::id()));
				let res =
					std::fs::copy(src, &part).and_then(|_| std::fs::File::open(&part)?.sync_all());
				if let Err(e) = res {
					let _ = std::fs::remove_file(&part);
					return Err(e.into());
				}
				Some(part)
			}
			None => None,
		};
		let res = self.insert(op, part.as_deref());
		if res.is_err()
			&& let Some(p) = &part
		{
			let _ = std::fs::remove_file(p);
		}
		let seq = res?;
		self.wake.notify_one();
		Ok(seq)
	}

	fn insert(&self, op: &PendingOp, part: Option<&Path>) -> Result<i64> {
		let mut conn = self.db.conn();
		let tx = conn.transaction().map_err(sql)?;
		// resolved under the lock, so a concurrent ack cannot rewrite an id in between
		let entry_id = resolve_in(&tx, &op.entry_id)?;
		let parent_id = op.parent_id.as_deref().map(|p| resolve_in(&tx, p)).transpose()?;
		let base = op.base_content_id.as_deref().map(|b| resolve_in(&tx, b)).transpose()?;
		tx.execute(
			"INSERT INTO pending_ops (op, entry_id, local_path_hint, parent_id, drive, name,
				base_content_id, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
			params![
				op.op.encode(),
				entry_id,
				op.local_path_hint,
				parent_id,
				drive_col(op.drive.as_deref()),
				op.name,
				base,
				now_secs(),
			],
		)
		.map_err(sql)?;
		let seq = tx.last_insert_rowid();
		if op.op.creates() {
			tx.execute(
				"UPDATE pending_ops SET entry_id = ?2 WHERE seq = ?1",
				params![seq, local_id(seq)],
			)
			.map_err(sql)?;
		}
		if let Some(part) = part {
			let dest = self.staged_path(seq);
			std::fs::rename(part, &dest)?;
			let dest_s = dest.to_str().ok_or_else(|| io::Error::other("non-UTF-8 cache dir"))?;
			tx.execute(
				"UPDATE pending_ops SET staged_blob = ?2 WHERE seq = ?1",
				params![seq, dest_s],
			)
			.map_err(sql)?;
		}
		tx.commit().map_err(sql)?;
		Ok(seq)
	}

	/// Where op `seq` keeps its staged content.
	pub fn staged_path(&self, seq: i64) -> PathBuf {
		self.dir.join(seq.to_string())
	}

	/// Staged file behind content token `pending-<seq>`, while its op is unacknowledged.
	pub fn pending_blob(&self, content_id: &str) -> Option<PathBuf> {
		let seq: i64 = content_id.strip_prefix(PENDING_PREFIX)?.parse().ok()?;
		let p = self.staged_path(seq);
		p.is_file().then_some(p)
	}

	/// Oldest op to replay: not failed, and no earlier op left on its entry or its parent
	/// (so ops of one entry run strictly in order, and nothing waits on a failed op's entry).
	/// Rows with an op this build does not know (written by a newer one) are flagged failed
	/// first, so they cannot wedge the queue.
	pub fn next_ready(&self) -> Result<Option<PendingOp>> {
		let conn = self.db.conn();
		conn.execute(
			&format!(
				"UPDATE pending_ops SET failed = 1, last_error = 'unknown journal op ' || op
					WHERE failed = 0 AND op NOT IN {KNOWN_OPS}"
			),
			[],
		)
		.map_err(sql)?;
		conn.query_row(
			"SELECT * FROM pending_ops o WHERE failed = 0 AND NOT EXISTS (
					SELECT 1 FROM pending_ops f WHERE f.seq < o.seq
					AND (f.entry_id = o.entry_id OR f.entry_id = o.parent_id))
				ORDER BY seq LIMIT 1",
			[],
			op_row,
		)
		.optional()
		.map_err(sql)
	}

	/// Whether any op (failed or not) is still queued on `entry_id`.
	pub fn has_pending(&self, entry_id: &str) -> Result<bool> {
		self.db
			.conn()
			.query_row(
				"SELECT EXISTS(SELECT 1 FROM pending_ops WHERE entry_id = ?1)",
				[entry_id],
				|r| r.get(0),
			)
			.map_err(sql)
	}

	/// Op `seq` landed. `real_entry_id`: the entry it created (rewrites `local:<seq>`);
	/// `content_id`: the content it landed (rewrites `pending-<seq>`; `None` keeps the token,
	/// whose reads then fall back to downloading the entry). Deletes the op and its staged
	/// file.
	pub fn ack(
		&self,
		seq: i64,
		real_entry_id: Option<&str>,
		content_id: Option<&str>,
	) -> Result<()> {
		let blob = {
			let mut conn = self.db.conn();
			let tx = conn.transaction().map_err(sql)?;
			let blob: Option<Option<String>> = tx
				.query_row("SELECT staged_blob FROM pending_ops WHERE seq = ?1", [seq], |r| {
					r.get(0)
				})
				.optional()
				.map_err(sql)?;
			if let Some(real) = real_entry_id {
				let tmp = local_id(seq);
				// a feed round between the server's create and this ack may have added it
				tx.execute("DELETE FROM entries WHERE entry_id = ?1", [real]).map_err(sql)?;
				for q in [
					"UPDATE entries SET entry_id = ?2 WHERE entry_id = ?1",
					"UPDATE entries SET parent_id = ?2 WHERE parent_id = ?1",
					"UPDATE pending_ops SET entry_id = ?2 WHERE entry_id = ?1",
					"UPDATE pending_ops SET parent_id = ?2 WHERE parent_id = ?1",
					"INSERT OR REPLACE INTO id_alias (local_id, real_id) VALUES (?1, ?2)",
				] {
					tx.execute(q, params![tmp, real]).map_err(sql)?;
				}
			}
			if let Some(cid) = content_id {
				let tok = pending_cid(seq);
				for q in [
					"UPDATE entries SET content_id = ?2, hydrated = EXISTS(
						SELECT 1 FROM cache_lru WHERE content_id = ?2) WHERE content_id = ?1",
					"UPDATE pending_ops SET base_content_id = ?2 WHERE base_content_id = ?1",
					"INSERT OR REPLACE INTO id_alias (local_id, real_id) VALUES (?1, ?2)",
				] {
					tx.execute(q, params![tok, cid]).map_err(sql)?;
				}
			}
			tx.execute("DELETE FROM pending_ops WHERE seq = ?1", [seq]).map_err(sql)?;
			tx.commit().map_err(sql)?;
			blob.flatten()
		};
		// a failed removal leaves an orphan in `staged/` (acknowledged content)
		if let Some(b) = blob {
			let _ = std::fs::remove_file(b);
		}
		Ok(())
	}

	/// The server rejected op `seq`: keep it, flagged failed (shown in `status`).
	pub fn fail(&self, seq: i64, error: &str) -> Result<()> {
		self.note(seq, error, true)
	}

	/// A transient failure of op `seq` (offline, 5xx): count it, retried later.
	pub fn retry_later(&self, seq: i64, error: &str) -> Result<()> {
		self.note(seq, error, false)
	}

	fn note(&self, seq: i64, error: &str, failed: bool) -> Result<()> {
		self.db
			.conn()
			.execute(
				"UPDATE pending_ops SET attempts = attempts + 1, last_error = ?2,
					failed = failed OR ?3 WHERE seq = ?1",
				params![seq, error, failed],
			)
			.map_err(sql)?;
		Ok(())
	}

	/// Real id of a temporary `local:<seq>` id or `pending-<seq>` token once acknowledged;
	/// anything else unchanged.
	// aliases are never pruned (one small row per created entry / replace).
	pub fn resolve(&self, id: &str) -> Result<String> {
		resolve_in(&self.db.conn(), id)
	}

	/// `(unacknowledged ops, of which failed)`.
	pub fn counts(&self) -> Result<(u64, u64)> {
		let (n, f): (i64, i64) = self
			.db
			.conn()
			.query_row("SELECT COUNT(*), COALESCE(SUM(failed), 0) FROM pending_ops", [], |r| {
				Ok((r.get(0)?, r.get(1)?))
			})
			.map_err(sql)?;
		Ok((n.try_into().unwrap_or(0), f.try_into().unwrap_or(0)))
	}
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use super::*;
	use crate::db::{EntryKind, EntryRow, LIVE};

	fn row(id: &str, parent: &str, cid: &str) -> EntryRow {
		EntryRow {
			ino: 0,
			entry_id: id.into(),
			parent_id: parent.into(),
			drive: None,
			remote_name: id.into(),
			local_name: id.into(),
			kind: EntryKind::File,
			content_id: Some(cid.into()),
			size: 1,
			mtime: UNIX_EPOCH + Duration::from_secs(1),
			created_at: String::new(),
			updated_at: 0,
			status: LIVE.into(),
			hydrated: false,
		}
	}

	#[test]
	fn enqueue_ack_rewrites_temp_ids() -> Result<()> {
		let tmp = std::env::temp_dir().join(format!("cl-journal-{}", std::process::id()));
		let db = Arc::new(Db::open_in_memory()?);
		let j = Journal::new(Arc::clone(&db), tmp.join("staged"))?;
		let src = tmp.join("src");
		std::fs::write(&src, b"hi")?;

		let mut mk = PendingOp::new(OpKind::Mkdir, "", "d");
		mk.parent_id = Some(crate::tree::ROOT.into());
		let s1 = j.enqueue(&mk, None)?;
		let dir = local_id(s1);
		assert_eq!(j.resolve(&dir)?, dir);
		let mut cr = PendingOp::new(OpKind::Create, "", "f");
		cr.parent_id = Some(dir.clone());
		let s2 = j.enqueue(&cr, Some(&src))?;
		let file = local_id(s2);
		let ino = db.upsert(&row(&file, &dir, &pending_cid(s2)))?;
		assert_eq!(j.pending_blob(&pending_cid(s2)), Some(j.staged_path(s2)));
		let mut rp = PendingOp::new(OpKind::Replace, &file, "f");
		rp.base_content_id = Some(pending_cid(s2));
		let s3 = j.enqueue(&rp, Some(&src))?;
		assert_eq!(j.counts()?, (3, 0));

		// in order; the mkdir failing blocks what lives in it
		assert_eq!(j.next_ready()?.map(|o| o.seq), Some(s1));
		j.fail(s1, "nope")?;
		assert_eq!(j.next_ready()?, None);
		db.conn().execute("UPDATE pending_ops SET failed = 0", []).map_err(sql)?;

		j.ack(s1, Some("D"), None)?;
		let op = j.next_ready()?.ok_or(crate::ClientError::NotFound)?;
		assert_eq!((op.seq, op.parent_id.as_deref()), (s2, Some("D")));
		j.ack(s2, Some("F"), Some("c2"))?;
		assert!(j.pending_blob(&pending_cid(s2)).is_none());
		let r = db.get("F")?.ok_or(crate::ClientError::NotFound)?;
		assert_eq!((r.ino, r.parent_id.as_str(), r.content_id.as_deref()), (ino, "D", Some("c2")));
		let op = j.next_ready()?.ok_or(crate::ClientError::NotFound)?;
		assert_eq!((op.seq, op.entry_id.as_str()), (s3, "F"));
		assert_eq!(op.base_content_id.as_deref(), Some("c2"));
		assert_eq!(j.resolve(&file)?, "F");
		assert_eq!(j.resolve(&pending_cid(s2))?, "c2");
		j.ack(s3, None, Some("c3"))?;
		assert_eq!(j.counts()?, (0, 0));
		assert_eq!(j.resolve("e1")?, "e1");

		// acked without a content id: the token stays (and still resolves to itself)
		let s4 = j.enqueue(&PendingOp::new(OpKind::Replace, "F", "f"), Some(&src))?;
		db.upsert(&row("F", "D", &pending_cid(s4)))?;
		j.ack(s4, None, None)?;
		let r = db.get("F")?.ok_or(crate::ClientError::NotFound)?;
		assert_eq!(r.content_id, Some(pending_cid(s4)));
		assert_eq!(j.resolve(&pending_cid(s4))?, pending_cid(s4));
		let _ = std::fs::remove_dir_all(&tmp);
		Ok(())
	}

	fn journal(tag: &str) -> Result<(Arc<Db>, Journal, PathBuf)> {
		let tmp = std::env::temp_dir().join(format!("cl-journal-{tag}-{}", std::process::id()));
		let db = Arc::new(Db::open_in_memory()?);
		let j = Journal::new(Arc::clone(&db), tmp.join("staged"))?;
		Ok((db, j, tmp))
	}

	#[test]
	fn op_kind_round_trips() {
		use OpKind::{Create, Mkdir, Move, Rename, Replace, Trash};
		for k in [Create, Replace, Mkdir, Rename, Move, Trash] {
			assert_eq!(OpKind::decode(k.encode()), Some(k));
		}
		assert_eq!(OpKind::decode("bogus"), None);
	}

	#[test]
	fn unknown_op_is_failed_not_blocking() -> Result<()> {
		let (db, j, tmp) = journal("unknown")?;
		j.enqueue(&PendingOp::new(OpKind::Trash, "e1", "a"), None)?;
		db.conn().execute("UPDATE pending_ops SET op = 'bogus'", []).map_err(sql)?;
		let b = j.enqueue(&PendingOp::new(OpKind::Trash, "e2", "b"), None)?;
		assert!(!j.has_pending("e3")?);
		assert!(j.has_pending("e1")?);
		assert_eq!(j.next_ready()?.map(|o| o.seq), Some(b));
		assert_eq!(j.counts()?, (2, 1));
		let _ = std::fs::remove_dir_all(&tmp);
		Ok(())
	}

	#[test]
	fn retry_later_and_fail() -> Result<()> {
		let (_db, j, tmp) = journal("retry")?;
		let a = j.enqueue(&PendingOp::new(OpKind::Rename, "e1", "a"), None)?;
		let b = j.enqueue(&PendingOp::new(OpKind::Rename, "e2", "b"), None)?;
		j.retry_later(a, "net")?;
		let op = j.next_ready()?.ok_or(crate::ClientError::NotFound)?;
		assert_eq!(
			(op.seq, op.attempts, op.last_error.as_deref(), op.failed),
			(a, 1, Some("net"), false)
		);
		j.fail(a, "no")?;
		assert_eq!(j.next_ready()?.map(|o| o.seq), Some(b));
		assert_eq!(j.counts()?, (2, 1));
		j.enqueue(&PendingOp::new(OpKind::Rename, "e1", "a"), None)?;
		j.ack(b, None, None)?;
		// the new op on e1 waits behind the failed one
		assert_eq!(j.next_ready()?, None);
		let _ = std::fs::remove_dir_all(&tmp);
		Ok(())
	}
}
