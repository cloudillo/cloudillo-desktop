// SPDX-FileCopyrightText: Szilárd Hajba
// SPDX-License-Identifier: MPL-2.0

//! Per-mount metadata DB (`meta.sqlite`): entries, feed cursors, blob-cache LRU.
//!
//! Sentinels instead of SQL `NULL` so the partial unique index bites: top-level rows carry a
//! node id such as [`crate::tree::ROOT`] as `parent_id`, and the main drive is `''`.

use std::io;
use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, Row, params};

use crate::error::{ClientError, Result};

/// `status` of a row the tree shows.
pub const LIVE: &str = "live";
/// `status` of a row gone remotely; kept so `ino` is never reused.
pub const REMOVED: &str = "removed";

/// Numbered migrations; `PRAGMA user_version` = how many have run.
const MIGRATIONS: &[&str] = &[
	"
	CREATE TABLE entries (
		ino INTEGER PRIMARY KEY AUTOINCREMENT,
		entry_id TEXT UNIQUE NOT NULL,
		parent_id TEXT NOT NULL,
		drive TEXT NOT NULL,
		remote_name TEXT NOT NULL,
		local_name TEXT NOT NULL,
		kind TEXT NOT NULL,
		content_id TEXT,
		size INTEGER NOT NULL,
		mtime INTEGER NOT NULL,
		created_at TEXT NOT NULL,
		updated_at INTEGER NOT NULL,
		status TEXT NOT NULL,
		hydrated INTEGER NOT NULL DEFAULT 0
	);
	CREATE UNIQUE INDEX entries_name ON entries(parent_id, drive, local_name)
		WHERE status = 'live';
	CREATE INDEX entries_content ON entries(content_id);
	CREATE TABLE feed_cursor (drive TEXT PRIMARY KEY, since INTEGER NOT NULL, after TEXT);
	CREATE TABLE cache_lru (
		content_id TEXT PRIMARY KEY,
		size INTEGER NOT NULL,
		last_access INTEGER NOT NULL
	);
",
	"
	CREATE TABLE pending_ops (
		seq INTEGER PRIMARY KEY AUTOINCREMENT,
		op TEXT NOT NULL,
		entry_id TEXT NOT NULL,
		local_path_hint TEXT NOT NULL,
		parent_id TEXT,
		drive TEXT NOT NULL,
		name TEXT,
		staged_blob TEXT,
		base_content_id TEXT,
		created_at INTEGER NOT NULL,
		attempts INTEGER NOT NULL DEFAULT 0,
		last_error TEXT,
		failed INTEGER NOT NULL DEFAULT 0
	);
	CREATE INDEX pending_ops_entry ON pending_ops(entry_id);
	CREATE TABLE id_alias (local_id TEXT PRIMARY KEY, real_id TEXT NOT NULL);
",
	"
	ALTER TABLE entries ADD COLUMN pinned INTEGER NOT NULL DEFAULT 0;
",
];

/// CTE `p(id)`: live entries pinned themselves or below a pinned live entry.
const PINNED_TREE: &str = "WITH RECURSIVE p(id) AS (
	SELECT entry_id FROM entries WHERE pinned = 1 AND status = 'live'
	UNION SELECT e.entry_id FROM entries e JOIN p ON e.parent_id = p.id WHERE e.status = 'live')";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryKind {
	File,
	Folder,
	/// CRDT/RTDB entry; `app` is what `launcher::json` takes (content type, else file type).
	Launcher {
		app: String,
	},
}

impl EntryKind {
	fn encode(&self) -> String {
		match self {
			Self::File => "file".into(),
			Self::Folder => "folder".into(),
			Self::Launcher { app } => format!("launcher:{app}"),
		}
	}

	fn decode(s: &str) -> Self {
		match s {
			"folder" => Self::Folder,
			_ => s
				.strip_prefix("launcher:")
				.map_or(Self::File, |app| Self::Launcher { app: app.to_owned() }),
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryRow {
	/// Assigned by the DB; ignored by [`Db::upsert`].
	pub ino: u64,
	pub entry_id: String,
	/// Parent entry id or node id sentinel; never empty.
	pub parent_id: String,
	/// `None` = main drive, `Some("@tenant~name")` = room drive.
	pub drive: Option<String>,
	pub remote_name: String,
	pub local_name: String,
	pub kind: EntryKind,
	pub content_id: Option<String>,
	pub size: u64,
	pub mtime: SystemTime,
	/// RFC3339, as the server sends it.
	pub created_at: String,
	/// Feed `updatedAt`, unix seconds.
	pub updated_at: i64,
	/// [`LIVE`] or [`REMOVED`].
	pub status: String,
	pub hydrated: bool,
}

/// rusqlite errors surface as `ClientError::Io` (error.rs has no SQL variant).
pub(crate) fn sql(e: rusqlite::Error) -> ClientError {
	ClientError::Io(io::Error::other(e))
}

pub(crate) fn drive_col(drive: Option<&str>) -> &str {
	drive.unwrap_or("")
}

fn to_i64(v: u64) -> i64 {
	i64::try_from(v).unwrap_or(i64::MAX)
}

fn millis(t: SystemTime) -> i64 {
	t.duration_since(UNIX_EPOCH)
		.map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

fn row(r: &Row<'_>) -> rusqlite::Result<EntryRow> {
	let drive: String = r.get("drive")?;
	let kind: String = r.get("kind")?;
	let mtime: i64 = r.get("mtime")?;
	Ok(EntryRow {
		ino: r.get::<_, i64>("ino")?.try_into().unwrap_or(0),
		entry_id: r.get("entry_id")?,
		parent_id: r.get("parent_id")?,
		drive: (!drive.is_empty()).then_some(drive),
		remote_name: r.get("remote_name")?,
		local_name: r.get("local_name")?,
		kind: EntryKind::decode(&kind),
		content_id: r.get("content_id")?,
		size: r.get::<_, i64>("size")?.try_into().unwrap_or(0),
		mtime: UNIX_EPOCH + Duration::from_millis(mtime.try_into().unwrap_or(0)),
		created_at: r.get("created_at")?,
		updated_at: r.get("updated_at")?,
		status: r.get("status")?,
		hydrated: r.get("hydrated")?,
	})
}

/// One connection behind a mutex; shared via `Arc<Db>` by the feed, FUSE and the cache.
// one global lock; a read pool if FUSE lookups ever queue behind the feed.
#[derive(Debug)]
pub struct Db {
	conn: Mutex<Connection>,
}

impl Db {
	/// Open (creating if absent) and migrate.
	pub fn open(path: &Path) -> Result<Self> {
		Self::init(Connection::open(path).map_err(sql)?)
	}

	pub fn open_in_memory() -> Result<Self> {
		Self::init(Connection::open_in_memory().map_err(sql)?)
	}

	fn init(mut conn: Connection) -> Result<Self> {
		conn.pragma_update(None, "journal_mode", "WAL").map_err(sql)?;
		let done: i64 = conn.pragma_query_value(None, "user_version", |r| r.get(0)).map_err(sql)?;
		for (i, m) in (1_i64..).zip(MIGRATIONS).skip(usize::try_from(done).unwrap_or(0)) {
			let tx = conn.transaction().map_err(sql)?;
			tx.execute_batch(m).map_err(sql)?;
			tx.pragma_update(None, "user_version", i).map_err(sql)?;
			tx.commit().map_err(sql)?;
		}
		Ok(Self { conn: Mutex::new(conn) })
	}

	pub(crate) fn conn(&self) -> MutexGuard<'_, Connection> {
		crate::lock(&self.conn)
	}

	/// Insert or update by `entry_id`, keeping `ino`; returns the row's `ino`.
	pub fn upsert(&self, e: &EntryRow) -> Result<u64> {
		let ino: i64 = self
			.conn()
			.query_row(
				"INSERT INTO entries (entry_id, parent_id, drive, remote_name, local_name, kind,
					content_id, size, mtime, created_at, updated_at, status, hydrated)
				VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
				ON CONFLICT(entry_id) DO UPDATE SET parent_id = excluded.parent_id,
					drive = excluded.drive, remote_name = excluded.remote_name,
					local_name = excluded.local_name, kind = excluded.kind,
					content_id = excluded.content_id, size = excluded.size,
					mtime = excluded.mtime, created_at = excluded.created_at,
					updated_at = excluded.updated_at, status = excluded.status,
					hydrated = excluded.hydrated
				RETURNING ino",
				params![
					e.entry_id,
					e.parent_id,
					drive_col(e.drive.as_deref()),
					e.remote_name,
					e.local_name,
					e.kind.encode(),
					e.content_id,
					to_i64(e.size),
					millis(e.mtime),
					e.created_at,
					e.updated_at,
					e.status,
					e.hydrated,
				],
				|r| r.get(0),
			)
			.map_err(sql)?;
		Ok(ino.try_into().unwrap_or(0))
	}

	/// Mark a known row removed; `false` if the entry was unknown (or already removed).
	pub fn mark_removed(&self, entry_id: &str) -> Result<bool> {
		let n = self
			.conn()
			.execute(
				"UPDATE entries SET status = ?2 WHERE entry_id = ?1 AND status != ?2",
				params![entry_id, REMOVED],
			)
			.map_err(sql)?;
		Ok(n > 0)
	}

	fn one(&self, filter: &str, p: impl rusqlite::Params) -> Result<Option<EntryRow>> {
		self.conn()
			.query_row(&format!("SELECT * FROM entries WHERE {filter}"), p, row)
			.optional()
			.map_err(sql)
	}

	/// Row by entry id, any status.
	pub fn get(&self, entry_id: &str) -> Result<Option<EntryRow>> {
		self.one("entry_id = ?1", [entry_id])
	}

	/// Row by inode, any status.
	pub fn get_by_ino(&self, ino: u64) -> Result<Option<EntryRow>> {
		self.one("ino = ?1", [to_i64(ino)])
	}

	/// Live row named `local_name` in `parent_id` of `drive`.
	pub fn lookup(
		&self,
		parent_id: &str,
		drive: Option<&str>,
		local_name: &str,
	) -> Result<Option<EntryRow>> {
		self.one(
			"parent_id = ?1 AND drive = ?2 AND local_name = ?3 AND status = 'live'",
			params![parent_id, drive_col(drive), local_name],
		)
	}

	/// Live children of `parent_id` in `drive`, by local name.
	pub fn children(&self, parent_id: &str, drive: Option<&str>) -> Result<Vec<EntryRow>> {
		let conn = self.conn();
		let mut st = conn
			.prepare(
				"SELECT * FROM entries WHERE parent_id = ?1 AND drive = ?2 AND status = 'live'
				ORDER BY local_name",
			)
			.map_err(sql)?;
		st.query_map(params![parent_id, drive_col(drive)], row)
			.map_err(sql)?
			.collect::<rusqlite::Result<_>>()
			.map_err(sql)
	}

	/// Feed cursor of `drive`: `(since, after)`.
	pub fn cursor(&self, drive: Option<&str>) -> Result<Option<(i64, Option<String>)>> {
		self.conn()
			.query_row(
				"SELECT since, after FROM feed_cursor WHERE drive = ?1",
				[drive_col(drive)],
				|r| Ok((r.get(0)?, r.get(1)?)),
			)
			.optional()
			.map_err(sql)
	}

	pub fn set_cursor(&self, drive: Option<&str>, since: i64, after: Option<&str>) -> Result<()> {
		self.conn()
			.execute(
				"INSERT INTO feed_cursor (drive, since, after) VALUES (?1, ?2, ?3)
				ON CONFLICT(drive) DO UPDATE SET since = excluded.since, after = excluded.after",
				params![drive_col(drive), since, after],
			)
			.map_err(sql)?;
		Ok(())
	}

	/// Forget a drive's cursor (room left or deleted).
	pub fn drop_cursor(&self, drive: Option<&str>) -> Result<()> {
		self.conn()
			.execute("DELETE FROM feed_cursor WHERE drive = ?1", [drive_col(drive)])
			.map_err(sql)?;
		Ok(())
	}

	/// Room drives (`@tenant~name`) that have a cursor, i.e. finished their initial sync.
	pub fn room_drives(&self) -> Result<Vec<String>> {
		let conn = self.conn();
		let mut st = conn
			.prepare("SELECT drive FROM feed_cursor WHERE drive != '' ORDER BY drive")
			.map_err(sql)?;
		st.query_map([], |r| r.get(0))
			.map_err(sql)?
			.collect::<rusqlite::Result<_>>()
			.map_err(sql)
	}

	/// `(entry_id, drive)` of every live row (full-rescan diff).
	pub fn live_ids(&self) -> Result<Vec<(String, Option<String>)>> {
		let conn = self.conn();
		let mut st = conn
			.prepare("SELECT entry_id, drive FROM entries WHERE status = 'live'")
			.map_err(sql)?;
		st.query_map([], |r| {
			let drive: String = r.get(1)?;
			Ok((r.get(0)?, (!drive.is_empty()).then_some(drive)))
		})
		.map_err(sql)?
		.collect::<rusqlite::Result<_>>()
		.map_err(sql)
	}

	/// The temporary `local:<seq>` id that acknowledged entry `real_id` was created under.
	pub fn local_alias(&self, real_id: &str) -> Result<Option<String>> {
		self.conn()
			.query_row(
				"SELECT local_id FROM id_alias WHERE real_id = ?1 AND local_id LIKE ?2 LIMIT 1",
				params![real_id, format!("{}%", crate::journal::LOCAL_PREFIX)],
				|r| r.get(0),
			)
			.optional()
			.map_err(sql)
	}

	/// Drive of the live rows under `parent_id` (all children of one node share a drive);
	/// `None` for the main drive or an empty node.
	pub fn node_drive(&self, parent_id: &str) -> Result<Option<String>> {
		let drive: Option<String> = self
			.conn()
			.query_row(
				"SELECT drive FROM entries WHERE parent_id = ?1 AND status = 'live' LIMIT 1",
				[parent_id],
				|r| r.get(0),
			)
			.optional()
			.map_err(sql)?;
		Ok(drive.filter(|d| !d.is_empty()))
	}

	/// Record a cached blob as just used (insert or refresh) and flag its rows hydrated.
	pub fn lru_touch(&self, content_id: &str, size: u64) -> Result<()> {
		let conn = self.conn();
		conn.execute(
			"INSERT INTO cache_lru (content_id, size, last_access) VALUES (?1, ?2, ?3)
			ON CONFLICT(content_id) DO UPDATE SET size = excluded.size,
				last_access = excluded.last_access",
			params![content_id, to_i64(size), millis(SystemTime::now())],
		)
		.map_err(sql)?;
		conn.execute("UPDATE entries SET hydrated = 1 WHERE content_id = ?1", [content_id])
			.map_err(sql)?;
		Ok(())
	}

	/// Forget an evicted blob and clear `hydrated` on its rows.
	pub fn lru_remove(&self, content_id: &str) -> Result<()> {
		let conn = self.conn();
		conn.execute("DELETE FROM cache_lru WHERE content_id = ?1", [content_id])
			.map_err(sql)?;
		conn.execute("UPDATE entries SET hydrated = 0 WHERE content_id = ?1", [content_id])
			.map_err(sql)?;
		Ok(())
	}

	/// Total bytes of cached blobs.
	pub fn lru_total(&self) -> Result<u64> {
		let n: i64 = self
			.conn()
			.query_row("SELECT COALESCE(SUM(size), 0) FROM cache_lru", [], |r| r.get(0))
			.map_err(sql)?;
		Ok(n.try_into().unwrap_or(0))
	}

	/// Set (`on`, the entry only) or clear (the entry and its whole subtree) the pin flag.
	/// Kept out of [`EntryRow`] so [`Self::upsert`] never resets it.
	pub fn set_pinned(&self, entry_id: &str, on: bool) -> Result<()> {
		let n = if on {
			self.conn().execute(
				"UPDATE entries SET pinned = 1 WHERE entry_id = ?1 AND status = 'live'",
				[entry_id],
			)
		} else {
			self.conn().execute(
				"WITH RECURSIVE s(id) AS (SELECT ?1
					UNION SELECT e.entry_id FROM entries e JOIN s ON e.parent_id = s.id)
				UPDATE entries SET pinned = 0 WHERE entry_id IN s",
				[entry_id],
			)
		}
		.map_err(sql)?;
		if on && n == 0 { Err(ClientError::NotFound) } else { Ok(()) }
	}

	/// Whether a live ancestor of `entry_id` carries the pin flag.
	pub fn pinned_above(&self, entry_id: &str) -> Result<bool> {
		self.conn()
			.query_row(
				"WITH RECURSIVE a(id) AS (SELECT parent_id FROM entries WHERE entry_id = ?1
					UNION SELECT e.parent_id FROM entries e JOIN a ON e.entry_id = a.id)
				SELECT EXISTS (SELECT 1 FROM entries
					WHERE entry_id IN a AND pinned = 1 AND status = 'live')",
				[entry_id],
				|r| r.get(0),
			)
			.map_err(sql)
	}

	/// `(entry_id, content_id)` of live files with server content that are pinned (themselves
	/// or through an ancestor); every such file when `whole` (a `pinned` mount).
	pub fn pinned_files(&self, whole: bool) -> Result<Vec<(String, String)>> {
		let conn = self.conn();
		let mut st = conn
			.prepare(&format!(
				"{PINNED_TREE} SELECT entry_id, content_id FROM entries
				WHERE status = 'live' AND kind = 'file' AND content_id IS NOT NULL
					AND content_id NOT LIKE 'pending-%' AND (?1 OR entry_id IN p)
				ORDER BY ino"
			))
			.map_err(sql)?;
		st.query_map([whole], |r| Ok((r.get(0)?, r.get(1)?)))
			.map_err(sql)?
			.collect::<rusqlite::Result<_>>()
			.map_err(sql)
	}

	/// Cached blobs `(content_id, size)`, least recently used first.
	pub fn lru_oldest(&self) -> Result<Vec<(String, u64)>> {
		let conn = self.conn();
		let mut st = conn
			.prepare("SELECT content_id, size FROM cache_lru ORDER BY last_access, content_id")
			.map_err(sql)?;
		st.query_map([], |r| Ok((r.get(0)?, r.get::<_, i64>(1)?.try_into().unwrap_or(0))))
			.map_err(sql)?
			.collect::<rusqlite::Result<_>>()
			.map_err(sql)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn entry(id: &str, name: &str) -> EntryRow {
		EntryRow {
			ino: 0,
			entry_id: id.into(),
			parent_id: crate::tree::ROOT.into(),
			drive: None,
			remote_name: name.into(),
			local_name: name.into(),
			kind: EntryKind::Launcher { app: "quillo".into() },
			content_id: Some("c1".into()),
			size: 3,
			mtime: UNIX_EPOCH + Duration::from_millis(1_500),
			created_at: "2026-01-01T00:00:00Z".into(),
			updated_at: 7,
			status: LIVE.into(),
			hydrated: false,
		}
	}

	#[test]
	fn entries_cursor_lru() -> Result<()> {
		let db = Db::open_in_memory()?;
		let a = entry("a", "x");
		let ino = db.upsert(&a)?;
		// update keeps ino (a move is a parent change of the same row)
		let moved = EntryRow { parent_id: "f".into(), ..a.clone() };
		assert_eq!(db.upsert(&moved)?, ino);
		assert_eq!(db.get_by_ino(ino)?.map(|r| r.parent_id), Some("f".into()));
		db.upsert(&a)?;
		let got = db.lookup(crate::tree::ROOT, None, "x")?;
		assert_eq!(got, Some(EntryRow { ino, ..a.clone() }));
		// same name in the same folder and drive is rejected while live
		assert!(db.upsert(&entry("b", "x")).is_err());
		// ...but not in another drive
		db.upsert(&EntryRow { drive: Some("@t~r".into()), ..entry("b", "x") })?;
		assert_eq!(db.children(crate::tree::ROOT, None)?.len(), 1);
		assert!(db.mark_removed("a")?);
		assert!(!db.mark_removed("a")?);
		assert!(!db.mark_removed("zz")?);
		assert!(db.children(crate::tree::ROOT, None)?.is_empty());

		assert_eq!(db.cursor(None)?, None);
		db.set_cursor(None, 10, Some("e"))?;
		db.set_cursor(None, 20, None)?;
		assert_eq!(db.cursor(None)?, Some((20, None)));

		db.lru_touch("c1", 3)?;
		db.lru_touch("c2", 5)?;
		assert_eq!(db.lru_total()?, 8);
		assert!(db.get("b")?.is_some_and(|r| r.hydrated));
		db.lru_remove("c1")?;
		assert_eq!(db.lru_oldest()?, vec![("c2".into(), 5)]);
		assert!(db.get("b")?.is_some_and(|r| !r.hydrated));
		Ok(())
	}

	#[test]
	fn pinned_subtree() -> Result<()> {
		let db = Db::open_in_memory()?;
		let folder = EntryRow { kind: EntryKind::Folder, content_id: None, ..entry("f", "f") };
		db.upsert(&folder)?;
		let file = |id: &str, parent: &str, cid: &str| EntryRow {
			parent_id: parent.into(),
			kind: EntryKind::File,
			content_id: Some(cid.into()),
			..entry(id, id)
		};
		db.upsert(&file("a", "f", "ca"))?;
		db.upsert(&file("b", crate::tree::ROOT, "cb"))?;
		db.upsert(&file("p", "f", "pending-1"))?;
		assert!(db.pinned_files(false)?.is_empty());
		assert!(db.set_pinned("zz", true).is_err());
		db.set_pinned("f", true)?;
		// the upsert of a feed row keeps the flag
		db.upsert(&folder)?;
		assert_eq!(db.pinned_files(false)?, vec![("a".into(), "ca".into())]);
		assert_eq!(db.pinned_files(true)?.len(), 2);
		assert!(db.pinned_above("a")? && !db.pinned_above("b")? && !db.pinned_above("f")?);
		db.set_pinned("a", true)?;
		db.set_pinned("f", false)?;
		assert!(db.pinned_files(false)?.is_empty());
		Ok(())
	}
}
