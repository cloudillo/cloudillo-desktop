// SPDX-FileCopyrightText: Szilárd Hajba
// SPDX-License-Identifier: MPL-2.0

//! Change-feed poller: keeps one mount's `meta.sqlite` equal to the server, one cursor per
//! drive (`GET /api/file-changes` takes one drive or folder, never `*`).
//!
//! Per drive: no cursor -> full listing; else the feed from `cursor - 10 s`, paged through
//! `next`, deduped on `(entry_id, updatedAt, contentId)` against the stored row; 410 -> full
//! listing again. A full listing has no server cursor, so it saves the local clock minus
//! [`RESCAN_MARGIN`] and stores its rows with `updated_at = 0` (any feed row then wins).
//!
//! A remote change to an entry with pending journal ops goes through [`conflict::decide`]
//! instead of overwriting the local row.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::{OptionalExtension, params};
use tokio::sync::Notify;

use crate::api::{FileChange, FileView, Parent};
use crate::atomic;
use crate::conflict::{self, Action, LocalChange, RemoteChange};
use crate::db::{Db, EntryRow, LIVE, drive_col, sql};
use crate::error::{ClientError, Result};
use crate::journal::LOCAL_PREFIX;
use crate::session::Session;
use crate::tree::{self, Layout, ROOT};
use crate::{lock, names};

const POLL: Duration = Duration::from_secs(60);
const ROOMS_TTL: Duration = Duration::from_secs(15 * 60);
/// Seconds re-read before the saved cursor.
const OVERLAP: i64 = 10;
/// Cursor of a full listing = local now minus this: covers client/server clock skew up to
/// 5 min and changes racing the listing (re-applying them is idempotent).
const RESCAN_MARGIN: i64 = 300;

/// `(created_at, base local name, row)` from [`tree::entry_row`].
type Pending = (Option<SystemTime>, String, EntryRow);

/// One change feed: a drive, or the mount's folder.
struct Feed {
	/// `root=` of `/api/file-changes`: folder entry id, `__root__` or `~name`.
	root: String,
	/// Cursor key (`None` = main drive or a folder target).
	drive: Option<String>,
	/// Node id the feed's top level hangs under, and where its full listing starts.
	node: String,
	parent: Parent,
}

/// Feed poller of one mount; share as `Arc<FeedPoller>`, drive it with [`Self::run`].
#[derive(Debug)]
pub struct FeedPoller {
	session: Arc<Session>,
	db: Arc<Db>,
	layout: Layout,
	wake: Notify,
	/// Room channels and when they were listed; target `*` only.
	rooms: Mutex<Option<(Instant, Vec<String>)>>,
	/// Unlinks / renames still in their [`atomic::HOLD`] window, not journaled yet.
	held: Mutex<HashMap<String, (LocalChange, atomic::Hold)>>,
}

impl FeedPoller {
	pub fn new(session: Arc<Session>, db: Arc<Db>) -> Self {
		let layout = Layout::new(&session);
		Self {
			session,
			db,
			layout,
			wake: Notify::new(),
			rooms: Mutex::new(None),
			held: Mutex::new(HashMap::new()),
		}
	}

	/// Local change `c` to entry `id` is held by `h`: it counts as pending until claimed.
	pub fn hold(&self, id: &str, c: LocalChange, h: atomic::Hold) {
		lock(&self.held).insert(id.to_owned(), (c, h));
	}

	/// Poll now instead of at the next tick; call after every local mutation. Triggers
	/// arriving while a round runs coalesce into one more round.
	pub fn trigger(&self) {
		self.wake.notify_one();
	}

	/// [`Self::sync`] every 60 s and on [`Self::trigger`], forever. A failed round (offline,
	/// server error) is retried at the next one.
	// errors are dropped; keep the last one for `cloudillo status` when it needs it.
	pub async fn run(&self) {
		loop {
			let _ = self.sync().await;
			let _ = tokio::time::timeout(POLL, self.wake.notified()).await;
		}
	}

	/// One round over every drive of the mount (initial sync = full listing of each).
	pub async fn sync(&self) -> Result<()> {
		// one failing feed does not stop the others; the first error is reported
		let mut first = None;
		for feed in self.feeds().await? {
			let res = match self.db.cursor(feed.drive.as_deref()) {
				Ok(None) => self.rescan(&feed).await,
				Ok(Some((since, _))) => self.poll(&feed, since).await,
				Err(e) => Err(e),
			};
			let res = match res {
				Err(ClientError::Http { status: 410, .. }) => self.rescan(&feed).await,
				other => other,
			};
			if let Err(e) = res {
				first.get_or_insert(e);
			}
		}
		first.map_or(Ok(()), Err)
	}

	fn feeds_of(&self, rooms: Vec<String>) -> Vec<Feed> {
		let l = &self.layout;
		let root = match &l.root {
			Parent::Folder(id) => id.clone(),
			Parent::DriveRoot { channel: Some(ch) } => format!("~{}", tree::room_name(ch)),
			Parent::DriveRoot { channel: None } => "__root__".to_owned(),
		};
		let top = Feed {
			root,
			drive: l.root_drive.clone(),
			node: ROOT.to_owned(),
			parent: l.root.clone(),
		};
		let rooms = rooms.into_iter().map(|ch| Feed {
			root: format!("~{}", tree::room_name(&ch)),
			node: tree::room_node(&ch),
			parent: Parent::DriveRoot { channel: Some(ch.clone()) },
			drive: Some(ch),
		});
		std::iter::once(top).chain(rooms).collect()
	}

	async fn feeds(&self) -> Result<Vec<Feed>> {
		let rooms = if self.layout.rooms_of.is_some() { self.rooms().await? } else { Vec::new() };
		Ok(self.feeds_of(rooms))
	}

	/// Room channels, re-listed every 15 min (or after [`Self::poll`] saw an unknown one).
	/// Rooms no longer listed lose their rows and cursor.
	async fn rooms(&self) -> Result<Vec<String>> {
		if let Some((at, rooms)) = lock(&self.rooms).as_ref()
			&& at.elapsed() < ROOMS_TTL
		{
			return Ok(rooms.clone());
		}
		let Some(tenant) = &self.layout.rooms_of else { return Ok(Vec::new()) };
		let listed = self.session.call(|api| async move { api.list_channels().await }).await?;
		let rooms: Vec<String> =
			listed.into_iter().map(|c| format!("@{tenant}~{}", c.name)).collect();
		for ch in self.db.room_drives()? {
			if !rooms.contains(&ch) {
				self.remove_unseen(Some(&ch), &HashSet::new()).await?;
				self.db.drop_cursor(Some(&ch))?;
			}
		}
		*lock(&self.rooms) = Some((Instant::now(), rooms.clone()));
		Ok(rooms)
	}

	/// The feed of one drive from `stored - OVERLAP`, every page.
	async fn poll(&self, feed: &Feed, stored: i64) -> Result<()> {
		let mut since = stored - OVERLAP;
		let mut after: Option<String> = None;
		loop {
			let page = self
				.session
				.call(|api| {
					let a = after.clone();
					let root = feed.root.as_str();
					async move { api.file_changes(root, since, a.as_deref(), None).await }
				})
				.await?;
			let mut rows: Vec<Pending> = Vec::new();
			for change in page.changes {
				match change {
					FileChange::Gone(g) => {
						// keep feed order: rows before this marker land first
						self.apply(std::mem::take(&mut rows))?;
						self.gone(&g.id).await?;
					}
					FileChange::Row(r) => {
						if matches!(&self.layout.root, Parent::Folder(id) if *id == r.file.id) {
							continue; // the mount's own folder
						}
						let unknown_room = r.file.channel.as_ref().is_some_and(|ch| {
							self.layout.rooms_of.is_some()
								&& lock(&self.rooms).as_ref().is_some_and(|(_, k)| !k.contains(ch))
						});
						if unknown_room {
							*lock(&self.rooms) = None; // re-list rooms next round
							self.trigger();
						}
						let node = self.layout.node_of(&r.file);
						rows.push(tree::entry_row(r.file, node, r.updated_at));
					}
				}
			}
			self.apply(rows)?;
			// an empty page echoes `since`; never move the cursor back by the overlap
			self.db.set_cursor(
				feed.drive.as_deref(),
				page.cursor.max(stored),
				page.next.as_deref(),
			)?;
			match page.next {
				Some(next) => {
					since = page.cursor;
					after = Some(next);
				}
				None => return Ok(()),
			}
		}
	}

	/// Full listing of one drive: drop the rows it no longer has, upsert the rest.
	async fn rescan(&self, feed: &Feed) -> Result<()> {
		let cursor = now_secs() - RESCAN_MARGIN;
		let mut rows: Vec<Pending> = Vec::new();
		let mut seen = HashSet::new();
		let mut queue = vec![(feed.node.clone(), feed.parent.clone())];
		while let Some((node, parent)) = queue.pop() {
			for fv in tree::fetch_all(&self.session, &parent).await? {
				if fv.file_type.as_deref() == Some("FLDR") {
					queue.push((fv.id.clone(), Parent::Folder(fv.id.clone())));
				}
				seen.insert(fv.id.clone());
				let (created, base, mut row) = tree::entry_row(fv, node.clone(), 0);
				row.drive = row.drive.or_else(|| feed.drive.clone());
				rows.push((created, base, row));
			}
		}
		// removals first, so their names are free for the new rows
		self.remove_unseen(feed.drive.as_deref(), &seen).await?;
		self.apply(rows)?;
		self.db.set_cursor(feed.drive.as_deref(), cursor, None)
	}

	/// Mark removed every live row of the feed's drive (all rows, unless target `*`) not in
	/// `seen` (unreplayed local creates are not on the server yet).
	async fn remove_unseen(&self, drive: Option<&str>, seen: &HashSet<String>) -> Result<()> {
		let every_drive = self.layout.rooms_of.is_none();
		for (id, d) in self.db.live_ids()? {
			if (every_drive || d.as_deref() == drive)
				&& !seen.contains(&id)
				&& !id.starts_with(LOCAL_PREFIX)
			{
				self.gone(&id).await?;
			}
		}
		Ok(())
	}

	/// Entry `id` left the feed (trash or purge): pending local changes to it, or ops putting
	/// entries into it, go through the conflict table; otherwise it leaves the local view.
	async fn gone(&self, id: &str) -> Result<()> {
		let mut changes = self.pending(id)?.0;
		if self.moved_into(id)? {
			changes.push(LocalChange::MovedInto);
		}
		let (mut pull, mut keep) = (changes.is_empty(), false);
		for c in changes {
			for a in conflict::decide(c, RemoteChange::Trashed) {
				match a {
					Action::DropLocal => self.drop_local(id, c)?,
					Action::Pull => pull = true,
					// the worker restores before replaying the replace (sync.rs `replay`)
					Action::Restore => keep = true,
					Action::RestoreFolder => {
						keep = true;
						self.restore_folder(id).await?;
					}
					Action::Upload | Action::UploadAsConflictSibling => {}
				}
			}
		}
		// a restore wins over removing it from the view
		if pull && !keep {
			self.db.mark_removed(id)?;
		}
		Ok(())
	}

	/// `POST /api/files/{id}/restore` of folder `id` into its known parent.
	async fn restore_folder(&self, id: &str) -> Result<()> {
		let Some(row) = self.db.get(id)? else { return Ok(()) };
		let parent = match self.layout.parent_of(&row.parent_id) {
			Parent::Folder(p) => Some(p),
			Parent::DriveRoot { .. } => None,
		};
		let to = parent.as_deref();
		match self.session.call(|api| async move { api.restore_trashed(id, to).await }).await {
			Err(e @ ClientError::Network(_)) => Err(e),
			// purged, or moved out of the mount rather than trashed: the pending ops then fail
			// (or land) at replay and stay journaled
			_ => Ok(()),
		}
	}

	/// Adopt the server's current state of one entry (the worker, after a 412).
	pub(crate) fn pull(&self, fv: FileView) -> Result<()> {
		if fv.parent_id.as_deref() == Some(conflict::TRASH_PARENT) {
			return self.db.mark_removed(&fv.id).map(drop);
		}
		let node = self.layout.node_of(&fv);
		self.apply(vec![tree::entry_row(fv, node, 0)])
	}

	/// Local changes the journal (`pending_ops`) holds for entry `id`, the `If-Match` base of
	/// its pending replace and when its oldest op was made.
	fn pending(&self, id: &str) -> Result<(Vec<LocalChange>, Option<String>, i64)> {
		let conn = self.db.conn();
		let mut stmt = conn
			.prepare(
				"SELECT op, base_content_id, created_at FROM pending_ops WHERE entry_id = ?1
				ORDER BY seq",
			)
			.map_err(sql)?;
		let rows = stmt
			.query_map([id], |r| {
				Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?, r.get::<_, i64>(2)?))
			})
			.map_err(sql)?;
		let (mut changes, mut base, mut since) = (Vec::new(), None, i64::MAX);
		for r in rows {
			let (op, b, at) = r.map_err(sql)?;
			since = since.min(at);
			let c = match op.as_str() {
				"replace" => {
					base = base.or(b);
					LocalChange::Modified
				}
				"trash" => LocalChange::Trashed,
				"rename" | "move" => LocalChange::Renamed,
				_ => continue,
			};
			if !changes.contains(&c) {
				changes.push(c);
			}
		}
		let mut held = lock(&self.held);
		held.retain(|_, (_, h)| !h.claimed());
		if let Some((c, _)) = held.get(id) {
			if !changes.contains(c) {
				changes.push(*c);
			}
			since = since.min(now_secs());
		}
		Ok((changes, base, since))
	}

	/// A pending op puts an entry into folder `id`.
	fn moved_into(&self, id: &str) -> Result<bool> {
		self.db
			.conn()
			.query_row(
				"SELECT EXISTS(SELECT 1 FROM pending_ops WHERE parent_id = ?1
					AND op IN ('move', 'create', 'mkdir', 'replace'))",
				[id],
				|r| r.get(0),
			)
			.map_err(sql)
	}

	/// Drop the pending ops of local change `c` to entry `id`; never ops carrying content.
	fn drop_local(&self, id: &str, c: LocalChange) -> Result<()> {
		let ops: &[&str] = match c {
			LocalChange::Trashed => &["trash"],
			LocalChange::Renamed => &["rename", "move"],
			_ => return Ok(()),
		};
		let mut held = lock(&self.held);
		if held.get(id).is_some_and(|(h, _)| *h == c)
			&& let Some((_, h)) = held.remove(id)
		{
			h.claim(); // the timer then journals nothing
		}
		drop(held);
		let conn = self.db.conn();
		for op in ops {
			conn.execute(
				"DELETE FROM pending_ops WHERE entry_id = ?1 AND op = ?2 AND staged_blob IS NULL",
				params![id, op],
			)
			.map_err(sql)?;
		}
		Ok(())
	}

	/// Remote `row` of an entry with pending local ops: what of it applies. `None` keeps the
	/// local row (only its `updated_at` advances).
	// the row keeps no pre-rename name, so with a local rename pending any newer
	// remote row with the old name reads as a remote rename (remote wins). Our own echo of an
	// earlier op is the false positive; the rename op is then normally already in flight.
	fn reconcile(&self, mut row: EntryRow, old: &EntryRow) -> Result<Option<EntryRow>> {
		let (changes, base, oldest) = self.pending(&row.entry_id)?;
		if changes.is_empty() {
			return Ok(Some(row));
		}
		// a row from a full listing has no base time: changes after the oldest op count
		// compares server and local clocks
		let since = if old.updated_at > 0 { old.updated_at } else { oldest };
		let base = base.as_ref().or(old.content_id.as_ref());
		let place = |r: &EntryRow| (r.parent_id.clone(), r.drive.clone(), r.remote_name.clone());
		let remote = if row.updated_at != 0 && row.updated_at <= since {
			None
		} else if row.content_id.as_ref() != base {
			Some(RemoteChange::Modified)
		} else if place(&row) != place(old) {
			Some(RemoteChange::Renamed)
		} else {
			None
		};
		let mut pull = false;
		for c in changes {
			for a in remote.map(|r| conflict::decide(c, r)).unwrap_or_default() {
				match a {
					Action::DropLocal => self.drop_local(&row.entry_id, c)?,
					Action::Pull => pull = true,
					// replay does the rest: a replace's 412 uploads the conflict sibling
					_ => {}
				}
			}
		}
		if !pull {
			self.db
				.conn()
				.execute(
					"UPDATE entries SET updated_at = MAX(updated_at, ?2) WHERE entry_id = ?1",
					params![row.entry_id, row.updated_at],
				)
				.map_err(sql)?;
			return Ok(None);
		}
		// what a still-pending op changes keeps its local value
		let left = self.pending(&row.entry_id)?.0;
		if left.contains(&LocalChange::Modified) {
			row.content_id.clone_from(&old.content_id);
			row.size = old.size;
			row.mtime = old.mtime;
		}
		if left.contains(&LocalChange::Renamed) {
			row.parent_id.clone_from(&old.parent_id);
			row.drive.clone_from(&old.drive);
			row.remote_name.clone_from(&old.remote_name);
		}
		if left.contains(&LocalChange::Trashed) {
			row.status.clone_from(&old.status);
		}
		Ok(Some(row))
	}

	/// New remote `row` with the name of a pending local create in the same folder: both are
	/// kept, the local one gets the conflict suffix.
	// only while the create is unreplayed; one replayed first keeps its name and the
	// sticky local names tell the two apart.
	fn created_twice(&self, row: &EntryRow) -> Result<()> {
		let conn = self.db.conn();
		let local: Option<String> = conn
			.query_row(
				"SELECT entry_id FROM pending_ops WHERE op = 'create' AND parent_id = ?1
					AND drive = ?2 AND name = ?3 LIMIT 1",
				params![row.parent_id, drive_col(row.drive.as_deref()), row.remote_name],
				|r| r.get(0),
			)
			.optional()
			.map_err(sql)?;
		let Some(local) = local else { return Ok(()) };
		for a in conflict::decide(LocalChange::Created, RemoteChange::Created) {
			// Action::Pull: the remote row applies as usual
			if a == Action::UploadAsConflictSibling {
				let name = conflict::conflict_name(
					&row.remote_name,
					&conflict::device(),
					SystemTime::now(),
				);
				conn.execute(
					"UPDATE pending_ops SET name = ?2 WHERE entry_id = ?1 AND op = 'create'",
					params![local, name],
				)
				.map_err(sql)?;
				conn.execute(
					"UPDATE entries SET remote_name = ?2, local_name = ?3 WHERE entry_id = ?1",
					params![local, name, names::encode_name(&name)],
				)
				.map_err(sql)?;
			}
		}
		Ok(())
	}

	/// Upsert remote rows in order. A known row keeps its `ino`, `hydrated` (same content)
	/// and, unless renamed or moved remotely, its sticky `local_name`; the rest are named per
	/// folder by [`names::assign_sticky`]. A move is the same row with a new `parent_id`.
	// two rows swapping names in one batch end up suffixed, not swapped.
	fn apply(&self, rows: Vec<Pending>) -> Result<()> {
		let mut fresh: HashMap<(String, Option<String>), Vec<Pending>> = HashMap::new();
		for (created, base, mut row) in rows {
			if let Some(old) = self.db.get(&row.entry_id)? {
				let live = old.status == LIVE;
				let stale = old.updated_at > row.updated_at
					|| (old.updated_at == row.updated_at && old.content_id == row.content_id);
				if live && row.updated_at != 0 && stale {
					continue;
				}
				row = match self.reconcile(row, &old)? {
					Some(r) => r,
					None => continue,
				};
				row.hydrated = old.hydrated && old.content_id == row.content_id;
				if live
					&& old.parent_id == row.parent_id
					&& old.drive == row.drive
					&& old.remote_name == row.remote_name
				{
					row.local_name = old.local_name;
					self.db.upsert(&row)?;
					continue;
				}
			} else {
				self.created_twice(&row)?;
			}
			let key = (row.parent_id.clone(), row.drive.clone());
			fresh.entry(key).or_default().push((created, base, row));
		}
		for ((parent, drive), group) in fresh {
			let sibs: Vec<(&str, &str, Option<SystemTime>)> = group
				.iter()
				.map(|(t, base, r)| (r.entry_id.as_str(), base.as_str(), *t))
				.collect();
			let local = names::assign_sticky(&self.db, &parent, drive.as_deref(), &sibs)?;
			for ((_, _, mut row), (_, name)) in group.into_iter().zip(local) {
				row.local_name = name;
				self.db.upsert(&row)?;
			}
		}
		Ok(())
	}
}

fn now_secs() -> i64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::api::PairResult;
	use crate::credentials::Credentials;
	use crate::db::EntryKind;
	use crate::journal::{Journal, OpKind, PendingOp};

	/// A poller over an in-memory db; building it touches no network.
	fn poller() -> Result<(FeedPoller, Journal, std::path::PathBuf)> {
		let key = PairResult {
			api_key: "cl_test".into(),
			id_tag: "test.example".into(),
			scope: String::new(),
			context_id_tag: None,
			target: "__root__".into(),
			access: "write".into(),
			profile: None,
			expires_at: None,
		};
		let session = Arc::new(Session::new(Credentials { key })?);
		let db = Arc::new(Db::open_in_memory()?);
		// one dir per test thread
		let tmp = std::env::temp_dir().join(format!(
			"cl-feed-{}-{:?}",
			std::process::id(),
			std::thread::current().id()
		));
		let j = Journal::new(Arc::clone(&db), tmp.join("staged"))?;
		Ok((FeedPoller::new(session, db), j, tmp))
	}

	fn row(id: &str, name: &str, cid: &str, updated_at: i64) -> EntryRow {
		EntryRow {
			ino: 0,
			entry_id: id.into(),
			parent_id: ROOT.into(),
			drive: None,
			remote_name: name.into(),
			local_name: name.into(),
			kind: EntryKind::File,
			content_id: Some(cid.into()),
			size: 1,
			mtime: UNIX_EPOCH + Duration::from_secs(1),
			created_at: String::new(),
			updated_at,
			status: LIVE.into(),
			hydrated: false,
		}
	}

	#[test]
	fn reconcile_by_pending_ops() -> Result<()> {
		let (p, j, tmp) = poller()?;
		let old = row("e1", "a", "c1", 10);
		p.db.upsert(&old)?;
		// nothing pending: the remote row applies as is
		let got = p.reconcile(row("e1", "b", "c2", 20), &old)?;
		assert_eq!(got.map(|r| r.remote_name), Some("b".to_owned()));

		// local trash vs remote modified: the trash is dropped, the remote row pulled
		j.enqueue(&PendingOp::new(OpKind::Trash, "e1", "a"), None)?;
		let got = p.reconcile(row("e1", "a", "c2", 20), &old)?;
		assert_eq!(got.and_then(|r| r.content_id), Some("c2".to_owned()));
		assert_eq!(j.counts()?, (0, 0));

		// local rename vs a remote row no newer than the local base: local row kept
		j.enqueue(&PendingOp::new(OpKind::Rename, "e1", "z"), None)?;
		assert!(p.reconcile(row("e1", "a", "c1", 10), &old)?.is_none());
		assert_eq!(j.counts()?, (1, 0));
		let _ = std::fs::remove_dir_all(&tmp);
		Ok(())
	}

	#[test]
	fn held_unlink_stays_removed() -> Result<()> {
		let (p, _j, tmp) = poller()?;
		let mut gone = row("e1", "a", "c1", 10);
		gone.status = crate::db::REMOVED.into();
		p.db.upsert(&gone)?;
		p.hold("e1", LocalChange::Trashed, atomic::Hold::default());
		p.apply(vec![(None, "a".into(), row("e1", "a", "c1", 10))])?;
		let st = p.db.get("e1")?.map(|r| r.status);
		assert_eq!(st.as_deref(), Some(crate::db::REMOVED));
		let _ = std::fs::remove_dir_all(&tmp);
		Ok(())
	}

	#[test]
	fn created_twice_renames_the_local_create() -> Result<()> {
		let (p, j, tmp) = poller()?;
		let mut op = PendingOp::new(OpKind::Create, "", "a.txt");
		op.parent_id = Some(ROOT.into());
		op.name = Some("a.txt".into());
		let local = crate::journal::local_id(j.enqueue(&op, None)?);
		p.db.upsert(&row(&local, "a.txt", "pending-1", 0))?;

		p.created_twice(&row("r1", "a.txt", "c1", 5))?;
		let renamed = p.db.get(&local)?.ok_or(ClientError::NotFound)?.remote_name;
		assert!(renamed.starts_with("a (conflict ") && renamed.ends_with(").txt"), "{renamed}");
		let op = j.next_ready()?.ok_or(ClientError::NotFound)?;
		assert_eq!(op.name, Some(renamed));

		// another name in the folder: untouched
		p.created_twice(&row("r2", "b.txt", "c2", 5))?;
		assert_eq!(j.next_ready()?.and_then(|o| o.name), p.db.get(&local)?.map(|r| r.remote_name));
		let _ = std::fs::remove_dir_all(&tmp);
		Ok(())
	}

	#[test]
	fn moved_into_sees_pending_moves() -> Result<()> {
		let (p, j, tmp) = poller()?;
		let mut op = PendingOp::new(OpKind::Move, "e1", "a");
		op.parent_id = Some("D".into());
		j.enqueue(&op, None)?;
		assert!(p.moved_into("D")?);
		assert!(!p.moved_into("X")?);
		let _ = std::fs::remove_dir_all(&tmp);
		Ok(())
	}
}
