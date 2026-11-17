// SPDX-FileCopyrightText: Szilárd Hajba
// SPDX-License-Identifier: MPL-2.0

//! Tree sources keyed by node id, both producing [`Child`]: [`RemoteTree`] (in-memory, `off`
//! mode, 10 s TTL, no persistence) and [`DbTree`] (`meta.sqlite`, kept current by
//! [`crate::feed::FeedPoller`]).
//!
//! Node ids are entry ids, plus [`ROOT`] for the mount root and `#room:@tenant~name` for the
//! virtual `~name/` room folders (target `*` only).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::api::{ApiClient, FileView, Parent};
use crate::db::{Db, EntryKind, EntryRow, LIVE};
use crate::error::Result;
use crate::session::Session;
use crate::{launcher, lock, names};

/// Node id of the mount root.
pub const ROOT: &str = "#root";
const ROOM_PREFIX: &str = "#room:";
/// Server `parent_id` of a drive's top level.
const DRIVE_TOP: &str = "__root__";
const TTL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChildKind {
	File,
	Folder,
	/// CRDT/RTDB entry shown as a read-only `.cloudillo-doc` file holding `json`.
	Launcher {
		json: Vec<u8>,
	},
	/// Virtual `~name/` folder for a room; not an entry (rename/rmdir -> `EPERM`).
	RoomFolder,
}

#[derive(Debug, Clone)]
pub struct Child {
	/// Entry id, or the synthetic `#room:…` id for a [`ChildKind::RoomFolder`].
	pub entry_id: String,
	pub local_name: String,
	pub kind: ChildKind,
	/// Content bytes (0 for folders / unknown; JSON length for launchers).
	pub size: u64,
	pub mtime: SystemTime,
	pub content_id: Option<String>,
	/// Drive the child lives in: `None` = main drive, `Some("@tenant~name")` = room.
	pub drive: Option<String>,
}

/// Channel of a virtual room folder node id.
pub fn room_channel(node: &str) -> Option<&str> {
	node.strip_prefix(ROOM_PREFIX)
}

/// Node id of the virtual folder of room `channel` (`@tenant~name`).
pub fn room_node(channel: &str) -> String {
	format!("{ROOM_PREFIX}{channel}")
}

/// Short room name (`name` of `@tenant~name`).
pub fn room_name(channel: &str) -> &str {
	channel.split_once('~').map_or(channel, |(_, name)| name)
}

/// Mount layout from the paired target: `*` = main drive + rooms, `__root__` = main drive,
/// `~name` = that room, anything else = that folder entry.
#[derive(Debug, Clone)]
pub struct Layout {
	/// What [`ROOT`] lists.
	pub root: Parent,
	/// Drive of [`ROOT`]'s children (`None` also for a folder target, whose drive is unknown).
	pub root_drive: Option<String>,
	/// Tenant whose rooms appear at the root; `Some` only for target `*`.
	pub rooms_of: Option<String>,
}

impl Layout {
	pub fn new(session: &Session) -> Self {
		let key = &session.credentials().key;
		let tenant = key.context_id_tag.as_deref().unwrap_or(&key.id_tag).to_owned();
		let main = Parent::DriveRoot { channel: None };
		let (root, root_drive, rooms_of) = match key.target.as_str() {
			"*" => (main, None, Some(tenant)),
			"__root__" => (main, None, None),
			t => match t.strip_prefix('~') {
				Some(name) => {
					let ch = format!("@{tenant}~{name}");
					(Parent::DriveRoot { channel: Some(ch.clone()) }, Some(ch), None)
				}
				None => (Parent::Folder(t.to_owned()), None, None),
			},
		};
		Self { root, root_drive, rooms_of }
	}

	/// Drive of `node`'s children: fixed for the drive root and room nodes, else `lookup`.
	pub fn drive_of(
		&self,
		node: &str,
		lookup: impl FnOnce() -> Result<Option<String>>,
	) -> Result<Option<String>> {
		if node == ROOT && matches!(self.root, Parent::DriveRoot { .. }) {
			Ok(self.root_drive.clone())
		} else if let Some(ch) = room_channel(node) {
			Ok(Some(ch.to_owned()))
		} else {
			lookup()
		}
	}

	/// Where listings, uploads and mkdirs into `node` go.
	pub fn parent_of(&self, node: &str) -> Parent {
		if node == ROOT {
			self.root.clone()
		} else if let Some(ch) = room_channel(node) {
			Parent::DriveRoot { channel: Some(ch.to_owned()) }
		} else {
			Parent::Folder(node.to_owned())
		}
	}

	/// Node id a remote entry hangs under: [`ROOT`] for the mount's top level, the room node
	/// for a room's top level (target `*`), else its parent entry id.
	pub fn node_of(&self, fv: &FileView) -> String {
		match fv.parent_id.as_deref() {
			Some(p) if matches!(&self.root, Parent::Folder(r) if r == p) => ROOT.to_owned(),
			None | Some(DRIVE_TOP) => match (&self.rooms_of, &fv.channel) {
				(Some(_), Some(ch)) => room_node(ch),
				_ => ROOT.to_owned(),
			},
			Some(p) => p.to_owned(),
		}
	}
}

/// Origin launchers point at, without network (a brokered token is not needed for it).
fn host_of(session: &Session) -> Result<String> {
	let key = &session.credentials().key;
	let api = match &key.context_id_tag {
		Some(ctx) => ApiClient::for_id_tag(ctx, None)?,
		None => session.home().clone(),
	};
	Ok(api.host().origin().ascii_serialization())
}

#[derive(Debug)]
pub struct RemoteTree {
	session: Arc<Session>,
	layout: Layout,
	listings: Mutex<HashMap<String, (Instant, Vec<Child>)>>,
	// never pruned; entries that vanish remotely linger until unmount.
	known: Mutex<HashMap<String, Child>>,
}

impl RemoteTree {
	/// Layout from the paired target, see [`Layout`].
	pub fn new(session: Arc<Session>) -> Self {
		let layout = Layout::new(&session);
		Self {
			session,
			layout,
			listings: Mutex::new(HashMap::new()),
			known: Mutex::new(HashMap::new()),
		}
	}

	/// Where listings, uploads and mkdirs into `node` go.
	pub fn parent_of(&self, node: &str) -> Parent {
		self.layout.parent_of(node)
	}

	/// Last seen child record of a node id (from any listing so far).
	pub fn get(&self, id: &str) -> Option<Child> {
		lock(&self.known).get(id).cloned()
	}

	/// Drop `node`'s cached listing (after a mutation in it).
	pub fn invalidate(&self, node: &str) {
		lock(&self.listings).remove(node);
	}

	/// Children of `node` with unique local names; served from cache for 10 s.
	pub async fn children(&self, node: &str) -> Result<Vec<Child>> {
		if let Some((at, list)) = lock(&self.listings).get(node)
			&& at.elapsed() < TTL
		{
			return Ok(list.clone());
		}
		let parent = self.parent_of(node);
		// a folder target's ROOT is never `known`: `None`, as its `root_drive`
		let drive = self.layout.drive_of(node, || Ok(self.get(node).and_then(|c| c.drive)))?;
		let host = self.session.client().await?.host().origin().ascii_serialization();
		let entries = fetch_all(&self.session, &parent).await?;

		let mut pending: Vec<(Option<SystemTime>, String, Child)> = Vec::new();
		if let (Some(tenant), true) = (&self.layout.rooms_of, node == ROOT) {
			let rooms = self.session.call(|api| async move { api.list_channels().await }).await?;
			let now = SystemTime::now();
			for room in rooms {
				let ch = format!("@{tenant}~{}", room.name);
				let base = format!("~{}", names::encode_name(&room.name));
				pending.push((None, base, room_child(ch, now)));
			}
		}
		pending.extend(entries.into_iter().map(|fv| entry_child(fv, &host, drive.as_ref())));

		let sibs: Vec<(&str, &str, Option<SystemTime>)> = pending
			.iter()
			.map(|(t, base, c)| (c.entry_id.as_str(), base.as_str(), *t))
			.collect();
		let local = names::assign_names(&sibs);
		let children: Vec<Child> = pending
			.into_iter()
			.zip(local)
			.map(|((_, _, mut c), (_, name))| {
				c.local_name = name;
				c
			})
			.collect();

		{
			let mut known = lock(&self.known);
			for c in &children {
				known.insert(c.entry_id.clone(), c.clone());
			}
		}
		lock(&self.listings).insert(node.to_owned(), (Instant::now(), children.clone()));
		Ok(children)
	}
}

/// DB-backed tree for `on-demand` mode: same [`Child`] output as [`RemoteTree`], no network.
/// Names are the sticky `local_name`s the feed assigned.
#[derive(Debug)]
pub struct DbTree {
	db: Arc<Db>,
	layout: Layout,
	host: String,
}

impl DbTree {
	pub fn new(session: &Session, db: Arc<Db>) -> Result<Self> {
		Ok(Self { db, layout: Layout::new(session), host: host_of(session)? })
	}

	/// Where listings, uploads and mkdirs into `node` go.
	pub fn parent_of(&self, node: &str) -> Parent {
		self.layout.parent_of(node)
	}

	/// Live child record of a node id; room nodes only while the room is synced.
	pub fn get(&self, id: &str) -> Result<Option<Child>> {
		if let Some(ch) = room_channel(id) {
			let known =
				self.layout.rooms_of.is_some() && self.db.room_drives()?.iter().any(|d| d == ch);
			return Ok(known.then(|| room_child(ch.to_owned(), SystemTime::now())));
		}
		Ok(self.db.get(id)?.filter(|r| r.status == LIVE).map(|r| row_child(&r, &self.host)))
	}

	/// Children of `node`, by local name; at the root of target `*`, room folders first.
	// a main-drive entry literally named `~room` shadows that room's folder
	// (RemoteTree suffixes it instead); reserve room names in `assign_sticky` if it bites.
	pub fn children(&self, node: &str) -> Result<Vec<Child>> {
		let mut out = Vec::new();
		let drive = self.layout.drive_of(node, || {
			if node == ROOT {
				self.db.node_drive(ROOT)
			} else {
				Ok(self.db.get(node)?.and_then(|r| r.drive))
			}
		})?;
		if node == ROOT && self.layout.rooms_of.is_some() {
			let now = SystemTime::now();
			for ch in self.db.room_drives()? {
				let name = format!("~{}", names::encode_name(room_name(&ch)));
				out.push(Child { local_name: name, ..room_child(ch, now) });
			}
		}
		for r in self.db.children(node, drive.as_deref())? {
			out.push(row_child(&r, &self.host));
		}
		Ok(out)
	}
}

/// Virtual folder of room `channel`; `local_name` is left to the caller.
fn room_child(channel: String, mtime: SystemTime) -> Child {
	Child {
		entry_id: room_node(&channel),
		local_name: String::new(),
		kind: ChildKind::RoomFolder,
		size: 0,
		mtime,
		content_id: None,
		drive: Some(channel),
	}
}

/// Every page of one listing.
pub(crate) async fn fetch_all(session: &Session, parent: &Parent) -> Result<Vec<FileView>> {
	let mut out = Vec::new();
	let mut cursor: Option<String> = None;
	loop {
		let page = session
			.call(|api| {
				let c = cursor.clone();
				async move { api.list_children(parent, c.as_deref()).await }
			})
			.await?;
		out.extend(page.data);
		match page.next {
			Some(next) => cursor = Some(next),
			None => return Ok(out),
		}
	}
}

/// `(created_at, base local name, row)` of a remote entry under node `parent_id`; the row's
/// `local_name` is left empty for [`names`] to finalize. Drive = the entry's `channel`.
pub(crate) fn entry_row(
	fv: FileView,
	parent_id: String,
	updated_at: i64,
) -> (Option<SystemTime>, String, EntryRow) {
	let created = parse_time(&fv.created_at);
	let mtime = fv.modified_at.as_deref().and_then(parse_time).or(created).unwrap_or(UNIX_EPOCH);
	let encoded = names::encode_name(&fv.file_name);
	let (kind, base, size) = match fv.file_type.as_deref() {
		Some("FLDR") => (EntryKind::Folder, encoded, 0),
		Some(t @ ("CRDT" | "RTDB")) => {
			let app = fv.content_type.clone().unwrap_or_else(|| t.to_owned());
			(EntryKind::Launcher { app }, launcher::name(&encoded), 0)
		}
		_ => (EntryKind::File, encoded, fv.size.unwrap_or(0)),
	};
	let row = EntryRow {
		ino: 0,
		entry_id: fv.id,
		parent_id,
		drive: fv.channel,
		remote_name: fv.file_name,
		local_name: String::new(),
		kind,
		content_id: fv.content_id,
		size,
		mtime,
		created_at: fv.created_at,
		updated_at,
		status: LIVE.to_owned(),
		hydrated: false,
	};
	(created, base, row)
}

/// The [`Child`] of a stored row; launcher JSON is rebuilt from `host`.
pub(crate) fn row_child(r: &EntryRow, host: &str) -> Child {
	let (kind, size) = match &r.kind {
		EntryKind::File => (ChildKind::File, r.size),
		EntryKind::Folder => (ChildKind::Folder, 0),
		EntryKind::Launcher { app } => {
			let json = launcher::json(host, &r.entry_id, app);
			let size = u64::try_from(json.len()).unwrap_or(u64::MAX);
			(ChildKind::Launcher { json }, size)
		}
	};
	Child {
		entry_id: r.entry_id.clone(),
		local_name: r.local_name.clone(),
		kind,
		size,
		mtime: r.mtime,
		content_id: r.content_id.clone(),
		drive: r.drive.clone(),
	}
}

/// `(created_at, base local name, child)` of a real entry; the name is finalized by
/// [`names::assign_names`].
fn entry_child(
	fv: FileView,
	host: &str,
	parent_drive: Option<&String>,
) -> (Option<SystemTime>, String, Child) {
	let (created, base, mut row) = entry_row(fv, String::new(), 0);
	row.drive = row.drive.or_else(|| parent_drive.cloned());
	(created, base, row_child(&row, host))
}

/// RFC3339 (`YYYY-MM-DDTHH:MM:SS[.frac](Z|±HH:MM)`) -> `SystemTime`, std only.
pub fn parse_time(s: &str) -> Option<SystemTime> {
	let num = |from: usize, to: usize| -> Option<i64> {
		s.get(from..to).filter(|d| d.bytes().all(|b| b.is_ascii_digit()))?.parse().ok()
	};
	let (y, mo, d) = (num(0, 4)?, num(5, 7)?, num(8, 10)?);
	let (h, mi, sec) = (num(11, 13)?, num(14, 16)?, num(17, 19)?);
	if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 || sec > 60 {
		return None;
	}
	let mut rest = s.get(19..)?;
	let mut nanos = 0u32;
	if let Some(frac) = rest.strip_prefix('.') {
		let len = frac.bytes().take_while(u8::is_ascii_digit).count();
		let digits = frac.get(..len.min(9))?;
		nanos = format!("{digits:0<9}").parse().ok()?;
		rest = frac.get(len..)?;
	}
	let offset = match rest {
		"Z" | "z" => 0,
		_ => {
			let sign = match rest.get(..1)? {
				"+" => 1,
				"-" => -1,
				_ => return None,
			};
			let oh: i64 = rest.get(1..3)?.parse().ok()?;
			let om: i64 = rest.get(4..6)?.parse().ok()?;
			sign * (oh * 3600 + om * 60)
		}
	};
	// Days since 1970-01-01 (Howard Hinnant's days_from_civil).
	let yy = if mo <= 2 { y - 1 } else { y };
	let era = yy.div_euclid(400);
	let yoe = yy - era * 400;
	let doy = (153 * ((mo + 9) % 12) + 2) / 5 + d - 1;
	let days = era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468;
	let secs = days * 86_400 + h * 3600 + mi * 60 + sec - offset;
	let base = if secs >= 0 {
		UNIX_EPOCH.checked_add(Duration::from_secs(secs.unsigned_abs()))?
	} else {
		UNIX_EPOCH.checked_sub(Duration::from_secs(secs.unsigned_abs()))?
	};
	base.checked_add(Duration::from_nanos(u64::from(nanos)))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parses_rfc3339() {
		assert_eq!(parse_time("1970-01-01T00:00:00Z"), Some(UNIX_EPOCH));
		assert_eq!(
			parse_time("2024-02-29T12:00:00.5+01:00"),
			Some(UNIX_EPOCH + Duration::new(1_709_204_400, 500_000_000))
		);
		assert_eq!(parse_time("2024-02-29 12:00:00Z"), parse_time("2024-02-29T12:00:00Z"));
		assert_eq!(parse_time("not a time"), None);
		assert_eq!(parse_time("2024-13-01T00:00:00Z"), None);
	}

	#[test]
	fn rows_round_trip_to_children() {
		let fv = FileView {
			id: "d1".into(),
			file_name: "notes".into(),
			file_type: Some("CRDT".into()),
			content_type: Some("quillo".into()),
			created_at: "2026-01-01T00:00:00Z".into(),
			channel: Some("@t~team".into()),
			..FileView::default()
		};
		let (created, base, row) = entry_row(fv, room_node("@t~team"), 5);
		assert_eq!(created, parse_time("2026-01-01T00:00:00Z"));
		assert_eq!(base, "notes.cloudillo-doc");
		assert_eq!(row.kind, EntryKind::Launcher { app: "quillo".into() });
		let c = row_child(&row, "https://cl-o.t");
		let json = launcher::json("https://cl-o.t", "d1", "quillo");
		assert_eq!(c.size, u64::try_from(json.len()).unwrap_or(0));
		assert_eq!(c.kind, ChildKind::Launcher { json });
		assert_eq!(c.drive.as_deref(), Some("@t~team"));
		assert_eq!(room_name("@t~team"), "team");
	}

	#[test]
	fn room_ids() {
		assert_eq!(room_channel("#room:@t~team"), Some("@t~team"));
		assert_eq!(room_channel("e1"), None);
	}
}
