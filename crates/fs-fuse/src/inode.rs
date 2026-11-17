// SPDX-FileCopyrightText: Szilárd Hajba
// SPDX-License-Identifier: MPL-2.0

//! Bidirectional inode <-> node id map. Inode 1 is the mount root ([`tree::ROOT`]).
//! Each inode remembers its parent inode, for `..`.
//!
//! `on-demand` mode: an entry with a DB row gets `entries.ino + 1` (stable across restarts;
//! `+ 1` keeps clear of the root); nodes without a row (room folders, files not uploaded
//! yet) are numbered from [`MEM_BASE`]. `off` mode numbers everything from 2.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use cloudillo_desktop_core::{Db, tree};

pub const ROOT_INO: u64 = 1;
/// First in-memory inode in `on-demand` mode, far above any `entries.ino`.
const MEM_BASE: u64 = 1 << 62;

#[derive(Debug)]
struct Node {
	id: String,
	parent: u64,
}

#[derive(Debug)]
struct Maps {
	by_ino: HashMap<u64, Node>,
	by_id: HashMap<String, u64>,
	next: u64,
}

// never pruned (like `RemoteTree::known`); inodes live until unmount.
#[derive(Debug)]
pub struct Inodes {
	maps: Mutex<Maps>,
	/// `on-demand`: source of the stable inode numbers.
	db: Option<Arc<Db>>,
}

impl Default for Inodes {
	fn default() -> Self {
		Self::new(None)
	}
}

impl Inodes {
	pub fn new(db: Option<Arc<Db>>) -> Self {
		let next = if db.is_some() { MEM_BASE } else { ROOT_INO + 1 };
		let mut maps = Maps { by_ino: HashMap::new(), by_id: HashMap::new(), next };
		maps.by_ino
			.insert(ROOT_INO, Node { id: tree::ROOT.to_owned(), parent: ROOT_INO });
		maps.by_id.insert(tree::ROOT.to_owned(), ROOT_INO);
		Self { maps: Mutex::new(maps), db }
	}

	/// Node id of `ino`.
	pub fn id(&self, ino: u64) -> Option<String> {
		self.lock().by_ino.get(&ino).map(|n| n.id.clone())
	}

	/// Parent inode of `ino` (the root is its own parent).
	pub fn parent(&self, ino: u64) -> Option<u64> {
		self.lock().by_ino.get(&ino).map(|n| n.parent)
	}

	/// Inode of node `id` seen under `parent`; allocates on first sight, re-parents if moved.
	pub fn ino_for(&self, id: &str, parent: u64) -> u64 {
		let mut maps = self.lock();
		if let Some(&ino) = maps.by_id.get(id) {
			if let Some(node) = maps.by_ino.get_mut(&ino) {
				node.parent = parent;
			}
			return ino;
		}
		// a created entry, acknowledged since: keep the inode it was handed out under
		let local = self.db.as_ref().and_then(|db| db.local_alias(id).ok().flatten());
		if let Some(ino) = local.and_then(|l| maps.by_id.remove(&l)) {
			if let Some(node) = maps.by_ino.get_mut(&ino) {
				id.clone_into(&mut node.id);
				node.parent = parent;
			}
			maps.by_id.insert(id.to_owned(), ino);
			return ino;
		}
		let row = self.db.as_ref().and_then(|db| db.get(id).ok().flatten());
		let ino = if let Some(row) = row {
			row.ino + 1
		} else {
			maps.next += 1;
			maps.next - 1
		};
		maps.by_ino.insert(ino, Node { id: id.to_owned(), parent });
		maps.by_id.insert(id.to_owned(), ino);
		ino
	}

	/// Point `ino` at node `id` (a created file got its entry id); the old id is forgotten.
	pub fn rebind(&self, ino: u64, id: &str) {
		let mut maps = self.lock();
		let old = match maps.by_ino.get_mut(&ino) {
			Some(node) => std::mem::replace(&mut node.id, id.to_owned()),
			None => return,
		};
		maps.by_id.remove(&old);
		// the inode `id` had (an atomic save's replaced file) is forgotten: it must not alias it
		if let Some(prev) = maps.by_id.insert(id.to_owned(), ino)
			&& prev != ino
		{
			maps.by_ino.remove(&prev);
		}
	}

	fn lock(&self) -> std::sync::MutexGuard<'_, Maps> {
		cloudillo_desktop_core::lock(&self.maps)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn maps_both_ways() {
		let m = Inodes::default();
		assert_eq!(m.id(ROOT_INO).as_deref(), Some(tree::ROOT));
		let a = m.ino_for("e1", ROOT_INO);
		assert_eq!(m.ino_for("e1", ROOT_INO), a);
		let b = m.ino_for("e2", a);
		assert_ne!(a, b);
		assert_eq!(m.parent(b), Some(a));
		assert_eq!(m.ino_for("e2", ROOT_INO), b);
		assert_eq!(m.parent(b), Some(ROOT_INO));
		assert_eq!(m.id(b).as_deref(), Some("e2"));
		m.rebind(b, "e3");
		assert_eq!(m.id(b).as_deref(), Some("e3"));
		assert_eq!(m.ino_for("e3", ROOT_INO), b);
		assert_ne!(m.ino_for("e2", ROOT_INO), b);
	}

	#[test]
	fn rebind_forgets_replaced_ino() {
		let m = Inodes::default();
		let a = m.ino_for("e1", ROOT_INO);
		let b = m.ino_for("#new:1", ROOT_INO);
		m.rebind(b, "e1");
		assert_eq!(m.id(a), None);
		assert_eq!(m.ino_for("e1", ROOT_INO), b);
		assert_eq!(m.id(b).as_deref(), Some("e1"));
	}

	#[test]
	fn db_rows_keep_their_ino() -> cloudillo_desktop_core::Result<()> {
		use cloudillo_desktop_core::{EntryKind, EntryRow, db};
		let db = Arc::new(Db::open_in_memory()?);
		let row = EntryRow {
			ino: 0,
			entry_id: "e1".into(),
			parent_id: tree::ROOT.into(),
			drive: None,
			remote_name: "a".into(),
			local_name: "a".into(),
			kind: EntryKind::File,
			content_id: None,
			size: 0,
			mtime: std::time::UNIX_EPOCH,
			created_at: String::new(),
			updated_at: 0,
			status: db::LIVE.into(),
			hydrated: false,
		};
		let stored = db.upsert(&row)?;
		let m = Inodes::new(Some(Arc::clone(&db)));
		assert_eq!(m.ino_for("e1", ROOT_INO), stored + 1);
		assert_ne!(stored + 1, ROOT_INO);
		assert_eq!(m.ino_for("#room:@t~r", ROOT_INO), MEM_BASE);
		// A restart maps the row to the same inode.
		assert_eq!(Inodes::new(Some(db)).ino_for("e1", ROOT_INO), stored + 1);
		Ok(())
	}

	#[test]
	fn acked_create_keeps_its_ino() -> cloudillo_desktop_core::Result<()> {
		use cloudillo_desktop_core::{Journal, OpKind, PendingOp};
		let tmp = std::env::temp_dir().join(format!("cl-inode-{}", std::process::id()));
		let db = Arc::new(Db::open_in_memory()?);
		let j = Journal::new(Arc::clone(&db), tmp.join("staged"))?;
		let mut op = PendingOp::new(OpKind::Create, "", "f");
		op.parent_id = Some(tree::ROOT.into());
		let seq = j.enqueue(&op, None)?;
		let local = cloudillo_desktop_core::journal::local_id(seq);
		let m = Inodes::new(Some(Arc::clone(&db)));
		let ino = m.ino_for(&local, ROOT_INO);
		j.ack(seq, Some("e9"), None)?;
		assert_eq!(m.ino_for("e9", ROOT_INO), ino);
		assert_eq!(m.id(ino).as_deref(), Some("e9"));
		let _ = std::fs::remove_dir_all(&tmp);
		Ok(())
	}
}
