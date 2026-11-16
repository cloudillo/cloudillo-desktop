//! Mount context for tests: no FUSE, network through the core `stub` server.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;

use cloudillo_desktop_core::db::{self, EntryKind, EntryRow};
use cloudillo_desktop_core::stub::{Log, stub};
use cloudillo_desktop_core::{
	BlobCache, Db, DbTree, FeedPoller, Journal, RemoteTree, Uploader, tree,
};

use crate::inode::Inodes;
use crate::{Ctx, OnDemand, Owner, Tree};

pub(crate) struct T {
	pub ctx: Ctx,
	pub tmp: PathBuf,
}

impl Drop for T {
	fn drop(&mut self) {
		let _ = std::fs::remove_dir_all(&self.tmp);
	}
}

/// An `on-demand` mount context (target: the main drive).
pub(crate) async fn ctx(
	tag: &str,
	handler: impl Fn(&str) -> (u16, String) + Send + Sync + 'static,
) -> (T, Log) {
	build(tag, handler, true).await
}

/// An `off` mount context (`Tree::Remote`).
pub(crate) async fn off(
	tag: &str,
	handler: impl Fn(&str) -> (u16, String) + Send + Sync + 'static,
) -> (T, Log) {
	build(tag, handler, false).await
}

/// For tests that expect no network.
pub(crate) fn not_found(_: &str) -> (u16, String) {
	(404, "{}".into())
}

async fn build(
	tag: &str,
	handler: impl Fn(&str) -> (u16, String) + Send + Sync + 'static,
	on_demand: bool,
) -> (T, Log) {
	let (session, log) = stub(handler).await;
	let tmp = std::env::temp_dir().join(format!("cl-fuse-{tag}-{}", std::process::id()));
	let staging = tmp.join("staging");
	std::fs::create_dir_all(&staging).unwrap();
	let (tree, inodes) = if on_demand {
		let db = Arc::new(Db::open_in_memory().unwrap());
		let journal = Arc::new(Journal::new(Arc::clone(&db), tmp.join("journal")).unwrap());
		let cache = Arc::new(BlobCache::new(tmp.join("blobs"), 1 << 20, Arc::clone(&db)).unwrap());
		let poller = Arc::new(FeedPoller::new(Arc::clone(&session), Arc::clone(&db)));
		let od = OnDemand { db: Arc::clone(&db), cache, poller, journal, policy: Arc::default() };
		(Tree::Db(DbTree::new(&session, Arc::clone(&db)).unwrap(), od), Inodes::new(Some(db)))
	} else {
		(Tree::Remote(RemoteTree::new(Arc::clone(&session))), Inodes::default())
	};
	let ctx = Ctx {
		tree,
		inodes,
		uploader: Arc::new(Uploader::new(Arc::clone(&session), None)),
		session,
		staging,
		staged: Mutex::default(),
		stage_lock: tokio::sync::Mutex::default(),
		seq: AtomicU64::new(1),
		dead: AtomicBool::new(false),
		owner: Owner { uid: 0, gid: 0, read_only: false, mounted: UNIX_EPOCH },
		handles: Mutex::default(),
		next_fh: AtomicU64::new(1),
	};
	(T { ctx, tmp }, log)
}

pub(crate) fn od(ctx: &Ctx) -> &OnDemand {
	ctx.tree.on_demand().unwrap()
}

/// A tracked file `id` named `name` at the mount root, content `c-<id>`.
pub(crate) fn track(ctx: &Ctx, id: &str, name: &str) {
	od(ctx)
		.db
		.upsert(&EntryRow {
			ino: 0,
			entry_id: id.into(),
			parent_id: tree::ROOT.into(),
			drive: None,
			remote_name: name.into(),
			local_name: name.into(),
			kind: EntryKind::File,
			content_id: Some(format!("c-{id}")),
			size: 1,
			mtime: UNIX_EPOCH,
			created_at: String::new(),
			updated_at: 0,
			status: db::LIVE.into(),
			hydrated: false,
		})
		.unwrap();
}
