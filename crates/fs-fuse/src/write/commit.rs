// SPDX-FileCopyrightText: Szilárd Hajba
// SPDX-License-Identifier: MPL-2.0

//! Commit: debounce, journal (`on-demand`) or upload (`off`) a staged file.

use std::path::Path;
use std::time::{Duration, SystemTime};

use cloudillo_desktop_core::api::{FileView, Parent};
use cloudillo_desktop_core::ignore::{self, Kept};
use cloudillo_desktop_core::{
	EntryKind, EntryRow, OpKind, PendingOp, Replaced, atomic, db, journal, names, tree,
};
use fuser::Errno;

use super::hold::fire_held;
use super::ns::trash;
use super::{remove_staging, staged};
use crate::{Ctx, Fail, OnDemand};

/// Upload (`on-demand`: journal, after the debounce) `ino`'s staging file if it changed since
/// the last upload. On success the staging file is deleted once no writer is left; on failure
/// it stays (path logged).
pub async fn commit(ctx: &Ctx, ino: u64) -> Result<(), Fail> {
	if ctx.tree.on_demand().is_some() && !quiet(ctx, ino).await {
		return Ok(());
	}
	commit_now(ctx, ino).await
}

/// `on-demand` debounce: wait until `ino` saw no change for [`atomic::QUIET`]. `false` = do
/// not commit (gone, or reopened while waiting: its release commits). A file open for writing
/// at the start (`fsync`) is not waited for.
pub(super) async fn quiet(ctx: &Ctx, ino: u64) -> bool {
	let mut first = true;
	loop {
		let left = {
			let mut st = staged(ctx);
			let Some(rec) = st.get_mut(&ino) else {
				return false;
			};
			if rec.writers > 0 {
				if !first {
					rec.debouncing = false;
				}
				return first;
			}
			first = false;
			let left = atomic::QUIET.saturating_sub(rec.touched.elapsed());
			rec.debouncing = !left.is_zero();
			if left.is_zero() {
				return true;
			}
			left
		};
		tokio::time::sleep(left).await;
	}
}

/// Inodes and names of files whose last upload failed (hidden held/orphan records excluded).
pub(crate) fn failed(ctx: &Ctx) -> Vec<(u64, String)> {
	staged(ctx)
		.iter()
		.filter(|(_, r)| r.failed && r.held.is_none() && !r.orphan)
		.map(|(&ino, r)| (ino, r.name.clone()))
		.collect()
}

/// Upload every failed file once more (a failed record keeps `edits != committed`).
pub(crate) async fn retry(ctx: &Ctx) {
	for (ino, _) in failed(ctx) {
		let _ = commit_now(ctx, ino).await;
	}
}

/// After unmount (no callback can arrive): journal / upload what is still held, debounced or
/// open, and wait for commits in flight.
pub(crate) async fn finish(ctx: &Ctx) {
	if let Some(od) = ctx.tree.on_demand()
		&& let Err(e) = fire_held(ctx, od, None)
	{
		tracing::warn!("journaling held unlinks / renames: {e:?}");
	}
	let (dirty, clean) = {
		let mut st = staged(ctx);
		st.retain(|_, r| !r.orphan);
		let (mut dirty, mut clean) = (Vec::new(), Vec::new());
		for (&ino, r) in st.iter_mut() {
			r.writers = 0;
			if r.held.is_none() && r.local.is_none() && !r.committing && r.edits == r.committed {
				clean.push(ino);
			} else if r.local.is_none() && (r.unlinked || r.edits != r.committed) {
				dirty.push(ino);
			}
		}
		let clean: Vec<_> = clean.into_iter().filter_map(|k| st.remove(&k)?.path).collect();
		(dirty, clean)
	};
	for p in clean {
		remove_staging(Some(p));
	}
	for ino in dirty {
		let _ = commit_now(ctx, ino).await;
	}
	while staged(ctx).values().any(|r| r.committing) {
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
}

/// Held ops and closed or open changes not journaled / uploaded yet (failed ones excluded).
pub(crate) fn pending(ctx: &Ctx) -> usize {
	staged(ctx)
		.values()
		.filter(|r| {
			r.held.as_ref().is_some_and(|h| !h.hold.claimed())
				|| (!r.unlinked && !r.failed && r.local.is_none() && r.edits != r.committed)
		})
		.count()
}

pub(super) async fn commit_now(ctx: &Ctx, ino: u64) -> Result<(), Fail> {
	loop {
		let (path, remote, parent, name, edits) = {
			let mut st = staged(ctx);
			let Some(rec) = st.get_mut(&ino) else {
				return Ok(());
			};
			if rec.unlinked && rec.writers == 0 && !rec.committing && rec.path.is_some() {
				// discarded while debouncing: never uploaded
				let path = st.remove(&ino).and_then(|r| r.path);
				drop(st);
				remove_staging(path);
				return Ok(());
			}
			if rec.committing || rec.unlinked || rec.edits == rec.committed {
				return Ok(());
			}
			let Some(path) = rec.path.clone() else {
				return Ok(());
			};
			rec.committing = true;
			rec.failed = false;
			(path, rec.remote.clone(), rec.parent, rec.name.clone(), rec.edits)
		};
		let od = ctx.tree.on_demand();
		let created = remote.is_none();
		let kept = od.and_then(|od| keep_local(&od.policy, &path, created, &name));
		let res = match od {
			_ if kept.is_some() => Ok(None),
			Some(od) => enqueue_upload(ctx, od, ino, &path, remote, parent, &name).map(Some),
			None => upload(ctx, ino, &path, remote, parent, &name).await.map(Some),
		};
		// a block: the guard must be gone before the `.await` below
		let undo = {
			let mut st = staged(ctx);
			let Some(rec) = st.get_mut(&ino) else {
				return res.map(drop);
			};
			rec.committing = false;
			if rec.unlinked {
				// discarded during the commit; a create that landed meanwhile is trashed again
				let path =
					if rec.writers == 0 { st.remove(&ino).and_then(|r| r.path) } else { None };
				drop(st);
				remove_staging(path);
				match res {
					Ok(Some((id, _))) if created => id,
					r => return r.map(drop),
				}
			} else {
				rec.local = kept;
				if let Some(od) = od {
					od.policy.mark(ino, kept);
				}
				match res {
					// the record stays: it serves (and, if new, lists) the content
					Ok(None) => {
						rec.committed = edits;
						tracing::info!(
							"{name} not synced ({kept:?}); kept local at {}",
							path.display()
						);
						return Ok(());
					}
					Ok(Some(landed)) => {
						rec.committed = edits;
						rec.remote = Some(landed);
						if rec.writers > 0 {
							return Ok(());
						}
						if rec.edits != edits {
							continue; // changed while uploading, and no writer is left to commit it
						}
						st.remove(&ino);
						drop(st);
						if let Err(e) = std::fs::remove_file(&path) {
							tracing::warn!("removing {}: {e}", path.display());
						}
						return Ok(());
					}
					Err(f) => {
						rec.failed = true;
						tracing::warn!(
							"upload of {name} failed; staged copy kept at {}",
							path.display()
						);
						return Err(f);
					}
				}
			}
		};
		let r = trash(ctx, &undo).await;
		if let Some(dir) = ctx.node(parent) {
			ctx.tree.invalidate(&dir);
		}
		return r;
	}
}

/// One create/replace. Returns the entry id and its landed content id (the next base).
pub(super) async fn upload(
	ctx: &Ctx,
	ino: u64,
	path: &Path,
	remote: Option<(String, Option<String>)>,
	parent: u64,
	name: &str,
) -> Result<(String, Option<String>), Fail> {
	let dir = ctx.node(parent).ok_or(Errno::ENOENT)?;
	let to = ctx.tree.parent_of(&dir);
	let remote_name = name.to_owned();
	let Some((id, base)) = remote else {
		let fv = ctx.uploader.create(&to, &remote_name, path).await?;
		ctx.record("upload", |od| {
			new_row(od, &fv, &dir, name, EntryKind::File, file_len(path))?;
			od.cache.put(fv.content_id.as_deref(), path)
		});
		ctx.inodes.rebind(ino, &fv.id);
		if let Some(rec) = staged(ctx).get_mut(&ino) {
			rec.remote = Some((fv.id.clone(), fv.content_id.clone()));
		}
		ctx.tree.invalidate(&dir);
		// Relist so the entry is known before its staged record goes away.
		if let Err(e) = ctx.tree.children(&dir).await {
			tracing::warn!("relisting after upload of {name}: {e}");
		}
		return Ok((fv.id, fv.content_id));
	};
	let base = match base {
		Some(b) => b,
		None => ctx.uploader.base(&id).await?.ok_or(Errno::EIO)?,
	};
	let landed = match ctx.uploader.replace(&id, &base, &to, &remote_name, path).await? {
		Replaced::Landed(c) => {
			// `None` = the content id was not known in time: the feed brings it.
			if let Some(cid) = &c {
				ctx.record("upload", |od| {
					if let Some(mut row) = od.db.get(&id)? {
						row.content_id = Some(cid.clone());
						row.size = file_len(path);
						row.mtime = SystemTime::now();
						row.hydrated = false; // set again by `BlobCache::put`
						od.db.upsert(&row)?;
					}
					od.cache.put(Some(cid.as_str()), path)
				});
			}
			c
		}
		Replaced::Conflict(fv) => {
			tracing::warn!(
				"{name} changed remotely while open; the local version was saved as \"{}\"",
				fv.file_name
			);
			Some(base)
		}
	};
	ctx.tree.invalidate(&dir);
	Ok((id, landed))
}

/// `on-demand` commit: journal a Create / Replace of `path` (the journal copies it) and update
/// the row at once. Returns the entry id (`local:<seq>` if new) and the content token.
pub(super) fn enqueue_upload(
	ctx: &Ctx,
	od: &OnDemand,
	ino: u64,
	path: &Path,
	remote: Option<(String, Option<String>)>,
	parent: u64,
	name: &str,
) -> Result<(String, Option<String>), Fail> {
	let size = file_len(path);
	let Some((id, base)) = remote else {
		let dir = ctx.node(parent).ok_or(Errno::ENOENT)?;
		let mut op = PendingOp::new(OpKind::Create, "", name);
		op.parent_id = Some(dir.clone());
		op.drive = drive_of(ctx, od, &dir);
		op.name = Some(name.to_owned());
		let seq = od.journal.enqueue(&op, Some(path))?;
		let fv = FileView {
			id: journal::local_id(seq),
			content_id: Some(journal::pending_cid(seq)),
			file_name: name.to_owned(),
			channel: op.drive,
			..FileView::default()
		};
		// journaled: the op is the source of truth now, so a retry must not create it twice
		if let Err(e) = new_row(od, &fv, &dir, name, EntryKind::File, size) {
			tracing::warn!("row for new file {name}: {e}; the feed restores it after sync");
		}
		ctx.inodes.rebind(ino, &fv.id);
		return Ok((fv.id, fv.content_id));
	};
	let id = od.journal.resolve(&id)?;
	let mut row = od.db.get(&id)?.ok_or(Errno::ENOENT)?;
	let mut op = PendingOp::new(OpKind::Replace, &id, name);
	op.parent_id = Some(row.parent_id.clone());
	op.drive.clone_from(&row.drive);
	op.name = Some(row.remote_name.clone());
	op.base_content_id = base.or_else(|| row.content_id.clone());
	let cid = journal::pending_cid(od.journal.enqueue(&op, Some(path))?);
	row.content_id = Some(cid.clone());
	row.size = size;
	row.mtime = SystemTime::now();
	row.hydrated = false;
	if let Err(e) = od.db.upsert(&row) {
		tracing::warn!("row for {name}: {e}; the feed restores it after sync");
	}
	Ok((id, Some(cid)))
}

/// `on-demand`: why a staged file must not be journaled (ignore patterns apply to new files).
pub(super) fn keep_local(
	policy: &ignore::Policy,
	path: &Path,
	new: bool,
	name: &str,
) -> Option<Kept> {
	if new && policy.ignored(name) {
		Some(Kept::Ignored)
	} else if file_len(path) > policy.max_bytes {
		Some(Kept::TooLarge)
	} else {
		None
	}
}

/// `on-demand`: drive of the entries in node `dir`.
pub(super) fn drive_of(ctx: &Ctx, od: &OnDemand, dir: &str) -> Option<String> {
	match ctx.tree.parent_of(dir) {
		Parent::DriveRoot { channel } => channel,
		Parent::Folder(id) => match ctx.tree.get(&id) {
			Some(c) => c.drive,
			// the root of a folder target is no row
			None => od.db.node_drive(dir).ok().flatten(),
		},
	}
}

/// `on-demand`: journal a content-less `op` (its entry id resolved first).
pub(super) fn enqueue(od: &OnDemand, mut op: PendingOp) -> Result<i64, Fail> {
	op.entry_id = od.journal.resolve(&op.entry_id)?;
	Ok(od.journal.enqueue(&op, None)?)
}

pub(super) fn file_len(path: &Path) -> u64 {
	std::fs::metadata(path).map_or(0, |m| m.len())
}

/// `on-demand`: the row of entry `fv`, just created here as local name `name` in node `dir`.
/// `updated_at = 0`, so the feed's version of it always applies (keeping the sticky name).
pub(super) fn new_row(
	od: &OnDemand,
	fv: &FileView,
	dir: &str,
	name: &str,
	kind: EntryKind,
	size: u64,
) -> cloudillo_desktop_core::Result<()> {
	let created = tree::parse_time(&fv.created_at);
	let drive = fv.channel.as_deref();
	let local = names::assign_sticky(&od.db, dir, drive, &[(fv.id.as_str(), name, created)])?;
	let local_name = local.into_iter().next().map_or_else(|| name.to_owned(), |(_, n)| n);
	od.db.upsert(&EntryRow {
		ino: 0,
		entry_id: fv.id.clone(),
		parent_id: dir.to_owned(),
		drive: fv.channel.clone(),
		remote_name: fv.file_name.clone(),
		local_name,
		kind,
		content_id: fv.content_id.clone(),
		size,
		mtime: SystemTime::now(),
		created_at: fv.created_at.clone(),
		updated_at: 0,
		status: db::LIVE.to_owned(),
		hydrated: false,
	})?;
	Ok(())
}
