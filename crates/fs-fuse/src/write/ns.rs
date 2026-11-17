// SPDX-FileCopyrightText: Szilárd Hajba
// SPDX-License-Identifier: MPL-2.0

//! Namespace ops: mkdir, unlink, rmdir, rename.

use std::time::SystemTime;

use cloudillo_desktop_core::api::{FileView, Parent};
use cloudillo_desktop_core::{Child, ChildKind, EntryKind, OpKind, PendingOp, journal, launcher};
use fuser::{Errno, FileAttr, FileType, RenameFlags};

use super::commit::{commit_now, drive_of, enqueue, new_row};
use super::hold::{adopt_held, fire_held, hold, replace_with, take_orphan};
use super::{discard_staged, settle, staged, staged_lookup, staged_new_in};
use crate::{Ctx, Fail};

pub(super) async fn find(ctx: &Ctx, dir: &str, name: &str) -> Result<Option<Child>, Fail> {
	Ok(ctx.tree.children(dir).await?.into_iter().find(|c| c.local_name == name))
}

/// `DELETE /api/files/{id}` (to the trash; recoverable).
pub(super) async fn trash(ctx: &Ctx, id: &str) -> Result<(), Fail> {
	if let Some(od) = ctx.tree.on_demand() {
		let id = od.journal.resolve(id)?;
		let hint = od.db.get(&id)?.map(|r| r.local_name).unwrap_or_default();
		fire_held(ctx, od, Some(&id))?;
		enqueue(od, PendingOp::new(OpKind::Trash, &id, &hint))?;
		od.db.mark_removed(&id)?;
		return Ok(());
	}
	ctx.session.call(|api| async move { api.trash(id).await }).await?;
	ctx.record("trash", |od| od.db.mark_removed(id).map(drop));
	Ok(())
}

pub async fn mkdir(ctx: &Ctx, parent: u64, name: &str) -> Result<FileAttr, Fail> {
	let dir = ctx.dir_node(parent)?;
	let to = ctx.tree.parent_of(&dir);
	let remote = name.to_owned();
	if let Some(od) = ctx.tree.on_demand() {
		let mut op = PendingOp::new(OpKind::Mkdir, "", name);
		op.parent_id = Some(dir.clone());
		op.drive = drive_of(ctx, od, &dir);
		op.name = Some(remote.clone());
		let seq = enqueue(od, op.clone())?;
		let fv = FileView {
			id: journal::local_id(seq),
			file_name: remote,
			channel: op.drive,
			..FileView::default()
		};
		// the row first, so the inode is `entries.ino + 1`
		// journaled: the op is the source of truth now, so a retry must not create it twice
		if let Err(e) = new_row(od, &fv, &dir, name, EntryKind::Folder, 0) {
			tracing::warn!("row for new folder {name}: {e}; the feed restores it after sync");
		}
		let ino = ctx.inodes.ino_for(&fv.id, parent);
		return Ok(ctx.owner.raw(ino, FileType::Directory, 0o755, 0, SystemTime::now()));
	}
	let (to, remote) = (&to, remote.as_str());
	let fv = ctx.session.call(|api| async move { api.mkdir(to, remote).await }).await?;
	ctx.record("mkdir", |od| new_row(od, &fv, &dir, name, EntryKind::Folder, 0));
	ctx.tree.invalidate(&dir);
	// Relist so the next callback (`mkdir -p`, `cd`) knows the folder.
	if let Err(e) = ctx.tree.children(&dir).await {
		tracing::warn!("relisting after mkdir {name}: {e}");
	}
	let ino = ctx.inodes.ino_for(&fv.id, parent);
	Ok(ctx.owner.raw(ino, FileType::Directory, 0o755, 0, SystemTime::now()))
}

pub async fn unlink(ctx: &Ctx, parent: u64, name: &str) -> Result<(), Fail> {
	let dir = ctx.dir_node(parent)?;
	settle(ctx, parent, name).await;
	if let Some(ino) = staged_lookup(ctx, parent, name) {
		// Never uploaded (still open, or its upload failed): drop it locally.
		discard_staged(ctx, ino)?;
		return Ok(());
	}
	let Some(c) = find(ctx, &dir, name).await? else {
		// the away-name of an atomic save: local only, nothing to trash
		return if take_orphan(ctx, parent, name) { Ok(()) } else { Err(Errno::ENOENT.into()) };
	};
	if matches!(c.kind, ChildKind::Folder | ChildKind::RoomFolder) {
		return Err(Errno::EISDIR.into());
	}
	// An open writer or a debounced dirty copy must not upload (and so restore) it afterwards.
	let ino = ctx.inodes.ino_for(&c.entry_id, parent);
	if staged(ctx).contains_key(&ino) {
		discard_staged(ctx, ino)?;
	}
	if let Some(od) = ctx.tree.on_demand()
		&& matches!(c.kind, ChildKind::File)
	{
		let id = od.journal.resolve(&c.entry_id)?;
		hold(ctx, od, (parent, name), PendingOp::new(OpKind::Trash, &id, name), None)?;
		od.db.mark_removed(&id)?;
	} else {
		trash(ctx, &c.entry_id).await?;
	}
	ctx.tree.invalidate(&dir);
	Ok(())
}

pub async fn rmdir(ctx: &Ctx, parent: u64, name: &str) -> Result<(), Fail> {
	let dir = ctx.dir_node(parent)?;
	let c = find(ctx, &dir, name).await?.ok_or(Errno::ENOENT)?;
	match c.kind {
		ChildKind::RoomFolder => return Err(Errno::EPERM.into()),
		ChildKind::File | ChildKind::Launcher { .. } => return Err(Errno::ENOTDIR.into()),
		ChildKind::Folder => {}
	}
	if !dir_empty(ctx, ctx.inodes.ino_for(&c.entry_id, parent), &c.entry_id).await? {
		return Err(Errno::ENOTEMPTY.into());
	}
	trash(ctx, &c.entry_id).await?;
	ctx.tree.invalidate(&dir);
	Ok(())
}

/// Same folder -> patch the name; other folder -> patch the parent (and name if changed);
/// other drive -> EXDEV without calling the server; virtual room folder -> EPERM. An existing
/// target: `RENAME_NOREPLACE` -> EEXIST, folder -> EISDIR / ENOTEMPTY, file -> trashed first
/// (`on-demand`: a not yet journaled file renamed over a tracked file replaces it instead).
/// `on-demand` renames of tracked files are held (atomic save, see [`hold`]).
pub async fn rename(
	ctx: &Ctx,
	(parent, name): (u64, &str),
	(newparent, newname): (u64, &str),
	flags: RenameFlags,
) -> Result<(), Fail> {
	if flags.intersects(RenameFlags::RENAME_EXCHANGE | RenameFlags::RENAME_WHITEOUT) {
		return Err(Errno::EINVAL.into());
	}
	let dir = ctx.dir_node(parent)?;
	let dest = ctx.dir_node(newparent)?;
	settle(ctx, parent, name).await;
	settle(ctx, newparent, newname).await;
	let target = find(ctx, &dest, newname).await?;
	let src_staged = staged_lookup(ctx, parent, name);
	let st_target = staged_lookup(ctx, newparent, newname).filter(|&t| Some(t) != src_staged);

	if let Some(ino) = src_staged {
		let to = (newparent, dest.as_str(), newname);
		return rename_staged(ctx, ino, (target.as_ref(), st_target), to, flags).await;
	}

	let src = find(ctx, &dir, name).await?.ok_or(Errno::ENOENT)?;
	let src_dir = match src.kind {
		ChildKind::RoomFolder => return Err(Errno::EPERM.into()),
		ChildKind::Folder => true,
		ChildKind::File | ChildKind::Launcher { .. } => false,
	};
	if target.as_ref().is_some_and(|t| t.entry_id == src.entry_id) {
		return Ok(());
	}
	let to = ctx.tree.parent_of(&dest);
	// `None` = drive unknown (e.g. the root of a folder target): the server decides, and its
	// CROSSROOM error maps to EXDEV too.
	let dest_drive = match &to {
		Parent::DriveRoot { channel } => Some(channel.clone()),
		Parent::Folder(id) => ctx.tree.get(id).map(|c| c.drive),
	};
	if dest_drive.is_some_and(|d| d != src.drive) {
		return Err(Errno::EXDEV.into());
	}
	if let Some(t) = st_target {
		drop_staged_target(ctx, src_dir, t, flags)?;
	}
	if let Some(t) = &target {
		clear_target(ctx, src_dir, (newparent, t), flags).await?;
	}
	let new_name = (newname != name).then(|| {
		let base = match src.kind {
			ChildKind::Launcher { .. } => newname.strip_suffix(launcher::SUFFIX).unwrap_or(newname),
			_ => newname,
		};
		base.to_owned()
	});
	let new_parent = (dir != dest).then_some(to);
	let (id, nm, np) = (src.entry_id.as_str(), new_name.as_deref(), new_parent.as_ref());
	if let Some(od) = ctx.tree.on_demand() {
		let kind = if np.is_some() { OpKind::Move } else { OpKind::Rename };
		let mut op = PendingOp::new(kind, id, newname);
		op.parent_id = np.is_some().then(|| dest.clone());
		op.drive.clone_from(&src.drive);
		op.name.clone_from(&new_name);
		if target.is_none() && matches!(src.kind, ChildKind::File) {
			// a rename-away (vim `file` -> `file~`) may be an atomic save
			hold(ctx, od, (parent, name), op, Some((newparent, newname.to_owned())))?;
		} else {
			fire_held(ctx, od, Some(&od.journal.resolve(id)?))?;
			enqueue(od, op)?;
		}
	} else {
		ctx.session.call(|api| async move { api.patch(id, nm, np).await }).await?;
	}
	ctx.record("rename", |od| {
		// Same row, new parent/name: a move is never delete + create.
		if let Some(mut row) = od.db.get(id)? {
			row.parent_id.clone_from(&dest);
			newname.clone_into(&mut row.local_name);
			if let Some(n) = &new_name {
				row.remote_name.clone_from(n);
			}
			od.db.upsert(&row)?;
		}
		Ok(())
	});
	ctx.tree.invalidate(&dir);
	ctx.tree.invalidate(&dest);
	ctx.inodes.ino_for(&src.entry_id, newparent);
	Ok(())
}

/// [`rename`] of `ino`, a file not uploaded yet: renamed locally, the upload goes to the new
/// place. `target` / `st_target`: the entry / new staged file `newname` in `dest` (inode
/// `newparent`) names now.
pub(super) async fn rename_staged(
	ctx: &Ctx,
	ino: u64,
	(target, st_target): (Option<&Child>, Option<u64>),
	(newparent, dest, newname): (u64, &str, &str),
	flags: RenameFlags,
) -> Result<(), Fail> {
	if let Some(t) = st_target {
		drop_staged_target(ctx, false, t, flags)?;
	}
	let od = ctx.tree.on_demand();
	// atomic save (gedit / LibreOffice rename-over): the tracked target is replaced
	let over = target.filter(|t| {
		od.is_some()
			&& matches!(t.kind, ChildKind::File)
			&& !flags.contains(RenameFlags::RENAME_NOREPLACE)
	});
	if let Some(t) = target.filter(|_| over.is_none()) {
		clear_target(ctx, false, (newparent, t), flags).await?;
	}
	if let Some(t) = over {
		// the target's own unjournaled save goes first, so this replace is based on it
		let prev = staged(ctx).iter().find_map(|(&k, r)| {
			(r.writers == 0 && r.remote.as_ref().is_some_and(|x| x.0 == t.entry_id)).then_some(k)
		});
		if let Some(k) = prev {
			commit_now(ctx, k).await?;
		}
	}
	let remote = match (od, over) {
		(Some(od), Some(t)) => Some(replace_with(ctx, od, ino, dest, newname, &t.entry_id)?),
		(Some(od), None) => adopt_held(ctx, od, ino, newparent, newname),
		(None, _) => None,
	};
	let idle = staged(ctx).get_mut(&ino).is_some_and(|rec| {
		rec.parent = newparent;
		newname.clone_into(&mut rec.name);
		if remote.is_some() {
			rec.remote = remote;
		}
		// kept local or failed, so no commit is coming: commit it as what it is now
		let idle = od.is_some() && rec.writers == 0 && !rec.debouncing && !rec.committing;
		if idle {
			rec.touch();
		}
		idle
	});
	if let Some(id) = ctx.node(ino) {
		ctx.inodes.ino_for(&id, newparent);
	}
	ctx.tree.invalidate(dest);
	if idle {
		commit_now(ctx, ino).await?;
	}
	Ok(())
}

/// Folder `id` (inode `ino`) has no entries and no new files staged in it.
pub(super) async fn dir_empty(ctx: &Ctx, ino: u64, id: &str) -> Result<bool, Fail> {
	Ok(ctx.tree.children(id).await?.is_empty() && staged_new_in(ctx, ino).is_empty())
}

/// Make room for a rename onto new file `t`, not uploaded yet: it is dropped locally.
pub(super) fn drop_staged_target(
	ctx: &Ctx,
	src_dir: bool,
	t: u64,
	flags: RenameFlags,
) -> Result<(), Fail> {
	if flags.contains(RenameFlags::RENAME_NOREPLACE) {
		return Err(Errno::EEXIST.into());
	}
	if src_dir {
		return Err(Errno::ENOTDIR.into());
	}
	Ok(discard_staged(ctx, t)?)
}

/// Make room for a rename onto existing `t` in `newparent`, per POSIX (`src_dir`: the source
/// is a folder).
pub(super) async fn clear_target(
	ctx: &Ctx,
	src_dir: bool,
	(newparent, t): (u64, &Child),
	flags: RenameFlags,
) -> Result<(), Fail> {
	if flags.contains(RenameFlags::RENAME_NOREPLACE) {
		return Err(Errno::EEXIST.into());
	}
	match t.kind {
		ChildKind::RoomFolder => return Err(Errno::EPERM.into()),
		ChildKind::Folder if !src_dir => return Err(Errno::EISDIR.into()),
		ChildKind::Folder => {
			if !dir_empty(ctx, ctx.inodes.ino_for(&t.entry_id, newparent), &t.entry_id).await? {
				return Err(Errno::ENOTEMPTY.into());
			}
		}
		ChildKind::File | ChildKind::Launcher { .. } if src_dir => {
			return Err(Errno::ENOTDIR.into());
		}
		ChildKind::File | ChildKind::Launcher { .. } => {}
	}
	// POSIX replace: the target goes to the trash (its id and history are lost in `off` mode).
	trash(ctx, &t.entry_id).await
}
