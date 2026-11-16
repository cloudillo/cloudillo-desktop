//! Atomic save: held unlinks / rename-aways claimed by a new file at the same place.

use cloudillo_desktop_core::conflict::LocalChange;
use cloudillo_desktop_core::{ClientError, OpKind, PendingOp, atomic, db};

use super::commit::enqueue;
use super::{Held, Staged, side_key, staged};
use crate::{Ctx, Fail, OnDemand};

/// `on-demand`: hold `op`, an unlink / rename-away of a tracked file at `(parent, name)`, for
/// [`atomic::HOLD`]; journaled then, unless a new file there claimed it ([`adopt_held`]).
// a held op lives in memory only; a crash inside the window loses it (the local row
// already shows the change; the next remote change of the entry brings it back)
pub(super) fn hold(
	ctx: &Ctx,
	od: &OnDemand,
	(parent, name): (u64, &str),
	op: PendingOp,
	away: Option<(u64, String)>,
) -> Result<(), Fail> {
	// at most one held op per entry: an earlier one is journaled first, so it replays first
	let id = od.journal.resolve(&op.entry_id)?;
	fire_held(ctx, od, Some(&id))?;
	let h = atomic::Hold::default();
	// the feed must treat the entry as locally changed while held
	let change = if op.op == OpKind::Trash { LocalChange::Trashed } else { LocalChange::Renamed };
	od.poller.hold(&id, change, h.clone());
	let mut rec = Staged::new(None, parent, name.to_owned());
	rec.unlinked = true;
	rec.held = Some(Held { hold: h.clone(), op: op.clone(), away });
	staged(ctx).insert(side_key(ctx), rec);
	let od = od.clone();
	tokio::spawn(async move {
		tokio::time::sleep(atomic::HOLD).await;
		// between claim() and enqueue the op is neither held nor journaled for a few
		// microseconds; a feed row landing exactly then revives the entry
		if h.claim()
			&& let Err(e) = enqueue(&od, op)
		{
			tracing::warn!("journaling a held unlink / rename: {e:?}");
		}
	});
	Ok(())
}

/// `on-demand`: journal now the ops held on entry `id` (`None` = all, oldest first), so later
/// ops replay after them.
pub(crate) fn fire_held(ctx: &Ctx, od: &OnDemand, id: Option<&str>) -> Result<(), Fail> {
	let ops: Vec<PendingOp> = {
		let mut st = staged(ctx);
		let mut keys: Vec<u64> = st
			.iter()
			.filter(|(_, r)| {
				r.held.as_ref().is_some_and(|h| {
					id.is_none_or(|id| od.journal.resolve(&h.op.entry_id).is_ok_and(|e| e == id))
				})
			})
			.map(|(&k, _)| k)
			.collect();
		// side keys count down: the highest is the oldest
		keys.sort_unstable_by(|a, b| b.cmp(a));
		keys.into_iter()
			.filter_map(|k| st.remove(&k)?.held)
			.filter(|h| h.hold.claim())
			.map(|h| h.op)
			.collect()
	};
	// every claimed op is enqueued even after a failure: dropping the rest would lose them
	// an op whose own enqueue fails is still lost; needs Hold unclaim support
	let mut first = Ok(());
	for op in ops {
		if let Err(e) = enqueue(od, op) {
			tracing::warn!("journaling a held unlink / rename: {e:?}");
			if first.is_ok() {
				first = Err(e);
			}
		}
	}
	first
}

/// `on-demand`: new file `ino` now stands at `(parent, name)`. If an unlink / rename-away of a
/// tracked file is held there, claim it: `ino` becomes that entry (returned as the `remote` to
/// replace), the held op is dropped and a rename-away leaves an orphan at its away-name.
pub(super) fn adopt_held(
	ctx: &Ctx,
	od: &OnDemand,
	ino: u64,
	parent: u64,
	name: &str,
) -> Option<(String, Option<String>)> {
	let dir = ctx.node(parent)?;
	let held = {
		let mut st = staged(ctx);
		st.retain(|_, r| r.held.as_ref().is_none_or(|h| !h.hold.claimed())); // fired: done
		let key = st
			.iter()
			.find(|(_, r)| r.held.is_some() && r.parent == parent && r.name == name)
			.map(|(&k, _)| k)?;
		st.remove(&key)?.held?
	};
	if !held.hold.claim() {
		return None; // the timer journaled it meanwhile
	}
	match replace_with(ctx, od, ino, &dir, name, &held.op.entry_id) {
		Ok(remote) => {
			if let Some((p, n)) = held.away {
				let mut rec = Staged::new(None, p, n);
				rec.unlinked = true;
				rec.orphan = true;
				staged(ctx).insert(side_key(ctx), rec);
			}
			Some(remote)
		}
		Err(e) => {
			tracing::warn!("atomic save of {name}: {e}; journaling the held op, saving as new");
			if let Err(e) = enqueue(od, held.op) {
				tracing::warn!("journaling a held unlink / rename: {e:?}");
			}
			None
		}
	}
}

/// `on-demand`: staged file `ino` takes the place of tracked entry `id` as local name `name` in
/// node `dir` (row revived / moved there). Returns the `remote` to replace: id + content id.
pub(super) fn replace_with(
	ctx: &Ctx,
	od: &OnDemand,
	ino: u64,
	dir: &str,
	name: &str,
	id: &str,
) -> cloudillo_desktop_core::Result<(String, Option<String>)> {
	let id = od.journal.resolve(id)?;
	let mut row = od.db.get(&id)?.ok_or(ClientError::NotFound)?;
	db::LIVE.clone_into(&mut row.status);
	dir.clone_into(&mut row.parent_id);
	name.clone_into(&mut row.local_name);
	name.clone_into(&mut row.remote_name); // a rename-away renamed it
	od.db.upsert(&row)?;
	ctx.inodes.rebind(ino, &id);
	Ok((id, row.content_id))
}

/// Drop the orphan at `(parent, name)`; `false` if there is none.
pub(super) fn take_orphan(ctx: &Ctx, parent: u64, name: &str) -> bool {
	let mut st = staged(ctx);
	let key = st
		.iter()
		.find(|(_, r)| r.orphan && r.parent == parent && r.name == name)
		.map(|(&k, _)| k);
	key.and_then(|k| st.remove(&k)).is_some()
}
