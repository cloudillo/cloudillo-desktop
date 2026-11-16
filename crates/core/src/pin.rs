//! Pinned subtrees: kept hydrated for offline use and never LRU-evicted
//! ([`BlobCache::evict_to_fit`]). The flag is `entries.pinned`; an entry is pinned when it or
//! any ancestor carries it. A `pinned` mount pins its root, i.e. every entry (`whole`).

use crate::cache::BlobCache;
use crate::db::Db;
use crate::error::Result;
use crate::session::Session;

/// Pin entry `entry_id` (file or folder) and so everything under it.
pub fn pin(db: &Db, entry_id: &str) -> Result<()> {
	db.set_pinned(entry_id, true)
}

/// Clear the pin of `entry_id` and of everything under it. `true` if an ancestor still pins it.
pub fn unpin(db: &Db, entry_id: &str) -> Result<bool> {
	db.set_pinned(entry_id, false)?;
	db.pinned_above(entry_id)
}

/// Download every pinned file not cached yet (every file when `whole`). Run after feed
/// updates. A failed file does not stop the others; the last error is returned.
pub async fn hydrate_pinned(
	db: &Db,
	cache: &BlobCache,
	session: &Session,
	whole: bool,
) -> Result<()> {
	let mut res = Ok(());
	for (entry_id, cid) in db.pinned_files(whole)? {
		if cache.get(&cid).is_none()
			&& let Err(e) = cache.hydrate(session, &entry_id, &cid).await
		{
			res = Err(e);
		}
	}
	res
}
