//! Content-addressed blob cache: `<dir>/<content_id>`, LRU state in [`Db`]'s `cache_lru`.
//!
//! A remote replace changes the content id, so it simply misses the cache.

use std::collections::HashSet;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::db::Db;
use crate::error::{ClientError, Result};
use crate::journal;
use crate::session::Session;

/// Default `cache_max_mb`.
pub const DEFAULT_MAX_MB: u64 = 10_240;

#[derive(Debug)]
pub struct BlobCache {
	dir: PathBuf,
	max_bytes: u64,
	db: Arc<Db>,
	part_seq: AtomicU64,
}

/// Content ids come from the server and become file names: allow only a safe alphabet.
fn checked(content_id: &str) -> Result<&str> {
	let ok = !content_id.is_empty()
		&& !content_id.starts_with('.')
		&& content_id.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.~".contains(&b))
		// `.part` names are in-flight downloads, swept on startup
		&& !Path::new(content_id).extension().is_some_and(|e| e.eq_ignore_ascii_case("part"));
	if ok { Ok(content_id) } else { Err(ClientError::Decode("bad content id".into())) }
}

impl BlobCache {
	/// `dir` is created if missing; leftover `.part` files of a crashed run are removed.
	pub fn new(dir: PathBuf, max_bytes: u64, db: Arc<Db>) -> Result<Self> {
		std::fs::create_dir_all(&dir)?;
		for e in std::fs::read_dir(&dir)?.flatten() {
			if e.file_name().to_string_lossy().ends_with(".part") {
				let _ = std::fs::remove_file(e.path());
			}
		}
		Ok(Self { dir, max_bytes, db, part_seq: AtomicU64::new(0) })
	}

	/// Path of the cached blob, if present. Does not touch the LRU.
	pub fn get(&self, content_id: &str) -> Option<PathBuf> {
		let p = self.dir.join(checked(content_id).ok()?);
		p.is_file().then_some(p)
	}

	/// Mark a cached blob as just used.
	pub fn touch(&self, content_id: &str) -> Result<()> {
		let p = self.get(content_id).ok_or(ClientError::NotFound)?;
		self.db.lru_touch(content_id, std::fs::metadata(p)?.len())
	}

	/// Cached path of `content_id`, downloading entry `entry_id` in full first on a miss.
	/// Through `Session::call`, so a brokered 401 re-brokers once. Call
	/// [`Self::evict_to_fit`] afterwards.
	pub async fn hydrate(
		&self,
		session: &Session,
		entry_id: &str,
		content_id: &str,
	) -> Result<PathBuf> {
		// a stale local token (its upload has landed meanwhile): use the entry's current id
		let resolved;
		let content_id = if content_id.starts_with(journal::PENDING_PREFIX) {
			resolved = session
				.call(|api| async move { api.get_entry(entry_id).await })
				.await?
				.content_id
				.ok_or(ClientError::NotFound)?;
			resolved.as_str()
		} else {
			content_id
		};
		let dest = self.dir.join(checked(content_id)?);
		if !dest.is_file() {
			let seq = self.part_seq.fetch_add(1, Ordering::Relaxed);
			let part = self.dir.join(format!("{content_id}.{seq}.part"));
			let res = session
				.call(|api| {
					let part = part.clone();
					async move {
						// no content id on the download: make sure `content_id` is what the
						// entry held throughout, or a stale/newer blob lands under its name
						let current = async || {
							Ok::<_, ClientError>(
								api.get_entry(entry_id).await?.content_id.as_deref()
									== Some(content_id),
							)
						};
						if !current().await? {
							return Err(ClientError::PreconditionFailed);
						}
						let mut dl = api.download(entry_id, None).await?;
						// blocking std::fs writes on the async thread (as upload.rs
						// does); tokio "fs" if hydrates ever stall other tasks.
						let mut f = std::fs::File::create(&part)?;
						while let Some(chunk) = dl.chunk().await? {
							f.write_all(&chunk)?;
						}
						f.sync_all()?;
						if !current().await? {
							return Err(ClientError::PreconditionFailed);
						}
						Ok(())
					}
				})
				.await;
			if let Err(e) = res.and_then(|()| Ok(std::fs::rename(&part, &dest)?)) {
				let _ = std::fs::remove_file(&part);
				return Err(e);
			}
		}
		self.db.lru_touch(content_id, std::fs::metadata(&dest)?.len())?;
		Ok(dest)
	}

	/// Copy local file `src` into the cache as `content_id` (write-through after an upload).
	/// Call [`Self::evict_to_fit`] afterwards.
	pub fn insert(&self, content_id: &str, src: &Path) -> Result<PathBuf> {
		let dest = self.dir.join(checked(content_id)?);
		if !dest.is_file() {
			let seq = self.part_seq.fetch_add(1, Ordering::Relaxed);
			let part = self.dir.join(format!("{content_id}.{seq}.part"));
			if let Err(e) = std::fs::copy(src, &part).and_then(|_| std::fs::rename(&part, &dest)) {
				let _ = std::fs::remove_file(&part);
				return Err(e.into());
			}
		}
		self.db.lru_touch(content_id, std::fs::metadata(&dest)?.len())?;
		Ok(dest)
	}

	/// Write-through of uploaded content: [`Self::insert`] unless `content_id` is unknown or
	/// pending (`@…`, it changes once the server finalizes it).
	// no eviction here; the next FUSE `open` evicts down to the limit.
	pub fn put(&self, content_id: Option<&str>, path: &Path) -> Result<()> {
		if let Some(cid) = content_id.filter(|c| !c.starts_with('@')) {
			self.insert(cid, path)?;
		}
		Ok(())
	}

	/// Evict least recently used blobs until the total fits `max_bytes`, skipping `open`
	/// content ids and pinned files (`crate::pin`).
	pub fn evict_to_fit(&self, open: &HashSet<String>) -> Result<()> {
		let pinned: HashSet<String> =
			self.db.pinned_files(false)?.into_iter().map(|(_, cid)| cid).collect();
		let mut total = self.db.lru_total()?;
		for (cid, size) in self.db.lru_oldest()? {
			if total <= self.max_bytes {
				break;
			}
			if open.contains(&cid) || pinned.contains(&cid) {
				continue;
			}
			if let Ok(cid_ok) = checked(&cid) {
				match std::fs::remove_file(self.dir.join(cid_ok)) {
					Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e.into()),
					_ => {}
				}
			}
			self.db.lru_remove(&cid)?;
			total = total.saturating_sub(size);
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn checked_rejects_part_suffix() {
		assert!(checked("x.part").is_err());
		assert!(checked("x.0.part").is_err());
		assert!(checked("b1~abc").is_ok());
	}

	#[test]
	fn evicts_lru_skipping_open() -> Result<()> {
		let dir = std::env::temp_dir().join(format!("cloudillo-cache-{}", std::process::id()));
		let db = Arc::new(Db::open_in_memory()?);
		let cache = BlobCache::new(dir.clone(), 6, db.clone())?;
		for cid in ["a", "b", "c"] {
			std::fs::write(dir.join(cid), b"1234")?;
			cache.touch(cid)?;
		}
		assert!(cache.get("../x").is_none());
		let open: HashSet<String> = ["a".to_owned()].into();
		cache.evict_to_fit(&open)?;
		// 12 bytes > 6: "a" is open, "b" goes, then 8 > 6 so "c" goes too.
		assert!(cache.get("a").is_some());
		assert!(cache.get("b").is_none() && cache.get("c").is_none());
		assert_eq!(db.lru_total()?, 4);
		std::fs::remove_dir_all(dir)?;
		Ok(())
	}

	#[test]
	fn put_skips_unknown_and_pending() -> Result<()> {
		let dir = std::env::temp_dir().join(format!("cloudillo-put-{}", std::process::id()));
		let cache = BlobCache::new(dir.clone(), 1 << 20, Arc::new(Db::open_in_memory()?))?;
		let src = dir.join("src.part.tmp");
		std::fs::write(&src, b"data")?;
		cache.put(None, &src)?;
		cache.put(Some("@p1"), &src)?;
		assert!(cache.get("@p1").is_none());
		cache.put(Some("c1"), &src)?;
		assert_eq!(cache.get("c1").map(std::fs::read).transpose()?, Some(b"data".to_vec()));
		std::fs::remove_dir_all(dir)?;
		Ok(())
	}

	#[tokio::test]
	async fn hydrate_checks_content_id() -> Result<()> {
		let (session, _) = crate::stub::stub(|l| {
			if l.starts_with("GET /api/files/E1/metadata") {
				(200, r#"{"data":{"id":"E1","contentId":"c1"}}"#.into())
			} else if l.starts_with("GET /api/files/E1 ") {
				(200, "blob".into())
			} else {
				(500, String::new())
			}
		})
		.await;
		let dir = std::env::temp_dir().join(format!("cloudillo-hydrate-{}", std::process::id()));
		let cache = BlobCache::new(dir.clone(), 1 << 20, Arc::new(Db::open_in_memory()?))?;
		let stale = cache.hydrate(&session, "E1", "c0").await;
		assert!(matches!(stale, Err(ClientError::PreconditionFailed)));
		assert!(cache.get("c0").is_none());
		let p = cache.hydrate(&session, "E1", "c1").await?;
		assert_eq!(std::fs::read(p)?, b"blob");
		std::fs::remove_dir_all(&dir)?;
		// a stale pending token resolves to the entry's current content id
		let cache = BlobCache::new(dir.clone(), 1 << 20, Arc::new(Db::open_in_memory()?))?;
		let p = cache.hydrate(&session, "E1", "pending-3").await?;
		assert!(p.ends_with("c1"));
		assert_eq!(std::fs::read(p)?, b"blob");
		std::fs::remove_dir_all(dir)?;
		Ok(())
	}
}
