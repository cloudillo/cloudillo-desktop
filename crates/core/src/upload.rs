//! Create / replace a remote BLOB from a staged local file.
//!
//! At most one replace per entry is in flight: [`Uploader::replace`] holds the entry's lock
//! until the pending content has landed (poll metadata every 1 s, at most 60 s), and
//! [`Uploader::base`] waits on the same lock, so a new edit never starts from stale content.
//! A 412 uploads the staged file as a new sibling `name (conflict <host> <date>).ext`.
//! Too large (400) / quota (413) surface as `ClientError::TooLarge` / `QuotaExceeded`.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use bytes::Bytes;

use crate::api::{FileView, Parent};
use crate::error::{ClientError, Result};
use crate::seal::Seal;
use crate::session::Session;

const POLL: Duration = Duration::from_secs(1);
const POLL_TRIES: u32 = 60;

/// Outcome of [`Uploader::replace`].
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)] // one per upload
pub enum Replaced {
	/// The entry's content was swapped. Landed content id; `None` if the 60 s wait timed out
	/// (identical content can dedup back to the old id, so that is not an error).
	Landed(Option<String>),
	/// 412: the entry changed remotely. The staged content went to this new sibling; the entry
	/// itself is untouched.
	Conflict(FileView),
}

#[derive(Debug)]
pub struct Uploader {
	session: Arc<Session>,
	/// Upload preset: the pairing `profile`, default `standard`.
	profile: String,
	// never pruned (one small entry per written file until unmount).
	locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
	inflight: AtomicUsize,
	/// Staging files are sealed (temp mounts).
	seal: Option<Arc<Seal>>,
}

impl Uploader {
	pub fn new(session: Arc<Session>, seal: Option<Arc<Seal>>) -> Self {
		let profile = session
			.credentials()
			.key
			.profile
			.clone()
			.unwrap_or_else(|| "standard".to_owned());
		Self { session, profile, locks: Mutex::default(), inflight: AtomicUsize::new(0), seal }
	}

	pub fn seal(&self) -> Option<&Seal> {
		self.seal.as_deref()
	}

	/// The (unsealed) content of staging file `path`.
	fn body(&self, path: &Path) -> std::io::Result<Bytes> {
		let mut data = std::fs::read(path)?;
		if let Some(s) = &self.seal {
			s.apply(path, 0, &mut data)?;
		}
		Ok(Bytes::from(data))
	}

	/// Current content id of entry `id`, once any in-flight replace of it has landed. The
	/// `If-Match` base for an edit starting now.
	pub async fn base(&self, id: &str) -> Result<Option<String>> {
		let lock = self.lock_of(id);
		let _held = lock.lock().await;
		Ok(self
			.session
			.call(|api| async move { api.get_entry(id).await })
			.await?
			.content_id)
	}

	/// New BLOB `name` (remote form) under `parent` from the file at `path`.
	pub async fn create(&self, parent: &Parent, name: &str, path: &Path) -> Result<FileView> {
		let _busy = Busy::new(&self.inflight);
		self.post(parent, name, path).await
	}

	/// Replace entry `id`'s content with the file at `path`, `If-Match: base`. `parent` and
	/// `name` (remote form) place the conflict sibling and pick the content type.
	pub async fn replace(
		&self,
		id: &str,
		base: &str,
		parent: &Parent,
		name: &str,
		path: &Path,
	) -> Result<Replaced> {
		let _busy = Busy::new(&self.inflight);
		let lock = self.lock_of(id);
		let _held = lock.lock().await;
		// whole file in memory (also makes the 401 retry re-sendable); stream it if
		// big files matter.
		let body = self.body(path)?;
		let ct = crate::names::content_type(name);
		let res = self
			.session
			.call(|api| {
				let body = body.clone();
				async move { api.replace(id, base, None, ct, body).await }
			})
			.await;
		match res {
			Ok(p) => Ok(Replaced::Landed(self.wait(id, base, &p.pending_file_id).await)),
			Err(ClientError::PreconditionFailed) => {
				Ok(Replaced::Conflict(self.sibling(parent, name, path).await?))
			}
			Err(e) => Err(e),
		}
	}

	/// Upload the file at `path` as a new conflict sibling of `name` (remote form) in `parent`.
	pub(crate) async fn sibling(
		&self,
		parent: &Parent,
		name: &str,
		path: &Path,
	) -> Result<FileView> {
		let _busy = Busy::new(&self.inflight);
		let sibling =
			crate::conflict::conflict_name(name, &crate::conflict::device(), SystemTime::now());
		self.post(parent, &sibling, path).await
	}

	/// Resolves once no create/replace is in flight (call before unmounting).
	pub async fn idle(&self) {
		// 100 ms poll; a Notify if shutdown latency ever matters.
		while self.inflight.load(Ordering::SeqCst) > 0 {
			tokio::time::sleep(Duration::from_millis(100)).await;
		}
	}

	async fn post(&self, parent: &Parent, name: &str, path: &Path) -> Result<FileView> {
		let body = self.body(path)?;
		let (profile, ct) = (self.profile.as_str(), crate::names::content_type(name));
		let up = self
			.session
			.call(|api| {
				let body = body.clone();
				async move { api.upload_new(profile, parent, name, ct, body).await }
			})
			.await?;
		Ok(up.file)
	}

	/// Poll until the pending content landed: `contentId` is final (no `@`) and differs from
	/// `base` or equals `pending` without its `@`.
	pub(crate) async fn wait(&self, id: &str, base: &str, pending: &str) -> Option<String> {
		let want = pending.trim_start_matches('@');
		for _ in 0..POLL_TRIES {
			tokio::time::sleep(POLL).await;
			if let Ok(fv) = self.session.call(|api| async move { api.get_entry(id).await }).await
				&& let Some(c) = fv.content_id
				&& !c.starts_with('@')
				&& (c != base || c == want)
			{
				return Some(c);
			}
		}
		None
	}

	fn lock_of(&self, id: &str) -> Arc<tokio::sync::Mutex<()>> {
		let mut locks = crate::lock(&self.locks);
		Arc::clone(locks.entry(id.to_owned()).or_default())
	}
}

/// Counts an upload as in flight while alive.
struct Busy<'a>(&'a AtomicUsize);

impl<'a> Busy<'a> {
	fn new(count: &'a AtomicUsize) -> Self {
		count.fetch_add(1, Ordering::SeqCst);
		Self(count)
	}
}

impl Drop for Busy<'_> {
	fn drop(&mut self) {
		self.0.fetch_sub(1, Ordering::SeqCst);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn tmp_file(tag: &str) -> Result<(std::path::PathBuf, std::path::PathBuf)> {
		let dir = std::env::temp_dir().join(format!("cloudillo-{tag}-{}", std::process::id()));
		std::fs::create_dir_all(&dir)?;
		let src = dir.join("a.txt");
		std::fs::write(&src, b"new")?;
		Ok((dir, src))
	}

	#[tokio::test]
	async fn replace_landed_returns_final_content() -> Result<()> {
		let polls = AtomicUsize::new(0);
		let (session, log) = crate::stub::stub(move |l| {
			if l.starts_with("PUT ") {
				(202, r#"{"data":{"entryId":"E1","pendingFileId":"@f9"}}"#.into())
			} else if l.starts_with("GET /api/files/E1/metadata") {
				let c = if polls.fetch_add(1, Ordering::SeqCst) == 0 { "@f9" } else { "f9" };
				(200, format!(r#"{{"data":{{"id":"E1","contentId":"{c}"}}}}"#))
			} else {
				(500, String::new())
			}
		})
		.await;
		let (dir, src) = tmp_file("landed")?;
		let up = Uploader::new(session, None);
		let got = up.replace("E1", "c0", &Parent::Folder("P1".into()), "a.txt", &src).await?;
		assert!(matches!(&got, Replaced::Landed(Some(c)) if c == "f9"), "{got:?}");
		let log = crate::lock(&log).clone();
		assert!(log.iter().any(|h| h.starts_with("put ") && h.contains("if-match: \"c0\"")));
		assert!(tokio::time::timeout(Duration::ZERO, up.idle()).await.is_ok());
		std::fs::remove_dir_all(dir)?;
		Ok(())
	}

	#[tokio::test]
	async fn create_posts_under_parent() -> Result<()> {
		let (session, log) = crate::stub::stub(|l| {
			if l.starts_with("POST /api/uploads/standard") {
				(200, r#"{"data":{"id":"N1","contentId":"n1","fileName":"a.txt"}}"#.into())
			} else {
				(500, String::new())
			}
		})
		.await;
		let (dir, src) = tmp_file("create")?;
		let up = Uploader::new(session, None);
		let fv = up.create(&Parent::Folder("P1".into()), "a.txt", &src).await?;
		assert_eq!(fv.id, "N1");
		assert!(crate::lock(&log).iter().any(|h| h.contains("parentid=p1")));
		assert!(tokio::time::timeout(Duration::ZERO, up.idle()).await.is_ok());
		std::fs::remove_dir_all(dir)?;
		Ok(())
	}

	#[tokio::test]
	async fn replace_412_uploads_conflict_sibling() -> Result<()> {
		let (session, log) = crate::stub::stub(|l| {
			if l.starts_with("PUT ") {
				(412, String::new())
			} else if l.starts_with("POST /api/uploads/standard") {
				(200, r#"{"data":{"id":"S1","contentId":"s1","fileName":"x"}}"#.into())
			} else {
				(500, String::new())
			}
		})
		.await;
		let dir = std::env::temp_dir().join(format!("cloudillo-replace-{}", std::process::id()));
		std::fs::create_dir_all(&dir)?;
		let src = dir.join("a.txt");
		std::fs::write(&src, b"new")?;
		let up = Uploader::new(session, None);
		let got = up.replace("E1", "c0", &Parent::Folder("P1".into()), "a.txt", &src).await?;
		assert!(matches!(&got, Replaced::Conflict(fv) if fv.id == "S1"), "{got:?}");
		let log = crate::lock(&log).clone();
		assert!(log.iter().any(|h| h.starts_with("put ") && h.contains("if-match: \"c0\"")));
		assert!(log.iter().any(|h| h.starts_with("post ")
			&& h.contains("parentid=p1")
			&& h.contains("conflict")));
		assert!(!log.iter().any(|h| h.contains("/metadata")));
		std::fs::remove_dir_all(dir)?;
		Ok(())
	}
}
