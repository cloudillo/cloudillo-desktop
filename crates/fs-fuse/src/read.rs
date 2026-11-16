//! Reads: `off` mode reads ahead per open handle (one ranged download of at least 1 MiB serves
//! the reads after it); staged files and `on-demand` blobs are read locally with [`read_at`].

use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;

use cloudillo_desktop_core::{ClientError, Result, Session};
use tokio::sync::Mutex;

pub const READ_AHEAD: u64 = 1 << 20;

/// Up to `size` bytes of local file `path` from `off` (short only at EOF).
pub fn read_at(path: &Path, off: u64, size: u32) -> io::Result<Vec<u8>> {
	let f = File::open(path)?;
	let mut buf = vec![0; usize::try_from(size).unwrap_or(usize::MAX)];
	let mut n = 0;
	while let Some(rest) = buf.get_mut(n..).filter(|r| !r.is_empty()) {
		let k = f.read_at(rest, off + n as u64)?;
		if k == 0 {
			break;
		}
		n += k;
	}
	buf.truncate(n);
	Ok(buf)
}

#[derive(Debug)]
struct Buf {
	off: u64,
	data: Vec<u8>,
	/// The download ended before the requested length: nothing exists past `data`.
	eof: bool,
}

impl Buf {
	/// Bytes `[off, off+size)` if this buffer can answer for them.
	fn serve(&self, off: u64, size: u64) -> Option<Vec<u8>> {
		let end = self.off + self.data.len() as u64;
		if off < self.off || (off + size > end && !self.eof) {
			return None;
		}
		let len = self.data.len();
		let from = usize::try_from(off - self.off).unwrap_or(usize::MAX).min(len);
		let to = usize::try_from(off + size - self.off).unwrap_or(usize::MAX).min(len);
		self.data.get(from..to).map(<[u8]>::to_vec)
	}
}

/// Serialized per handle, so sequential readers trigger one download, not several.
#[derive(Debug, Default)]
pub struct ReadAhead(Mutex<Option<Buf>>);

impl ReadAhead {
	pub async fn read(&self, session: &Session, id: &str, off: u64, size: u32) -> Result<Vec<u8>> {
		let size = u64::from(size);
		let mut cur = self.0.lock().await;
		if let Some(hit) = cur.as_ref().and_then(|b| b.serve(off, size)) {
			return Ok(hit);
		}
		let want = size.max(READ_AHEAD);
		let data = fetch(session, id, off, want).await?;
		let buf = Buf { off, eof: (data.len() as u64) < want, data };
		let out = buf.serve(off, size).unwrap_or_default();
		*cur = Some(buf);
		Ok(out)
	}
}

/// Up to `len` bytes of entry `id` from `off`. Past EOF (416) is an empty read.
async fn fetch(session: &Session, id: &str, off: u64, len: u64) -> Result<Vec<u8>> {
	let res = session
		.call(|api| async move {
			let mut dl = api.download(id, Some((off, off + len - 1))).await?;
			// a 200 (Range ignored) streams from byte 0 and we discard up to `off`;
			// fine for small files, costly deep into big ones.
			let mut skip = if dl.partial { 0 } else { off };
			let mut out = Vec::new();
			while let Some(chunk) = dl.chunk().await? {
				let mut c = &chunk[..];
				if skip > 0 {
					let n = usize::try_from(skip).unwrap_or(usize::MAX).min(c.len());
					c = c.get(n..).unwrap_or_default();
					skip -= n as u64;
				}
				let room = usize::try_from(len - out.len() as u64).unwrap_or(usize::MAX);
				out.extend_from_slice(c.get(..room.min(c.len())).unwrap_or_default());
				if out.len() as u64 >= len {
					break;
				}
			}
			Ok(out)
		})
		.await;
	match res {
		Err(ClientError::Http { status: 416, .. }) => Ok(Vec::new()),
		r => r,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn serves_from_buffer() {
		let b = Buf { off: 10, data: (0..100).collect(), eof: false };
		assert_eq!(b.serve(10, 3), Some(vec![0, 1, 2]));
		assert_eq!(b.serve(105, 5), Some(vec![95, 96, 97, 98, 99]));
		assert_eq!(b.serve(105, 6), None);
		assert_eq!(b.serve(9, 1), None);
		let e = Buf { eof: true, ..b };
		assert_eq!(e.serve(108, 10), Some(vec![98, 99]));
		assert_eq!(e.serve(200, 10), Some(vec![]));
	}
}
