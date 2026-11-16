//! Staging-file encryption for temp mounts: a ChaCha20 keystream XORed in at the file offset
//! (random access, ciphertext length = plaintext length). The key is random and never leaves
//! the process; the threat is staging data at rest after a crash or disk theft, so no auth tag.

use std::fmt;
use std::fs::File;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;

use chacha20::ChaCha20;
use chacha20::cipher::{KeyIvInit, StreamCipher, StreamCipherSeek};

pub struct Seal {
	key: [u8; 32],
}

impl fmt::Debug for Seal {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("Seal(<redacted>)")
	}
}

impl Seal {
	pub fn random() -> io::Result<Self> {
		let mut key = [0; 32];
		getrandom::fill(&mut key).map_err(io::Error::other)?;
		Ok(Self { key })
	}

	/// XOR the keystream of file `path` at `off` into `buf` (encrypt == decrypt).
	pub fn apply(&self, path: &Path, off: u64, buf: &mut [u8]) -> io::Result<()> {
		// Staging names are unique per process, so per-file keystreams differ.
		// CTR, rewriting the same offset reuses keystream (old^new leaks to someone who
		// saw both on disk); XTS if that threat matters.
		let mut h = DefaultHasher::new();
		path.hash(&mut h);
		let mut nonce = [0; 12];
		nonce[..8].copy_from_slice(&h.finish().to_le_bytes());
		let mut c = ChaCha20::new(&self.key.into(), &nonce.into());
		c.try_seek(off).map_err(io::Error::other)?;
		c.try_apply_keystream(buf).map_err(io::Error::other)
	}

	/// Extend `f` from `from` to `to` with sealed zeros, so holes decrypt to 0.
	pub fn zero_fill(&self, f: &File, path: &Path, from: u64, to: u64) -> io::Result<()> {
		const CHUNK: u64 = 64 * 1024;
		let mut off = from;
		while off < to {
			let n = usize::try_from((to - off).min(CHUNK)).unwrap_or(0);
			let mut buf = vec![0; n];
			self.apply(path, off, &mut buf)?;
			f.write_all_at(&buf, off)?;
			off += CHUNK;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn roundtrip_and_zero_fill() -> io::Result<()> {
		let dir = std::env::temp_dir().join(format!("cloudillo-seal-{}", std::process::id()));
		std::fs::create_dir_all(&dir)?;
		let path = dir.join("f");
		let f = File::create(&path)?;
		let seal = Seal::random()?;
		let plain = b"hello, sealed staging world".to_vec();

		seal.zero_fill(&f, &path, 0, 10)?;
		let mut buf = plain.clone();
		seal.apply(&path, 10, &mut buf)?;
		f.write_all_at(&buf, 10)?;

		let mut disk = std::fs::read(&path)?;
		assert_eq!(disk.len(), 10 + plain.len());
		assert_ne!(&disk[10..], &plain[..]);
		seal.apply(&path, 0, &mut disk)?;
		assert_eq!(&disk[..10], &[0; 10]);
		assert_eq!(&disk[10..], &plain[..]);
		std::fs::remove_dir_all(&dir)
	}
}
