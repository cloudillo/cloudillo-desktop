//! Files never uploaded: ignore patterns (built-in + per mount) and the size cap. Such files
//! stay local (in the mount's staging dir) and are counted for `status`.

use std::collections::HashMap;
use std::sync::Mutex;

/// Editor / OS litter, matched against the local file name.
pub const BUILTIN: &[&str] = &[
	"~$*",
	"~*.tmp",
	".*.swp",
	"*~",
	".~lock.*#",
	"Thumbs.db",
	"desktop.ini",
	".DS_Store",
];

/// Size cap when `mounts.toml` sets no `max_file_size_mb`.
pub const DEFAULT_MAX_FILE_MB: u64 = 50;

/// Why a file is kept local.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kept {
	Ignored,
	TooLarge,
}

/// Per-mount upload policy.
#[derive(Debug)]
pub struct Policy {
	patterns: Vec<String>,
	pub max_bytes: u64,
	/// Files currently kept local, by frontend key (inode).
	kept: Mutex<HashMap<u64, Kept>>,
}

impl Default for Policy {
	fn default() -> Self {
		Self::new(&[], None)
	}
}

impl Policy {
	/// Built-in patterns plus `extra`; cap `max_file_size_mb` [default: [`DEFAULT_MAX_FILE_MB`]].
	pub fn new(extra: &[String], max_file_size_mb: Option<u64>) -> Self {
		let patterns = BUILTIN.iter().map(|&p| p.to_owned()).chain(extra.iter().cloned()).collect();
		let mb = max_file_size_mb.unwrap_or(DEFAULT_MAX_FILE_MB);
		Self { patterns, max_bytes: mb.saturating_mul(1024 * 1024), kept: Mutex::default() }
	}

	pub fn ignored(&self, name: &str) -> bool {
		self.patterns.iter().any(|p| glob(p, name))
	}

	/// Record (`Some`) or forget (`None`) a file kept local.
	pub fn mark(&self, key: u64, kept: Option<Kept>) {
		let mut m = crate::lock(&self.kept);
		match kept {
			Some(k) => m.insert(key, k),
			None => m.remove(&key),
		};
	}

	/// `(ignored, too large)` files kept local.
	pub fn kept(&self) -> (usize, usize) {
		let m = crate::lock(&self.kept);
		let ignored = m.values().filter(|&&k| k == Kept::Ignored).count();
		(ignored, m.len() - ignored)
	}
}

/// Shell-style match of the whole `name`: `*` = any run, `?` = one char, all else literal.
// no `[...]` classes (globset is not a dependency); add globset if patterns need them
pub fn glob(pattern: &str, name: &str) -> bool {
	let (p, n): (Vec<char>, Vec<char>) = (pattern.chars().collect(), name.chars().collect());
	let (mut i, mut j) = (0, 0);
	// last `*` seen in the pattern, and the name position it currently absorbs up to
	let mut star: Option<(usize, usize)> = None;
	while j < n.len() {
		if i < p.len() && (p[i] == '?' || (p[i] != '*' && p[i] == n[j])) {
			i += 1;
			j += 1;
		} else if i < p.len() && p[i] == '*' {
			star = Some((i, j));
			i += 1;
		} else if let Some((si, sj)) = star {
			star = Some((si, sj + 1));
			i = si + 1;
			j = sj + 1;
		} else {
			return false;
		}
	}
	p[i..].iter().all(|&c| c == '*')
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn matches_builtins() {
		let p = Policy::new(&["*.part".to_owned()], Some(1));
		for name in [
			"~$report.docx",
			"~WRL0001.tmp",
			".notes.txt.swp",
			"notes.txt~",
			".~lock.report.odt#",
			"Thumbs.db",
			"desktop.ini",
			".DS_Store",
			"movie.part",
		] {
			assert!(p.ignored(name), "{name}");
		}
		for name in ["report.docx", "notes.txt", "a~b", "swp", ".swp.txt", "Thumbs.dbx", "4913"] {
			assert!(!p.ignored(name), "{name}");
		}
		assert_eq!(p.max_bytes, 1024 * 1024);
		assert!(glob("a?c*", "abcdef") && glob("*", "") && !glob("a*c", "abd"));
	}

	#[test]
	fn counts_kept() {
		let p = Policy::default();
		p.mark(1, Some(Kept::Ignored));
		p.mark(2, Some(Kept::TooLarge));
		p.mark(2, Some(Kept::TooLarge));
		assert_eq!(p.kept(), (1, 1));
		p.mark(1, None);
		assert_eq!(p.kept(), (0, 1));
	}
}
