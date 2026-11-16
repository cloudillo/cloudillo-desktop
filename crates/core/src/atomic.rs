//! Atomic-save detection. Editors save via temp file + rename, or unlink + create; a local
//! unlink or rename-away of a tracked file is therefore held [`HOLD`] keyed by its
//! `(parent, local name)`. A new file appearing there within the window becomes a `Replace` of
//! the original entry (keeping its id, shares and history) and the held op is dropped;
//! otherwise the held op is journaled when the window ends. Content is journaled only after
//! [`QUIET`] without writes following the last close, so a rapid save sequence uploads once.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// How long an unlink / rename-away of a tracked file waits for its replacement.
pub const HOLD: Duration = Duration::from_secs(5);
/// Write-quiet after the last close before a Create / Replace is journaled.
pub const QUIET: Duration = Duration::from_secs(2);

/// A held op, claimed exactly once: by the replacing file, or by the timer journaling it.
#[derive(Debug, Clone, Default)]
pub struct Hold(Arc<AtomicBool>);

impl Hold {
	/// `true` for the first caller only.
	pub fn claim(&self) -> bool {
		!self.0.swap(true, Ordering::AcqRel)
	}

	pub fn claimed(&self) -> bool {
		self.0.load(Ordering::Acquire)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn claimed_once() {
		let h = Hold::default();
		let timer = h.clone();
		assert!(!h.claimed());
		assert!(h.claim());
		assert!(!timer.claim());
		assert!(timer.claimed());
	}
}
