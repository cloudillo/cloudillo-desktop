// SPDX-FileCopyrightText: Szilárd Hajba
// SPDX-License-Identifier: MPL-2.0

//! Conflict table: what happens when an entry changed both locally (pending journal ops) and
//! remotely (a feed row, a `gone` marker, a 412) since the local base (`base_content_id` /
//! last feed state). [`decide`] is pure; the feed poller and the sync worker only execute its
//! output. No branch drops local content: `Modified` / `Created` / `MovedInto` always upload.

use std::time::{SystemTime, UNIX_EPOCH};

/// `parentId` of an entry in the trash (the server's `TRASH_PARENT_ID`).
pub const TRASH_PARENT: &str = "__trash__";

/// The local side: what the journal still holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalChange {
	/// A pending `Replace` of the entry.
	Modified,
	/// A pending `Trash` of the entry.
	Trashed,
	/// A pending `Rename` / `Move` of the entry.
	Renamed,
	/// Pending ops put entries into this folder (`Move`, `Create`, `Mkdir`, or a `Replace` of
	/// a child).
	MovedInto,
	/// A pending `Create` whose name a new remote sibling took.
	Created,
}

/// The remote side, against the local base.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteChange {
	/// Other content (a 412, or a feed row with another content id).
	Modified,
	/// In the trash, or gone from the feed (it cannot tell trash from purge).
	Trashed,
	/// Other name or parent, same content.
	Renamed,
	/// A new entry with the name of a pending local create.
	Created,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
	/// Replay the pending local op as journaled.
	Upload,
	/// Upload the local content as a new sibling named by [`conflict_name`] instead.
	UploadAsConflictSibling,
	/// Restore the entry from the trash into its known parent, before the upload.
	Restore,
	/// Restore the trashed folder (with its entries) the pending ops put entries into.
	RestoreFolder,
	/// Adopt the remote state locally (a trashed entry leaves the local view); fields a
	/// still-pending local op changes keep their local value.
	Pull,
	/// Drop the pending local ops of this change (only ever a `Trash` or `Rename` / `Move`).
	DropLocal,
}

/// The conflict table.
pub fn decide(local: LocalChange, remote: RemoteChange) -> Vec<Action> {
	use Action::{DropLocal, Pull, Restore, RestoreFolder, Upload, UploadAsConflictSibling};
	use LocalChange as L;
	use RemoteChange as R;
	match (local, remote) {
		(L::Modified, R::Modified) | (L::Created, R::Created) => {
			vec![UploadAsConflictSibling, Pull]
		}
		(L::Modified, R::Trashed) => vec![Restore, Upload],
		(L::Trashed, R::Modified | R::Trashed) | (L::Renamed, R::Renamed | R::Trashed) => {
			vec![DropLocal, Pull]
		}
		(L::Trashed, R::Renamed) => vec![Upload],
		(L::MovedInto, R::Trashed) => vec![RestoreFolder, Upload],
		// modified vs renamed, and everything outside the table: both sides apply
		_ => vec![Upload, Pull],
	}
}

/// `a.txt` -> `a (conflict <device> YYYY-MM-DD HHMM).txt`.
// UTC (std has no local timezone); use the local zone if users find it confusing.
pub fn conflict_name(name: &str, device: &str, now: SystemTime) -> String {
	let secs = now.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
	let (year, month, day) = civil_from_days(i64::try_from(secs / 86_400).unwrap_or(0));
	let (hour, min) = (secs % 86_400 / 3600, secs % 3600 / 60);
	let (stem, ext) = crate::names::split_ext(name);
	format!("{stem} (conflict {device} {year:04}-{month:02}-{day:02} {hour:02}{min:02}){ext}")
}

/// The conflict sibling's `<device>`: the hostname, which is also what `cloudillo login` names
/// the device by (the pairing does not store a device name).
pub fn device() -> String {
	std::fs::read_to_string("/proc/sys/kernel/hostname")
		.map(|s| s.trim().to_owned())
		.ok()
		.filter(|s| !s.is_empty())
		.unwrap_or_else(|| "cloudillo-desktop".to_owned())
}

/// Days since 1970-01-01 -> (year, month, day) (Howard Hinnant's `civil_from_days`).
fn civil_from_days(z: i64) -> (i64, i64, i64) {
	let z = z + 719_468;
	let era = z.div_euclid(146_097);
	let doe = z - era * 146_097;
	let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
	let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
	let mp = (5 * doy + 2) / 153;
	let d = doy - (153 * mp + 2) / 5 + 1;
	let m = if mp < 10 { mp + 3 } else { mp - 9 };
	let y = yoe + era * 400;
	(if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use super::Action::{DropLocal, Pull, Restore, RestoreFolder, Upload, UploadAsConflictSibling};
	use super::LocalChange as L;
	use super::RemoteChange as R;
	use super::*;

	#[test]
	fn modified_vs_modified_uploads_conflict_sibling() {
		assert_eq!(decide(L::Modified, R::Modified), [UploadAsConflictSibling, Pull]);
	}

	#[test]
	fn modified_vs_trashed_restores_and_replaces() {
		assert_eq!(decide(L::Modified, R::Trashed), [Restore, Upload]);
	}

	#[test]
	fn trashed_vs_modified_drops_trash_and_pulls() {
		assert_eq!(decide(L::Trashed, R::Modified), [DropLocal, Pull]);
	}

	#[test]
	fn renamed_vs_renamed_remote_wins() {
		assert_eq!(decide(L::Renamed, R::Renamed), [DropLocal, Pull]);
	}

	#[test]
	fn modified_vs_renamed_both_apply() {
		assert_eq!(decide(L::Modified, R::Renamed), [Upload, Pull]);
	}

	#[test]
	fn trashed_vs_renamed_trash_applies() {
		assert_eq!(decide(L::Trashed, R::Renamed), [Upload]);
	}

	#[test]
	fn renamed_vs_trashed_remote_wins() {
		assert_eq!(decide(L::Renamed, R::Trashed), [DropLocal, Pull]);
	}

	#[test]
	fn moved_into_trashed_folder_restores_it() {
		assert_eq!(decide(L::MovedInto, R::Trashed), [RestoreFolder, Upload]);
	}

	#[test]
	fn created_same_name_keeps_both() {
		assert_eq!(decide(L::Created, R::Created), [UploadAsConflictSibling, Pull]);
	}

	#[test]
	fn never_drops_local_content() {
		let remotes = [R::Modified, R::Trashed, R::Renamed, R::Created];
		for local in [L::Modified, L::Created, L::MovedInto] {
			for remote in remotes {
				let a = decide(local, remote);
				assert!(!a.contains(&DropLocal), "{local:?} vs {remote:?}");
				assert!(
					a.contains(&Upload) || a.contains(&UploadAsConflictSibling),
					"{local:?} vs {remote:?}"
				);
			}
		}
	}

	#[test]
	fn names_conflict_sibling() {
		// 2024-02-29T11:00:00Z
		let t = UNIX_EPOCH + Duration::from_secs(1_709_204_400);
		assert_eq!(
			conflict_name("report.pdf", "box", t),
			"report (conflict box 2024-02-29 1100).pdf"
		);
		assert_eq!(conflict_name(".bashrc", "box", t), ".bashrc (conflict box 2024-02-29 1100)");
		assert_eq!(civil_from_days(0), (1970, 1, 1));
		assert_eq!(civil_from_days(-1), (1969, 12, 31));
	}
}
