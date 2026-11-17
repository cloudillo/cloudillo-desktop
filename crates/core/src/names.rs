// SPDX-FileCopyrightText: Szilárd Hajba
// SPDX-License-Identifier: MPL-2.0

//! Remote name <-> local name mapping. [`assign_names`] is stateless (`off` mode);
//! [`assign_sticky`] keeps the names already stored in the metadata DB.

use std::collections::HashSet;
use std::time::SystemTime;

use crate::db::Db;
use crate::error::Result;

/// Full-width solidus for `/`.
const SLASH: char = '\u{FF0F}';
/// Symbol for null for NUL.
const NUL: char = '\u{2400}';
/// Full-width full stop(s) for the names `.` and `..`.
const DOT: &str = "\u{FF0E}";
const DOTDOT: &str = "\u{FF0E}\u{FF0E}";

/// Linux `NAME_MAX`, in bytes.
const NAME_MAX: usize = 255;

/// Remote name -> a name legal on Linux (at most [`NAME_MAX`] bytes, never empty).
// one-way; the DB keeps `remote_name`. Local names go to the server as they are
// (they can never hold `/` or NUL).
pub fn encode_name(remote: &str) -> String {
	match remote {
		"" => "_".to_owned(),
		"." => DOT.to_owned(),
		".." => DOTDOT.to_owned(),
		_ => {
			let mapped: String = remote
				.chars()
				.map(|c| match c {
					'/' => SLASH,
					'\0' => NUL,
					c => c,
				})
				.collect();
			fit(&mapped, "")
		}
	}
}

/// `stem + tag + ext` of `name`, the stem cut (on a char boundary) to fit [`NAME_MAX`].
fn fit(name: &str, tag: &str) -> String {
	let (mut stem, mut ext) = split_ext(name);
	if ext.len() + tag.len() > NAME_MAX / 2 {
		(stem, ext) = (name, "");
	}
	let mut cut = NAME_MAX.saturating_sub(tag.len() + ext.len()).min(stem.len());
	while !stem.is_char_boundary(cut) {
		cut -= 1;
	}
	format!("{}{tag}{ext}", &stem[..cut])
}

/// Unique local names for one folder's children, `(entry_id, name, created_at)` in, same order
/// out. Oldest (then lowest id) keeps the plain name; the rest get `name (2).ext`, `(3)`, … by
/// age, skipping any name a sibling already has. `created_at = None` sorts first (virtual room
/// folders use it so they always keep their name).
pub fn assign_names(siblings: &[(&str, &str, Option<SystemTime>)]) -> Vec<(String, String)> {
	assign_with(siblings, HashSet::new())
}

/// [`assign_names`] for rows new to `parent_id` of `drive` (or renamed / moved into it): the
/// names live rows already hold there are taken, so existing rows keep theirs (sticky).
/// Callers pass only rows whose stored name is not kept.
pub fn assign_sticky(
	db: &Db,
	parent_id: &str,
	drive: Option<&str>,
	new_rows: &[(&str, &str, Option<SystemTime>)],
) -> Result<Vec<(String, String)>> {
	let held = db.children(parent_id, drive)?.into_iter().map(|r| r.local_name).collect();
	Ok(assign_with(new_rows, held))
}

/// The naming rule with `held` names already in use by siblings outside `siblings`.
fn assign_with(
	siblings: &[(&str, &str, Option<SystemTime>)],
	held: HashSet<String>,
) -> Vec<(String, String)> {
	let mut order: Vec<usize> = (0..siblings.len()).collect();
	order.sort_by(|&a, &b| {
		let (ia, _, ta) = siblings[a];
		let (ib, _, tb) = siblings[b];
		ta.cmp(&tb).then_with(|| ia.cmp(ib))
	});
	let mut taken: HashSet<String> =
		siblings.iter().map(|s| s.1.to_owned()).chain(held.iter().cloned()).collect();
	let mut claimed = held;
	let mut out = vec![String::new(); siblings.len()];
	for i in order {
		let name = siblings[i].1;
		if claimed.insert(name.to_owned()) {
			name.clone_into(&mut out[i]);
			continue;
		}
		let mut n = 2u32;
		let local = loop {
			let candidate = fit(name, &format!(" ({n})"));
			if !taken.contains(&candidate) {
				break candidate;
			}
			n = n.saturating_add(1);
		};
		taken.insert(local.clone());
		out[i] = local;
	}
	siblings.iter().zip(out).map(|(s, local)| (s.0.to_owned(), local)).collect()
}

/// `a.b.txt` -> (`a.b`, `.txt`); dotfiles and extensionless names have no extension.
pub(crate) fn split_ext(name: &str) -> (&str, &str) {
	match name.rfind('.') {
		Some(i) if i > 0 => name.split_at(i),
		_ => (name, ""),
	}
}

/// Content type by extension; `None` lets the server default to `application/octet-stream`.
pub(crate) fn content_type(name: &str) -> Option<&'static str> {
	let ext = split_ext(name).1.get(1..)?.to_ascii_lowercase();
	Some(match ext.as_str() {
		"txt" => "text/plain",
		"md" => "text/markdown",
		"html" | "htm" => "text/html",
		"css" => "text/css",
		"csv" => "text/csv",
		"json" => "application/json",
		"pdf" => "application/pdf",
		"zip" => "application/zip",
		"png" => "image/png",
		"jpg" | "jpeg" => "image/jpeg",
		"gif" => "image/gif",
		"webp" => "image/webp",
		"svg" => "image/svg+xml",
		"mp3" => "audio/mpeg",
		"mp4" => "video/mp4",
		"webm" => "video/webm",
		_ => return None,
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::time::{Duration, UNIX_EPOCH};

	#[allow(clippy::unnecessary_wraps)] // matches assign_names input
	fn t(s: u64) -> Option<SystemTime> {
		Some(UNIX_EPOCH + Duration::from_secs(s))
	}

	#[test]
	fn encodes() {
		for (remote, local) in [
			("a/b", "a\u{FF0F}b"),
			("a\0b", "a\u{2400}b"),
			(".", "\u{FF0E}"),
			("..", "\u{FF0E}\u{FF0E}"),
			("...", "..."),
			("plain.txt", "plain.txt"),
		] {
			assert_eq!(encode_name(remote), local);
		}
		assert_eq!(encode_name(""), "_");
	}

	#[test]
	fn names_fit_name_max() {
		let long = format!("{}.txt", "é".repeat(300));
		let enc = encode_name(&long);
		assert!(enc.len() <= NAME_MAX && split_ext(&enc).1 == ".txt", "{}", enc.len());
		let out = assign_names(&[("a", &enc, t(1)), ("b", &enc, t(2))]);
		assert_eq!(out[0].1, enc);
		assert!(out[1].1.len() <= NAME_MAX && out[1].1.ends_with(" (2).txt"), "{}", out[1].1);
	}

	#[test]
	fn duplicates_get_suffix_by_age() {
		let out = assign_names(&[
			("c", "a.txt", t(30)),
			("a", "a.txt", t(10)),
			("b", "a.txt", t(20)),
			("d", "x", t(1)),
		]);
		let names: Vec<&str> = out.iter().map(|(_, n)| n.as_str()).collect();
		assert_eq!(names, ["a (3).txt", "a.txt", "a (2).txt", "x"]);
		assert_eq!(out[0].0, "c");
	}

	#[test]
	fn ties_break_by_id_and_literal_names_are_skipped() {
		let out = assign_names(&[
			("z", "f", t(5)),
			("y", "f", t(5)),
			("w", "f (2)", t(9)),
			("v", ".rc", t(1)),
			("u", ".rc", t(2)),
		]);
		let names: Vec<&str> = out.iter().map(|(_, n)| n.as_str()).collect();
		assert_eq!(names, ["f (3)", "f", "f (2)", ".rc", ".rc (2)"]);
	}

	#[test]
	fn sticky_names_keep_existing() -> Result<()> {
		let db = Db::open_in_memory()?;
		db.upsert(&crate::db::EntryRow {
			ino: 0,
			entry_id: "old".into(),
			parent_id: "p".into(),
			drive: None,
			remote_name: "a.txt".into(),
			local_name: "a.txt".into(),
			kind: crate::db::EntryKind::File,
			content_id: None,
			size: 0,
			mtime: UNIX_EPOCH,
			created_at: String::new(),
			updated_at: 1,
			status: crate::db::LIVE.into(),
			hydrated: false,
		})?;
		// older than the stored row, still suffixed: the stored name is sticky
		let out = assign_sticky(&db, "p", None, &[("new", "a.txt", t(0)), ("b", "b", t(5))])?;
		assert_eq!(out, [("new".into(), "a (2).txt".into()), ("b".into(), "b".into())]);
		// another folder is unaffected
		assert_eq!(assign_sticky(&db, "q", None, &[("n", "a.txt", t(0))])?[0].1, "a.txt");
		Ok(())
	}

	#[test]
	fn virtual_names_win() {
		let out = assign_names(&[("e1", "~team", t(1)), ("#room:@t~team", "~team", None)]);
		assert_eq!(out[0].1, "~team (2)");
		assert_eq!(out[1].1, "~team");
	}

	#[test]
	fn guesses_content_type() {
		assert_eq!(content_type("a.PNG"), Some("image/png"));
		assert_eq!(content_type("notes"), None);
		assert_eq!(content_type(".bashrc"), None);
	}
}
