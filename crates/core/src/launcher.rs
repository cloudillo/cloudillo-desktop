//! `.cloudillo-doc` launcher files: CRDT/RTDB entries show up as small read-only JSON files.

/// Suffix appended to the (encoded) remote name of a CRDT/RTDB entry.
pub const SUFFIX: &str = ".cloudillo-doc";

/// Local base name of a launcher; `encoded` is the already name-encoded remote file name.
pub fn name(encoded: &str) -> String {
	format!("{encoded}{SUFFIX}")
}

/// Launcher file content: `{"host", "fileId": <entry id>, "app"}`.
pub fn json(host: &str, entry_id: &str, app: &str) -> Vec<u8> {
	serde_json::to_vec(&serde_json::json!({ "host": host, "fileId": entry_id, "app": app }))
		.unwrap_or_default()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn launcher_content() {
		assert_eq!(name("Notes"), "Notes.cloudillo-doc");
		let v: serde_json::Value =
			serde_json::from_slice(&json("https://a.example", "e1", "CRDT")).unwrap_or_default();
		assert_eq!(v["host"], "https://a.example");
		assert_eq!(v["fileId"], "e1");
		assert_eq!(v["app"], "CRDT");
	}
}
