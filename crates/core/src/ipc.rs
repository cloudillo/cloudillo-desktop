//! CLI <-> daemon protocol: one JSON object per line over the Unix socket
//! `<run_dir>/daemon.sock` (mode `0600`). One request, one response.

use std::io;
use std::path::PathBuf;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Request {
	/// New persistent mount; `mountpoint` absolute, `mode` `off` | `on-demand` | `pinned`.
	Mount {
		account: String,
		mountpoint: PathBuf,
		mode: String,
	},
	/// `target` = mount id or mountpoint. Refused while uploads are in flight unless `discard`.
	Unmount {
		target: String,
		discard: bool,
	},
	/// One line per mount; its state may add "re-pair needed", "N unsynced (F failed)",
	/// "N not synced: too large", "N quota exceeded", "N ignored (kept local)".
	Status,
	/// Stop / restart change-feed polling of mount `id`.
	Pause {
		id: String,
	},
	Resume {
		id: String,
	},
	/// Keep the file or folder at `path` (absolute, in an `on-demand` mount) hydrated.
	Pin {
		path: PathBuf,
	},
	/// Clear the pin of `path` and of everything under it.
	Unpin {
		path: PathBuf,
	},
}

/// Human-readable text on success, an error message otherwise.
/// Wire form: `{"Ok":"…"}` / `{"Err":"…"}`.
pub type Response = Result<String, String>;

/// `$XDG_RUNTIME_DIR/cloudillo`, else `~/.cache/cloudillo/run` (no logind session, e.g. `su`).
pub fn run_dir() -> Option<PathBuf> {
	dirs::runtime_dir()
		.map(|d| d.join("cloudillo"))
		.or_else(|| dirs::cache_dir().map(|d| d.join("cloudillo").join("run")))
}

/// Socket path, under [`run_dir`]. `None` without a runtime or cache directory.
pub fn socket_path() -> Option<PathBuf> {
	run_dir().map(|d| d.join("daemon.sock"))
}

/// One frame: the JSON of `msg` plus `\n`.
pub fn encode<T: Serialize>(msg: &T) -> io::Result<Vec<u8>> {
	let mut v = serde_json::to_vec(msg).map_err(io::Error::other)?;
	v.push(b'\n');
	Ok(v)
}

/// Parse one line read from the socket (trailing newline allowed).
pub fn decode<T: DeserializeOwned>(line: &str) -> io::Result<T> {
	serde_json::from_str(line.trim_end()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn frames_round_trip() {
		let req = Request::Pin { path: "/m/a.txt".into() };
		let frame = encode(&req).unwrap_or_default();
		assert_eq!(frame.last(), Some(&b'\n'));
		let back: Option<Request> = std::str::from_utf8(&frame).ok().and_then(|s| decode(s).ok());
		assert_eq!(back, Some(req));
		let resp: Response = Err("read-only mount".into());
		let line = encode(&resp).map(|v| String::from_utf8_lossy(&v).into_owned());
		assert_eq!(line.ok().as_deref(), Some("{\"Err\":\"read-only mount\"}\n"));
	}
}
