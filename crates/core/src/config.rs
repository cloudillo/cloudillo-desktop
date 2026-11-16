//! Persistent mounts: `$XDG_CONFIG_HOME/cloudillo/mounts.toml`, owned by the daemon.

use std::io;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::error::{ClientError, Result};

/// One persistent mount. `id` names its cache dir `$XDG_CACHE_HOME/cloudillo/<id>/`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MountEntry {
	pub id: String,
	/// Keyring account (idTag) of the paired key.
	pub account: String,
	pub mountpoint: PathBuf,
	/// `off` | `on-demand` | `pinned`
	pub mode: String,
	#[serde(default)]
	pub paused: bool,
	/// Extra ignore patterns (on top of `ignore::BUILTIN`); matching files stay local.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub ignore: Vec<String>,
	/// Larger files stay local [default: `ignore::DEFAULT_MAX_FILE_MB`].
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_file_size_mb: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
	/// Blob cache limit per mount [default: `cache::DEFAULT_MAX_MB`].
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cache_max_mb: Option<u64>,
	#[serde(default)]
	pub mounts: Vec<MountEntry>,
}

impl Config {
	pub fn path() -> Result<PathBuf> {
		let dir = dirs::config_dir()
			.ok_or_else(|| io::Error::other("no config dir ($XDG_CONFIG_HOME / $HOME unset)"))?;
		Ok(dir.join("cloudillo").join("mounts.toml"))
	}

	/// The config file; a missing file is an empty config.
	pub fn load() -> Result<Self> {
		match std::fs::read_to_string(Self::path()?) {
			Ok(s) => Self::parse(&s),
			Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Self::default()),
			Err(e) => Err(e.into()),
		}
	}

	pub fn parse(s: &str) -> Result<Self> {
		toml::from_str(s).map_err(|e| ClientError::Decode(format!("mounts.toml: {e}")))
	}

	/// Write via a temp file + rename, so a crash never leaves a half-written config.
	pub fn save(&self) -> Result<()> {
		let path = Self::path()?;
		if let Some(dir) = path.parent() {
			std::fs::create_dir_all(dir)?;
		}
		let s = toml::to_string(self).map_err(|e| ClientError::Decode(e.to_string()))?;
		let tmp = path.with_extension("toml.tmp");
		std::fs::write(&tmp, s)?;
		std::fs::rename(tmp, path)?;
		Ok(())
	}

	/// Smallest numeric id neither mounted nor `taken` (e.g. a cache dir kept for its journal).
	pub fn next_id(&self, taken: impl Fn(&str) -> bool) -> String {
		(1..=u64::MAX)
			.map(|n| n.to_string())
			.find(|id| !taken(id) && self.mounts.iter().all(|m| &m.id != id))
			.unwrap_or_default()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn round_trips_and_numbers_ids() {
		let mut cfg = Config::parse("").unwrap_or_default();
		assert_eq!(cfg.next_id(|_| false), "1");
		cfg.mounts.push(MountEntry {
			id: "1".into(),
			account: "alice.example.com".into(),
			mountpoint: "/home/a/Cloudillo".into(),
			mode: "on-demand".into(),
			paused: false,
			ignore: vec!["*.part".into()],
			max_file_size_mb: Some(200),
		});
		cfg.cache_max_mb = Some(512);
		let back = toml::to_string(&cfg)
			.map_err(|e| e.to_string())
			.and_then(|s| Config::parse(&s).map_err(|e| e.to_string()));
		assert_eq!(back.ok(), Some(cfg.clone()));
		assert_eq!(cfg.next_id(|_| false), "2");
		assert_eq!(cfg.next_id(|id| id == "2"), "3");
	}
}
