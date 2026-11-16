//! Paired-device credentials: OS keyring (service `cloudillo`, account `{idTag}`), with a
//! `0600` JSON file under `$XDG_CONFIG_HOME/cloudillo/credentials/` when no keyring is reachable.

use std::fs;
use std::io::{self, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::PathBuf;

use crate::api::PairResult;
use crate::error::{ClientError, Result};

const SERVICE: &str = "cloudillo";

/// What `pair` yields: the approved key. The idTag names both the user and the personal host
/// (`https://cl-o.{idTag}`). Debug redacts the key (via [`PairResult`]'s Debug).
#[derive(Debug, Clone)]
pub struct Credentials {
	/// Stored as the keyring value: `{apiKey, idTag, scope, contextIdTag, target, access,
	/// profile, expiresAt}`.
	pub key: PairResult,
}

impl Credentials {
	/// Keyring account: the idTag.
	pub fn account(&self) -> &str {
		&self.key.id_tag
	}
}

fn keyring_err(e: &keyring::Error) -> ClientError {
	ClientError::Io(io::Error::other(format!("keyring: {e}")))
}

/// No keyring service at all (no Secret Service on the bus, locked/denied store).
fn unavailable(e: &keyring::Error) -> bool {
	matches!(e, keyring::Error::PlatformFailure(_) | keyring::Error::NoStorageAccess(_))
}

fn entry(account: &str) -> std::result::Result<keyring::Entry, keyring::Error> {
	keyring::Entry::new(SERVICE, account)
}

fn fallback_path(account: &str) -> Result<PathBuf> {
	let dir = dirs::config_dir()
		.ok_or_else(|| io::Error::other("no config dir ($XDG_CONFIG_HOME / $HOME unset)"))?;
	// keep the account a single path component
	let name = account.replace(['/', '\\'], "_");
	Ok(dir.join("cloudillo").join("credentials").join(format!("{name}.json")))
}

/// Store under [`Credentials::account`]. Falls back to the `0600` file (with a stderr warning)
/// only when the keyring is unavailable.
pub fn store(creds: &Credentials) -> Result<()> {
	let account = creds.account();
	let json = serde_json::to_string(&creds.key).map_err(|e| ClientError::Decode(e.to_string()))?;
	match entry(account).and_then(|e| e.set_password(&json)) {
		// drop a stale plaintext copy left from a run without a keyring
		Ok(()) => remove_fallback(account).map(|_| ()),
		Err(e) if unavailable(&e) => {
			let path = fallback_path(account)?;
			eprintln!(
				"warning: no OS keyring ({e}); storing credentials in {} (mode 0600)",
				path.display()
			);
			if let Some(dir) = path.parent() {
				fs::DirBuilder::new().recursive(true).mode(0o700).create(dir)?;
			}
			// temp file + rename: a crash never leaves a half-written key
			let tmp = path.with_extension("json.tmp");
			let mut f = fs::OpenOptions::new()
				.write(true)
				.create(true)
				.truncate(true)
				.mode(0o600)
				.open(&tmp)?;
			// `mode` only applies on creation; tighten a pre-existing file too
			f.set_permissions(fs::Permissions::from_mode(0o600))?;
			f.write_all(json.as_bytes())?;
			f.sync_all()?;
			fs::rename(tmp, &path)?;
			Ok(())
		}
		Err(e) => Err(keyring_err(&e)),
	}
}

/// Load the idTag `account`: keyring first, then the fallback file. `None` if neither has it.
pub fn load(account: &str) -> Result<Option<Credentials>> {
	let json = match entry(account).and_then(|e| e.get_password()) {
		Ok(j) => j,
		Err(e) if unavailable(&e) || matches!(e, keyring::Error::NoEntry) => {
			match fs::read_to_string(fallback_path(account)?) {
				Ok(j) => j,
				Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
				Err(e) => return Err(e.into()),
			}
		}
		Err(e) => return Err(keyring_err(&e)),
	};
	let key: PairResult =
		serde_json::from_str(&json).map_err(|e| ClientError::Decode(e.to_string()))?;
	Ok(Some(Credentials { key }))
}

/// Remove the idTag `account` from the keyring and the fallback file. Missing is not an error.
pub fn delete(account: &str) -> Result<()> {
	let removed = remove_fallback(account)?;
	match entry(account).and_then(|e| e.delete_credential()) {
		Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
		// the key lived in the file
		Err(e) if unavailable(&e) && removed => Ok(()),
		Err(e) => Err(keyring_err(&e)),
	}
}

/// Remove the fallback file; whether one was there.
fn remove_fallback(account: &str) -> Result<bool> {
	match fs::remove_file(fallback_path(account)?) {
		Ok(()) => Ok(true),
		Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
		Err(e) => Err(e.into()),
	}
}
