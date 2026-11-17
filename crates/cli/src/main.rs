// SPDX-FileCopyrightText: Szilárd Hajba
// SPDX-License-Identifier: MPL-2.0

//! `cloudillo` — mount a Cloudillo identity's files over FUSE.
#![forbid(unsafe_code)]

mod daemon;
mod qr;

use std::error::Error;
use std::io::{IsTerminal, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use cloudillo_desktop_core::ipc::{self, Request};
use cloudillo_desktop_core::seal::Seal;
use cloudillo_desktop_core::{Config, Credentials, Session, Uploader, credentials, pair};
use cloudillo_desktop_fs_fuse::{Mode, MountOptions, mount};
use tokio::io::AsyncBufReadExt;
use tokio::signal::unix::{SignalKind, signal};
use tracing_subscriber::EnvFilter;

type Res<T> = Result<T, Box<dyn Error>>;

#[derive(Parser)]
#[command(name = "cloudillo", version, about = "Cloudillo desktop client")]
struct Cli {
	#[command(subcommand)]
	cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
	/// Log this machine in to a Cloudillo identity via a QR code scanned on your phone; stores the
	/// key in the OS keyring. Needed only to re-pair; `mount --persist` logs in automatically.
	Login {
		/// Your Cloudillo identity, e.g. `alice.example.com` (API host `cl-o.{idTag}`).
		id_tag: String,
		/// Device name shown on the phone [default: this machine's hostname].
		#[arg(long)]
		name: Option<String>,
	},
	/// Log this machine out: revoke its key on the server and delete it locally.
	Logout {
		/// Your Cloudillo identity, as given to `login`.
		id_tag: String,
	},
	/// Mount your files. By default the mount is temporary: you pair by QR, the key stays in
	/// memory and the mount runs in the foreground until Ctrl-C, when the key is revoked. With
	/// `--persist`, the key is stored and the daemon keeps the mount.
	Mount {
		/// Your Cloudillo identity, e.g. `alice.example.com`.
		id_tag: String,
		mountpoint: PathBuf,
		/// Keep the key in the OS keyring and let the daemon own the mount (survives
		/// logout/reboot; enables --mode).
		#[arg(long)]
		persist: bool,
		/// Device name shown on the phone when pairing [default: this machine's hostname;
		/// temporary mounts: "<hostname> (temporary)"].
		#[arg(long)]
		name: Option<String>,
		/// Cache mode (needs `--persist`): `off` (no local copy), `on-demand` (metadata DB, files
		/// cached on open) or `pinned` (as `on-demand`, every file kept cached for offline use)
		/// [default: off].
		#[arg(long, requires = "persist")]
		mode: Option<String>,
	},
	/// Run the mount daemon in the foreground (other commands start it on demand).
	Daemon,
	/// List the daemon's mounts.
	Status,
	/// Stop syncing an `on-demand` mount (the files stay mounted).
	Pause { id: String },
	/// Restart syncing a paused mount.
	Resume { id: String },
	/// Remove a persistent mount.
	Unmount {
		/// Mount id (see `status`) or mountpoint.
		target: String,
		/// Unmount even while uploads are in flight; unsent files are dropped.
		#[arg(long)]
		discard: bool,
	},
	/// List the earlier versions of a file in a mount.
	Versions { path: PathBuf },
	/// Make an earlier version the current content of a file.
	Restore {
		path: PathBuf,
		/// Version id, as listed by `versions`.
		file_id: String,
		/// Restore into a new sibling file instead of replacing the current content.
		#[arg(long)]
		as_copy: bool,
	},
	/// Keep a file or folder of an `on-demand` mount cached for offline use.
	Pin { path: PathBuf },
	/// Stop keeping a file or folder (and everything under it) cached.
	Unpin { path: PathBuf },
}

#[tokio::main]
async fn main() -> ExitCode {
	tracing_subscriber::fmt()
		.with_env_filter(
			EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
		)
		.with_writer(std::io::stderr)
		.init();
	let res = match Cli::parse().cmd {
		Cmd::Login { id_tag, name } => run_login(&id_tag, name).await,
		Cmd::Logout { id_tag } => run_logout(&id_tag).await,
		Cmd::Mount { id_tag, mountpoint, persist: false, name, .. } => {
			run_temp(&id_tag, &mountpoint, name).await
		}
		Cmd::Mount { id_tag, mountpoint, persist: true, name, mode } => {
			run_mount(id_tag, &mountpoint, name, mode.unwrap_or_else(|| "off".to_owned())).await
		}
		Cmd::Daemon => daemon::run().await,
		Cmd::Status => ask(Request::Status).await,
		Cmd::Pause { id } => ask(Request::Pause { id }).await,
		Cmd::Resume { id } => ask(Request::Resume { id }).await,
		Cmd::Unmount { target, discard } => {
			// an id stays as is; a path is made absolute without touching the (maybe dead) mount
			let target = if target.bytes().all(|b| b.is_ascii_digit()) {
				Ok(target)
			} else {
				std::path::absolute(&target).map(|p| p.to_string_lossy().into_owned())
			};
			match target {
				Ok(target) => ask(Request::Unmount { target, discard }).await,
				Err(e) => Err(e.into()),
			}
		}
		Cmd::Versions { path } => versions(&path),
		Cmd::Restore { path, file_id, as_copy } => restore(&path, &file_id, as_copy),
		Cmd::Pin { path } => match path.canonicalize() {
			Ok(path) => ask(Request::Pin { path }).await,
			Err(e) => Err(format!("{}: {e}", path.display()).into()),
		},
		Cmd::Unpin { path } => match path.canonicalize() {
			Ok(path) => ask(Request::Unpin { path }).await,
			Err(e) => Err(format!("{}: {e}", path.display()).into()),
		},
	};
	match res {
		Ok(code) => code,
		Err(e) => {
			eprintln!("error: {e}");
			ExitCode::FAILURE
		}
	}
}

/// Send `req` to the daemon and print its answer.
async fn ask(req: Request) -> Res<ExitCode> {
	println!("{}", daemon::request(&req).await?);
	Ok(ExitCode::SUCCESS)
}

/// Versions are served by the mount itself as xattrs (temporary and daemon mounts alike).
fn versions(path: &Path) -> Res<ExitCode> {
	// one 64 KiB read; size-probe first if lists ever exceed it
	let mut buf = vec![0u8; 65536];
	let n = rustix::fs::getxattr(path, "user.cloudillo.versions", &mut buf)
		.map_err(|e| xattr_err(path, e))?;
	let text = String::from_utf8_lossy(&buf[..n]);
	println!("{}", if text.is_empty() { "no earlier versions" } else { &text });
	Ok(ExitCode::SUCCESS)
}

fn restore(path: &Path, file_id: &str, as_copy: bool) -> Res<ExitCode> {
	let name = if as_copy { "user.cloudillo.restore_copy" } else { "user.cloudillo.restore" };
	rustix::fs::setxattr(path, name, file_id.as_bytes(), rustix::fs::XattrFlags::empty())
		.map_err(|e| xattr_err(path, e))?;
	if as_copy {
		println!("restored {file_id} as a copy");
	} else {
		println!("restored {file_id} into {}", path.display());
	}
	Ok(ExitCode::SUCCESS)
}

fn xattr_err(path: &Path, e: rustix::io::Errno) -> String {
	use rustix::io::Errno as E;
	match e {
		E::NODATA | E::NOTSUP | E::NOSYS => {
			format!("{} is not a file in a cloudillo mount", path.display())
		}
		E::AGAIN => "the file changed meanwhile (or is not uploaded yet); retry".to_owned(),
		E::ROFS => "read-only mount".to_owned(),
		e => format!("{}: {e}", path.display()),
	}
}

/// `name`, else the hostname (the same one conflict copies use).
fn device_name(name: Option<String>) -> String {
	name.unwrap_or_else(cloudillo_desktop_core::conflict::device)
}

async fn pair_qr(id_tag: &str, name: Option<String>) -> Res<Credentials> {
	Ok(pair(id_tag, &device_name(name), qr::print).await?)
}

/// Refused while a persistent mount uses the key. A failed revoke (e.g. the device was already
/// removed on the phone) still deletes the local key, but exits non-zero.
async fn run_logout(id_tag: &str) -> Res<ExitCode> {
	if let Some(m) = Config::load()?.mounts.iter().find(|m| m.account == id_tag) {
		return Err(format!(
			"{} uses this pairing; run `cloudillo unmount {}` first",
			m.mountpoint.display(),
			m.mountpoint.display()
		)
		.into());
	}
	// blocking keyring calls on the runtime thread; short D-Bus round trips.
	let Some(creds) = credentials::load(id_tag)? else {
		return Err(format!("{id_tag} is not logged in").into());
	};
	let revoked = revoke(&Session::new(creds)?, None).await;
	credentials::delete(id_tag)?;
	say(format!("Logged out of {id_tag} on this machine."));
	Ok(if revoked { ExitCode::SUCCESS } else { ExitCode::FAILURE })
}

/// `mount --persist`: log in here first if the keyring has no key (the daemon has no terminal).
async fn run_mount(id_tag: String, mp: &Path, name: Option<String>, mode: String) -> Res<ExitCode> {
	let mountpoint = mp.canonicalize().map_err(|e| format!("{}: {e}", mp.display()))?;
	// blocking keyring call on the runtime thread; one short D-Bus round trip.
	if credentials::load(&id_tag)?.is_none() {
		run_login(&id_tag, name).await?;
	}
	ask(Request::Mount { account: id_tag, mountpoint, mode }).await
}

/// A human status line on stderr; never panics (the tty may be gone after SIGHUP).
fn say(msg: impl std::fmt::Display) {
	let _ = writeln!(std::io::stderr(), "{msg}");
}

/// Up-front checks, before pairing: an existing directory that is not already a mount point.
fn check_mountpoint(p: &Path) -> Result<PathBuf, String> {
	let shown = p.display();
	let p = p.canonicalize().map_err(|e| format!("{shown}: {e}"))?;
	if !p.is_dir() {
		return Err(format!("{shown}: not a directory"));
	}
	let dev = |q: &Path| std::fs::metadata(q).map(|m| m.dev()).map_err(|e| format!("{shown}: {e}"));
	if dev(&p)? != dev(&p.join(".."))? {
		return Err(format!("{shown} is already a mount point"));
	}
	Ok(p)
}

async fn run_login(id_tag: &str, name: Option<String>) -> Res<ExitCode> {
	let creds = pair_qr(id_tag, name).await?;
	// blocking keyring call on the runtime thread; one short D-Bus round trip.
	credentials::store(&creds)?;
	println!("Logged in as {}", creds.key.id_tag);
	Ok(ExitCode::SUCCESS)
}

/// Temporary mount (the default): foreground, outside the daemon, key in memory only and revoked
/// on exit; `off` mode only.
async fn run_temp(id_tag: &str, mountpoint: &Path, name: Option<String>) -> Res<ExitCode> {
	let mp = check_mountpoint(mountpoint)?;
	// staging under the run dir, sealed: may be on persistent disk (no $XDG_RUNTIME_DIR)
	let staging = ipc::run_dir()
		.ok_or("no runtime or cache directory")?
		.join(std::process::id().to_string());
	let name = name.unwrap_or_else(|| format!("{} (temporary)", device_name(None)));
	let creds = pair_qr(id_tag, Some(name.clone())).await?;

	let session = Arc::new(Session::new(creds)?);
	let uploader = Arc::new(Uploader::new(Arc::clone(&session), Some(Arc::new(Seal::random()?))));
	let opts =
		MountOptions { staging: staging.clone(), uploader: Arc::clone(&uploader), mode: Mode::Off };
	let (bg, writes) = match mount(Arc::clone(&session), &mp, opts) {
		Ok(m) => m,
		Err(e) => {
			revoke(&session, Some(&name)).await;
			let _ = std::fs::remove_dir(&staging);
			return Err(format!("mount {}: {e}", mp.display()).into());
		}
	};
	let access = if session.credentials().key.access == "R" { "read-only" } else { "read-write" };
	say(format!(
		"Mounted {id_tag} at {} (temporary, {access}).\nPress Ctrl-C to unmount; this device's \
		 key is then revoked.",
		mp.display()
	));

	let mut term = signal(SignalKind::terminate())?;
	let mut hup = signal(SignalKind::hangup())?;
	// Enter-to-retry needs a person at the terminal: Ctrl-C or an eject, not a SIGTERM/SIGHUP.
	let attended = tokio::select! {
		r = tokio::signal::ctrl_c() => { r?; true }
		_ = term.recv() => false,
		_ = hup.recv() => false,
		() = async {
			let mut tick = tokio::time::interval(Duration::from_secs(1));
			while !bg.guard.is_finished() {
				tick.tick().await;
			}
		} => { say("The mount was removed outside cloudillo."); true }
	};
	let interactive = attended && std::io::stdin().is_terminal();

	let mut abandoned = false;
	if daemon::busy(&uploader).await {
		say("Unmounting: waiting for uploads to finish (Ctrl-C again to abandon them)…");
	}
	tokio::select! {
		() = async { uploader.idle().await; writes.retry().await } => {}
		_ = tokio::signal::ctrl_c() => abandoned = true,
	}
	let mut failed = writes.failed();
	if !failed.is_empty() && interactive && !abandoned {
		let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
		while !failed.is_empty() {
			say(format!(
				"{} file(s) could not be uploaded: {}.\nThey exist only in this session and are \
				 lost when it ends. The mount stays open so you can copy them elsewhere.\nPress \
				 Enter to retry, or Ctrl-C to discard them and exit.",
				failed.len(),
				failed.join(", ")
			));
			tokio::select! {
				l = lines.next_line() => match l {
					Ok(Some(_)) => writes.retry().await,
					_ => break, // stdin closed
				},
				_ = tokio::signal::ctrl_c() => break,
			}
			failed = writes.failed();
		}
	}

	// Dropping the session unmounts and joins the FUSE thread; keep that off the runtime.
	tokio::task::spawn_blocking(move || drop(bg)).await?;
	// Leftovers = failed uploads, unreadable once the key is gone with this process.
	let left = std::fs::read_dir(&staging).map_or(0, Iterator::count);
	let code = if left == 0 {
		let _ = std::fs::remove_dir(&staging);
		ExitCode::SUCCESS
	} else {
		if failed.is_empty() {
			say(format!("Discarded {left} file(s) that were not uploaded."));
		} else {
			say(format!("Discarded {left} file(s) that were not uploaded: {}", failed.join(", ")));
		}
		let _ = std::fs::remove_dir_all(&staging);
		ExitCode::FAILURE
	};
	revoke(&session, Some(&name)).await;
	Ok(code)
}

/// Delete this device's key on the server (`name`: the device name, if known); a failure is
/// reported, not fatal. `true` = revoked.
async fn revoke(session: &Session, name: Option<&str>) -> bool {
	let res = tokio::time::timeout(Duration::from_secs(10), session.home().self_revoke()).await;
	let err = match res {
		Ok(Ok(())) => {
			say("Revoked this device's key on the server.");
			return true;
		}
		Ok(Err(e)) => e.to_string(),
		Err(_) => "timed out".to_owned(),
	};
	let device = name.map_or_else(|| "this device".to_owned(), |n| format!("device \"{n}\""));
	say(format!(
		"warning: could not revoke this device's key ({err}); remove {device} in the Cloudillo app."
	));
	false
}

#[cfg(test)]
mod tests {
	use clap::CommandFactory;

	use super::*;

	#[test]
	fn cli_args() {
		Cli::command().debug_assert();
		let base = ["cloudillo", "mount", "a.b", "/x"];
		let parse = |extra: &[&str]| Cli::try_parse_from(base.iter().chain(extra));
		assert!(parse(&[]).is_ok());
		assert!(parse(&["--mode", "pinned"]).is_err());
		assert!(parse(&["--persist", "--mode", "pinned"]).is_ok());
		assert!(parse(&["--temp"]).is_err());
		assert!(parse(&["--name", "n"]).is_ok());
		assert!(parse(&["--persist", "--name", "n"]).is_ok());
		assert!(Cli::try_parse_from(["cloudillo", "login", "a.b"]).is_ok());
		assert!(Cli::try_parse_from(["cloudillo", "logout", "a.b"]).is_ok());
		assert!(Cli::try_parse_from(["cloudillo", "pair", "a.b"]).is_err());
	}

	#[test]
	fn mountpoint_checks() {
		assert!(check_mountpoint(Path::new("/nonexistent/cloudillo")).is_err());
		let file = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
		assert!(check_mountpoint(&file).is_err());
		// not temp_dir(): /tmp is often a tmpfs mount point of its own
		assert!(check_mountpoint(&Path::new(env!("CARGO_MANIFEST_DIR")).join("src")).is_ok());
	}

	#[test]
	fn xattr_err_messages() {
		use rustix::io::Errno as E;

		let p = Path::new("/m/f");
		let not_file = "/m/f is not a file in a cloudillo mount";
		for e in [E::NODATA, E::NOTSUP, E::NOSYS] {
			assert_eq!(xattr_err(p, e), not_file);
		}
		assert_eq!(
			xattr_err(p, E::AGAIN),
			"the file changed meanwhile (or is not uploaded yet); retry"
		);
		assert_eq!(xattr_err(p, E::ROFS), "read-only mount");
		assert_eq!(xattr_err(p, E::ACCESS), format!("/m/f: {}", E::ACCESS));
	}
}
