// SPDX-FileCopyrightText: Szilárd Hajba
// SPDX-License-Identifier: MPL-2.0

//! QR pairing against the personal-tenant host (server `cloudillo-auth/src/qr_pair.rs`).

use std::io;
use std::time::Duration;

use crate::api::PairResult;
use crate::credentials::Credentials;
use crate::error::{ClientError, Result};
use crate::session::home_client;

/// Long-poll window per status request (server max).
const POLL_SECS: u64 = 30;
/// Give up on the whole pairing after this.
const OVERALL: Duration = Duration::from_secs(300);

/// Start a pairing session on `id_tag`'s personal host (`https://cl-o.{idTag}`, or
/// `CLOUDILLO_API_HOST`), hand `on_code` the QR payload `cloudillo:qr-pair:<sessionId>:<code>`,
/// the browser URL of the same approval screen (no code: the user types it there) and the
/// 6-digit verification code, then wait for the approval. Denied / expired / 300 s timeout are
/// `ClientError::Io` with kind `PermissionDenied` / `TimedOut` / `TimedOut`.
/// A key approved for another idTag is rejected (`ClientError::Decode`).
/// Stores nothing: the caller decides (temp mounts never store).
pub async fn pair(
	id_tag: &str,
	device_name: &str,
	on_code: impl FnOnce(&str, &str, &str),
) -> Result<Credentials> {
	let api = home_client(id_tag, None)?;
	let init = api.pair_init(device_name).await?;
	let host = std::env::var(crate::session::HOST_ENV).ok();
	on_code(
		&format!("cloudillo:qr-pair:{}:{}", init.session_id, init.code),
		&approve_url(host.as_deref(), id_tag, &init.session_id),
		&init.code,
	);

	let poll = async {
		loop {
			let st = api.pair_status(&init.session_id, &init.secret, POLL_SECS).await?;
			match st.status.as_str() {
				"pending" => {}
				"approved" => {
					return st
						.result
						.ok_or_else(|| ClientError::Decode("approved without a key".into()));
				}
				"denied" => {
					return Err(io_err(io::ErrorKind::PermissionDenied, "pairing denied"));
				}
				"expired" => {
					return Err(io_err(io::ErrorKind::TimedOut, "pairing session expired"));
				}
				other => return Err(ClientError::Decode(format!("pair status {other}"))),
			}
		}
	};
	let key: PairResult = tokio::time::timeout(OVERALL, poll)
		.await
		.map_err(|_| io_err(io::ErrorKind::TimedOut, "pairing timed out after 300 s"))??;
	if key.id_tag != id_tag {
		return Err(ClientError::Decode(format!("paired as {}, expected {id_tag}", key.id_tag)));
	}
	Ok(Credentials { key })
}

/// Shell page showing the same approval screen as scanning the QR. `host` is
/// `CLOUDILLO_API_HOST` (hostname or base URL), normalized like `ApiClient::new`.
fn approve_url(host: Option<&str>, id_tag: &str, session_id: &str) -> String {
	let base = match host {
		Some(h) if h.contains("://") => h.to_owned(),
		Some(h) if !h.is_empty() => format!("https://{h}"),
		_ => format!("https://{id_tag}"),
	};
	format!("{}/~/pair/{session_id}", base.trim_end_matches('/'))
}

fn io_err(kind: io::ErrorKind, msg: &str) -> ClientError {
	ClientError::Io(io::Error::new(kind, msg))
}

#[cfg(test)]
mod tests {
	use super::approve_url;

	#[test]
	fn approve_url_shapes() {
		assert_eq!(
			approve_url(None, "alice.example.com", "abc"),
			"https://alice.example.com/~/pair/abc"
		);
		assert_eq!(approve_url(Some(""), "a.b", "x"), "https://a.b/~/pair/x");
		assert_eq!(
			approve_url(Some("dev.local:8443"), "a.b", "x"),
			"https://dev.local:8443/~/pair/x"
		);
		assert_eq!(
			approve_url(Some("http://localhost:3000/"), "a.b", "x"),
			"http://localhost:3000/~/pair/x"
		);
	}
}
