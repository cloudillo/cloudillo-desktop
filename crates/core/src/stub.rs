// SPDX-FileCopyrightText: Szilárd Hajba
// SPDX-License-Identifier: MPL-2.0

//! Test-only HTTP/1.1 stub server: one canned answer per request, every request head logged.
#![allow(clippy::unwrap_used)]

use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::api::{ApiClient, PairResult};
use crate::credentials::Credentials;
use crate::session::Session;

/// Request heads seen so far, lowercased.
pub type Log = Arc<Mutex<Vec<String>>>;

/// A session (target `__root__`) talking to a local server that answers each request with
/// `handler(request line)` = `(status, body)`.
pub async fn stub(
	handler: impl Fn(&str) -> (u16, String) + Send + Sync + 'static,
) -> (Arc<Session>, Log) {
	let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let addr = listener.local_addr().unwrap();
	let log = Log::default();
	let seen = Arc::clone(&log);
	tokio::spawn(async move {
		while let Ok((mut sock, _)) = listener.accept().await {
			let mut buf = Vec::new();
			let mut chunk = [0u8; 8192];
			let mut need = usize::MAX;
			while buf.len() < need {
				let n = sock.read(&mut chunk).await.unwrap_or(0);
				if n == 0 {
					break;
				}
				buf.extend_from_slice(&chunk[..n]);
				if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
					let head = String::from_utf8_lossy(&buf[..i]).to_lowercase();
					let len = head
						.lines()
						.find_map(|l| l.strip_prefix("content-length:"))
						.and_then(|v| v.trim().parse::<usize>().ok())
						.unwrap_or(0);
					need = i + 4 + len;
				}
			}
			let end = buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap_or(buf.len());
			let head = String::from_utf8_lossy(&buf[..end]).into_owned();
			seen.lock().unwrap().push(head.to_lowercase());
			let (status, body) = handler(head.lines().next().unwrap_or_default());
			let resp = format!(
				"HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\
				 connection: close\r\n\r\n{body}",
				body.len()
			);
			let _ = sock.write_all(resp.as_bytes()).await;
		}
	});
	let creds = Credentials {
		key: PairResult {
			api_key: "cl_test".into(),
			id_tag: "test.example".into(),
			scope: String::new(),
			context_id_tag: None,
			target: "__root__".into(),
			access: "W".into(),
			profile: None,
			expires_at: None,
		},
	};
	let home = ApiClient::new(&format!("http://{addr}"), Some("cl_test".into())).unwrap();
	(Arc::new(Session::with_home(creds, home)), log)
}
