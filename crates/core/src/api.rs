// SPDX-FileCopyrightText: Szilárd Hajba
// SPDX-License-Identifier: MPL-2.0

//! HTTP client for one Cloudillo host. Wire structs mirror `../cloudillo-rs` JSON (camelCase,
//! unknown fields ignored). Every JSON answer is the `{data, next?, total?}` envelope,
//! unwrapped once in [`ApiClient::page`].

use std::fmt;
use std::time::Duration;

use bytes::Bytes;
use reqwest::{Method, RequestBuilder, Response, StatusCode, Url, header};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};

use crate::error::{ClientError, Result};

/// Page size for `GET /api/files` (server max).
const LIST_LIMIT: &str = "200";

/// Where a listing / upload / move lands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Parent {
	/// A folder entry id.
	Folder(String),
	/// A drive's top level: `None` = main drive, `Some("@tenant~name")` = that room.
	DriveRoot { channel: Option<String> },
}

/// One file/folder entry (`FileView`). Times are RFC3339 strings.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct FileView {
	/// Entry id: the identity every call takes.
	pub id: String,
	/// Content id (`file_id`); `@<f_id>` while pending; `None` for a folder.
	pub content_id: Option<String>,
	pub parent_id: Option<String>,
	pub root_id: Option<String>,
	pub file_name: String,
	/// `BLOB` | `CRDT` | `RTDB` | `FLDR`
	pub file_type: Option<String>,
	pub content_type: Option<String>,
	pub created_at: String,
	pub modified_at: Option<String>,
	/// `active` | `pending` | `deleted`
	pub status: String,
	/// Absolute channel `@tenant~name`; `None` = main drive.
	pub channel: Option<String>,
	pub preset: Option<String>,
	/// Bytes of the current content; absent until the server sends it.
	pub size: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadResponse {
	#[serde(flatten)]
	pub file: FileView,
	/// Dedup hit: no new entry was created.
	#[serde(default)]
	pub existed: bool,
}

/// `PUT /api/files/{id}/content` answer (202).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingReplace {
	pub entry_id: String,
	/// `@<f_id>` of the pending content.
	pub pending_file_id: String,
}

/// Room from `GET /api/channels` (`PorchEntry`); `name` identifies it.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Channel {
	pub name: String,
	#[serde(default)]
	pub title: Option<String>,
	#[serde(default)]
	pub descr: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VersionView {
	pub file_id: String,
	pub replaced_at: String,
	#[serde(default)]
	pub replaced_by: Option<String>,
	#[serde(default)]
	pub size: Option<u64>,
}

/// `GET /api/file-changes` page. `cursor` is unix seconds; page on with
/// `since=cursor&after=next`.
#[derive(Debug, Clone, Deserialize)]
pub struct FileChanges {
	pub changes: Vec<FileChange>,
	pub cursor: i64,
	#[serde(default)]
	pub next: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)] // decoded in batches, short-lived
pub enum FileChange {
	/// Must stay first: a row has no `gone` field, so it falls through to `Row`.
	Gone(FileGone),
	Row(FileChangeRow),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileGone {
	pub id: String,
	pub updated_at: i64,
	pub gone: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileChangeRow {
	#[serde(flatten)]
	pub file: FileView,
	pub updated_at: i64,
}

#[derive(Clone, Deserialize)]
pub struct ProxyToken {
	pub token: String,
	#[serde(default)]
	pub roles: Option<Vec<String>>,
}

impl fmt::Debug for ProxyToken {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("ProxyToken")
			.field("token", &"<redacted>")
			.field("roles", &self.roles)
			.finish()
	}
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PairInit {
	pub session_id: String,
	pub secret: String,
	/// 6-digit verification code, shown only on this device
	pub code: String,
}

impl fmt::Debug for PairInit {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("PairInit")
			.field("session_id", &self.session_id)
			.field("secret", &"<redacted>")
			.field("code", &"<redacted>")
			.finish()
	}
}

/// `status`: `pending` | `approved` | `denied` | `expired`; `result` only when approved.
#[derive(Debug, Clone, Deserialize)]
pub struct PairStatus {
	pub status: String,
	#[serde(flatten)]
	pub result: Option<PairResult>,
}

/// Delivered once on approval. Holds the plaintext key.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PairResult {
	pub api_key: String,
	pub id_tag: String,
	pub scope: String,
	#[serde(default)]
	pub context_id_tag: Option<String>,
	pub target: String,
	pub access: String,
	#[serde(default)]
	pub profile: Option<String>,
	#[serde(default)]
	pub expires_at: Option<String>,
}

impl fmt::Debug for PairResult {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("PairResult")
			.field("api_key", &"<redacted>")
			.field("id_tag", &self.id_tag)
			.field("scope", &self.scope)
			.field("context_id_tag", &self.context_id_tag)
			.field("target", &self.target)
			.field("access", &self.access)
			.field("profile", &self.profile)
			.field("expires_at", &self.expires_at)
			.finish()
	}
}

/// One page of a list call.
#[derive(Debug, Clone)]
pub struct Page<T> {
	pub data: T,
	/// Cursor for the next page (`cursor` query param); `None` on the last page.
	pub next: Option<String>,
}

#[derive(Deserialize)]
struct Envelope<T> {
	data: T,
	#[serde(default)]
	next: Option<String>,
}

/// A streaming download. `partial` is true when the server honoured the Range (206).
#[derive(Debug)]
pub struct Download {
	pub partial: bool,
	/// Bytes in this response body, if the server said.
	pub len: Option<u64>,
	resp: Response,
}

impl Download {
	/// Next body chunk; `None` at the end.
	pub async fn chunk(&mut self) -> Result<Option<Bytes>> {
		Ok(self.resp.chunk().await?)
	}
}

#[derive(Clone)]
pub struct ApiClient {
	host: Url,
	http: reqwest::Client,
	bearer: Option<String>,
}

impl fmt::Debug for ApiClient {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("ApiClient")
			.field("host", &self.host.as_str())
			.field("bearer", &self.bearer.as_ref().map(|_| "<redacted>"))
			.finish_non_exhaustive()
	}
}

impl ApiClient {
	/// `host` is a hostname (`cl-o.alice.example`, https assumed) or a full base URL (https;
	/// http only for loopback). IDN hosts are converted to their A-label form by URL parsing.
	pub fn new(host: &str, bearer: Option<String>) -> Result<Self> {
		let base = if host.contains("://") { host.to_owned() } else { format!("https://{host}") };
		let host = Url::parse(&base)
			.map_err(|e| ClientError::Decode(format!("invalid host {host}: {e}")))?;
		let local = match host.host_str() {
			Some("localhost") => true,
			Some(h) => h
				.trim_start_matches('[')
				.trim_end_matches(']')
				.parse::<std::net::IpAddr>()
				.is_ok_and(|ip| ip.is_loopback()),
			None => false,
		};
		if !(host.scheme() == "https" || host.scheme() == "http" && local) {
			return Err(ClientError::Decode("https required (http only for loopback)".into()));
		}
		let http = reqwest::Client::builder()
			.user_agent(concat!("cloudillo-desktop/", env!("CARGO_PKG_VERSION")))
			// never re-send credential headers (e.g. the pairing secret) to another host
			.redirect(reqwest::redirect::Policy::none())
			// no whole-request timeout: it would cut large streamed downloads
			.connect_timeout(Duration::from_secs(10))
			// above the longest long-poll (`pair_status`, <= 30 s)
			.read_timeout(Duration::from_secs(60))
			.build()?;
		Ok(Self { host, http, bearer })
	}

	/// API host of an identity: `https://cl-o.{idTag}` (server `request.rs` `host_for`).
	pub fn for_id_tag(id_tag: &str, bearer: Option<String>) -> Result<Self> {
		if id_tag.is_empty()
			|| id_tag
				.chars()
				.any(|c| c.is_whitespace() || c.is_control() || "/\\@:?#%[]".contains(c))
		{
			return Err(ClientError::Decode(format!("invalid id tag {id_tag:?}")));
		}
		Self::new(&format!("cl-o.{id_tag}"), bearer)
	}

	/// Same host and connection pool, different bearer.
	pub fn with_bearer(&self, bearer: Option<String>) -> Self {
		Self { host: self.host.clone(), http: self.http.clone(), bearer }
	}

	pub fn host(&self) -> &Url {
		&self.host
	}

	// --- plumbing -------------------------------------------------------------------------

	fn req(&self, method: Method, segments: &[&str]) -> RequestBuilder {
		let mut url = self.host.clone();
		// https URLs are always base URLs, so this cannot fail
		if let Ok(mut path) = url.path_segments_mut() {
			path.pop_if_empty().extend(segments);
		}
		let rb = self.http.request(method, url);
		match &self.bearer {
			Some(b) => rb.bearer_auth(b),
			None => rb,
		}
	}

	async fn send(rb: RequestBuilder) -> Result<Response> {
		let resp = rb.send().await?;
		if resp.status().is_success() {
			return Ok(resp);
		}
		let status = resp.status().as_u16();
		let body = resp.bytes().await.unwrap_or_default();
		Err(ClientError::from_status(status, &body))
	}

	async fn page<T: DeserializeOwned>(rb: RequestBuilder) -> Result<Page<T>> {
		let body = Self::send(rb).await?.bytes().await?;
		let env: Envelope<T> =
			serde_json::from_slice(&body).map_err(|e| ClientError::Decode(e.to_string()))?;
		Ok(Page { data: env.data, next: env.next })
	}

	async fn data<T: DeserializeOwned>(rb: RequestBuilder) -> Result<T> {
		Ok(Self::page(rb).await?.data)
	}

	// --- files ----------------------------------------------------------------------------

	/// `GET /api/files` — one page of a folder's or drive's children.
	pub async fn list_children(
		&self,
		parent: &Parent,
		cursor: Option<&str>,
	) -> Result<Page<Vec<FileView>>> {
		self.list(parent, cursor, LIST_LIMIT).await
	}

	/// `GET /api/files?limit=1` under `parent` — the dead-key probe (a revoked `cl_` key gets
	/// 403 here too).
	pub async fn probe(&self, parent: &Parent) -> Result<()> {
		self.list(parent, None, "1").await.map(drop)
	}

	async fn list(
		&self,
		parent: &Parent,
		cursor: Option<&str>,
		limit: &str,
	) -> Result<Page<Vec<FileView>>> {
		let mut q = vec![("limit", limit)];
		match parent {
			Parent::Folder(id) => q.push(("parentId", id.as_str())),
			Parent::DriveRoot { channel } => {
				q.push(("parentId", "__root__"));
				q.push(("channel", channel.as_deref().unwrap_or("")));
			}
		}
		if let Some(c) = cursor {
			q.push(("cursor", c));
		}
		Self::page(self.req(Method::GET, &["api", "files"]).query(&q)).await
	}

	/// `GET /api/files/{id}/metadata`
	pub async fn get_entry(&self, id: &str) -> Result<FileView> {
		Self::data(self.req(Method::GET, &["api", "files", id, "metadata"])).await
	}

	/// `GET /api/files/{id}` (original variant). `range` = inclusive `(first, last)` byte.
	pub async fn download(&self, id: &str, range: Option<(u64, u64)>) -> Result<Download> {
		let mut rb = self.req(Method::GET, &["api", "files", id]);
		if let Some((first, last)) = range {
			rb = rb.header(header::RANGE, format!("bytes={first}-{last}"));
		}
		let resp = Self::send(rb).await?;
		Ok(Download {
			partial: resp.status() == StatusCode::PARTIAL_CONTENT,
			len: resp.content_length(),
			resp,
		})
	}

	/// `POST /api/uploads/{profile}?name=&parentId=|channel=` — a new BLOB entry.
	pub async fn upload_new(
		&self,
		profile: &str,
		parent: &Parent,
		name: &str,
		content_type: Option<&str>,
		body: impl Into<reqwest::Body>,
	) -> Result<UploadResponse> {
		let mut q = vec![("name", name)];
		match parent {
			Parent::Folder(id) => q.push(("parentId", id.as_str())),
			Parent::DriveRoot { channel: Some(ch) } => q.push(("channel", ch.as_str())),
			Parent::DriveRoot { channel: None } => {}
		}
		let rb = self
			.req(Method::POST, &["api", "uploads", profile])
			.query(&q)
			.header(header::CONTENT_TYPE, content_type.unwrap_or("application/octet-stream"))
			.body(body);
		Self::data(rb).await
	}

	/// `PUT /api/files/{id}/content` with `If-Match: "<contentId>"`. `profile` = the `preset`
	/// query param; `None` reuses the current content's profile.
	pub async fn replace(
		&self,
		id: &str,
		if_match: &str,
		profile: Option<&str>,
		content_type: Option<&str>,
		body: impl Into<reqwest::Body>,
	) -> Result<PendingReplace> {
		let mut rb = self
			.req(Method::PUT, &["api", "files", id, "content"])
			.header(header::IF_MATCH, format!("\"{if_match}\""))
			.header(header::CONTENT_TYPE, content_type.unwrap_or("application/octet-stream"))
			.body(body);
		if let Some(p) = profile {
			rb = rb.query(&[("preset", p)]);
		}
		Self::data(rb).await
	}

	/// `POST /api/files` `{fileTp: "FLDR", …}`
	pub async fn mkdir(&self, parent: &Parent, name: &str) -> Result<FileView> {
		let mut body = json!({ "fileTp": "FLDR", "fileName": name });
		match parent {
			Parent::Folder(id) => body["parentId"] = json!(id),
			Parent::DriveRoot { channel: Some(ch) } => body["channel"] = json!(ch),
			Parent::DriveRoot { channel: None } => {}
		}
		Self::data(self.req(Method::POST, &["api", "files"]).json(&body)).await
	}

	/// `PATCH /api/files/{id}` — rename and/or move. A move to a drive root sends
	/// `parentId: null` plus `channel` (`null` = main drive).
	pub async fn patch(
		&self,
		id: &str,
		name: Option<&str>,
		parent: Option<&Parent>,
	) -> Result<FileView> {
		let mut body = Map::new();
		if let Some(n) = name {
			body.insert("fileName".into(), json!(n));
		}
		match parent {
			Some(Parent::Folder(pid)) => {
				body.insert("parentId".into(), json!(pid));
			}
			Some(Parent::DriveRoot { channel }) => {
				body.insert("parentId".into(), Value::Null);
				body.insert("channel".into(), json!(channel));
			}
			None => {}
		}
		Self::data(self.req(Method::PATCH, &["api", "files", id]).json(&body)).await
	}

	/// `DELETE /api/files/{id}` — move to trash.
	pub async fn trash(&self, id: &str) -> Result<()> {
		Self::send(self.req(Method::DELETE, &["api", "files", id])).await.map(drop)
	}

	/// `POST /api/files/{id}/restore` `{parentId?}`
	pub async fn restore_trashed(&self, id: &str, parent: Option<&str>) -> Result<FileView> {
		let body = match parent {
			Some(p) => json!({ "parentId": p }),
			None => json!({}),
		};
		Self::data(self.req(Method::POST, &["api", "files", id, "restore"]).json(&body)).await
	}

	/// `GET /api/files/{id}/versions`
	pub async fn list_versions(&self, id: &str) -> Result<Vec<VersionView>> {
		Self::data(self.req(Method::GET, &["api", "files", id, "versions"])).await
	}

	/// `POST /api/files/{id}/versions/{file_id}/restore`
	pub async fn restore_version(
		&self,
		id: &str,
		file_id: &str,
		if_match: Option<&str>,
	) -> Result<FileView> {
		let mut rb = self.req(Method::POST, &["api", "files", id, "versions", file_id, "restore"]);
		if let Some(m) = if_match {
			rb = rb.header(header::IF_MATCH, format!("\"{m}\""));
		}
		Self::data(rb).await
	}

	/// `POST /api/files/{id}/duplicate` `{fromVersion}` — a new sibling with that version.
	pub async fn duplicate_from_version(&self, id: &str, file_id: &str) -> Result<FileView> {
		let body = json!({ "fromVersion": file_id });
		Self::data(self.req(Method::POST, &["api", "files", id, "duplicate"]).json(&body)).await
	}

	/// `GET /api/file-changes`. `root` = folder entry id, `__root__` or `~room`;
	/// `since` = unix seconds. 410 (`Http{status: 410}`) means re-list.
	pub async fn file_changes(
		&self,
		root: &str,
		since: i64,
		after: Option<&str>,
		limit: Option<u32>,
	) -> Result<FileChanges> {
		let mut q = vec![("root", root.to_owned()), ("since", since.to_string())];
		if let Some(a) = after {
			q.push(("after", a.to_owned()));
		}
		if let Some(l) = limit {
			q.push(("limit", l.to_string()));
		}
		Self::data(self.req(Method::GET, &["api", "file-changes"]).query(&q)).await
	}

	/// `GET /api/channels`
	pub async fn list_channels(&self) -> Result<Vec<Channel>> {
		Self::data(self.req(Method::GET, &["api", "channels"])).await
	}

	// --- auth -----------------------------------------------------------------------------

	/// `POST /api/auth/proxy-token` `{idTag}` — bearer for the context's host.
	pub async fn proxy_token(&self, id_tag: &str) -> Result<ProxyToken> {
		let body = json!({ "idTag": id_tag });
		Self::data(self.req(Method::POST, &["api", "auth", "proxy-token"]).json(&body)).await
	}

	/// `DELETE /api/auth/api-keys/self` — revoke the key used as bearer.
	pub async fn self_revoke(&self) -> Result<()> {
		Self::send(self.req(Method::DELETE, &["api", "auth", "api-keys", "self"]))
			.await
			.map(drop)
	}

	/// `POST /api/auth/pair` `{deviceName}` (unauthenticated).
	pub async fn pair_init(&self, device_name: &str) -> Result<PairInit> {
		let body = json!({ "deviceName": device_name });
		Self::data(self.req(Method::POST, &["api", "auth", "pair"]).json(&body)).await
	}

	/// `GET /api/auth/pair/{sessionId}?timeout=` long-poll (`timeout` ≤ 30 s).
	/// An unknown/consumed session reads as `expired` (server sends 200 `expired`; 404 too).
	pub async fn pair_status(
		&self,
		session_id: &str,
		secret: &str,
		timeout_secs: u64,
	) -> Result<PairStatus> {
		let rb = self
			.req(Method::GET, &["api", "auth", "pair", session_id])
			.header("cloudillo-pair-secret", secret)
			.query(&[("timeout", timeout_secs)]);
		match Self::data(rb).await {
			Err(ClientError::NotFound) => Ok(PairStatus { status: "expired".into(), result: None }),
			other => other,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn decodes_change_rows_and_gone() {
		let j = r#"{"changes":[
			{"id":"e1","updatedAt":5,"gone":true},
			{"id":"e2","fileName":"a.txt","createdAt":"2026-01-01T00:00:00Z","status":"active",
			 "contentId":"b1","updatedAt":6,"size":3}
		],"cursor":6}"#;
		let c: FileChanges = serde_json::from_str(j).unwrap();
		assert!(matches!(&c.changes[0], FileChange::Gone(g) if g.id == "e1"));
		assert!(matches!(&c.changes[1], FileChange::Row(r) if r.file.size == Some(3)));
	}

	#[test]
	fn pair_status_flattens_result_only_when_approved() {
		let p: PairStatus = serde_json::from_str(r#"{"status":"pending"}"#).unwrap();
		assert!(p.result.is_none());
		let j = r#"{"status":"approved","apiKey":"cl_x","idTag":"a.b","scope":"s",
			"target":"*","access":"W","profile":"standard"}"#;
		let p: PairStatus = serde_json::from_str(j).unwrap();
		assert!(!format!("{p:?}").contains("cl_x"));
		assert_eq!(p.result.map(|r| r.api_key).as_deref(), Some("cl_x"));
	}

	#[test]
	fn rejects_bad_id_tags() {
		for bad in ["x@evil.com", "a/b", "a:1", "a?b", "a#b", "", "a b"] {
			assert!(ApiClient::for_id_tag(bad, None).is_err(), "{bad:?}");
		}
		let api = ApiClient::for_id_tag("alice.example", None).unwrap();
		assert_eq!(api.host().host_str(), Some("cl-o.alice.example"));
		assert!(ApiClient::for_id_tag("münchen.example", None).is_ok());
	}

	#[test]
	fn requires_https_except_loopback() {
		for bad in ["http://evil.com", "ftp://x"] {
			assert!(ApiClient::new(bad, None).is_err(), "{bad}");
		}
		for ok in ["https://x", "http://127.0.0.1:8080", "http://localhost:1", "http://[::1]:1"] {
			assert!(ApiClient::new(ok, None).is_ok(), "{ok}");
		}
	}
}
