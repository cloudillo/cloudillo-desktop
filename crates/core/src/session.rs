// SPDX-FileCopyrightText: Szilárd Hajba
// SPDX-License-Identifier: MPL-2.0

//! Bearer selection for the paired target. Own tenant: the `cl_` key on `https://cl-o.{idTag}`.
//! `contextIdTag`: a proxy token brokered by the personal host, used on `https://cl-o.{ctx}`,
//! cached until shortly before its JWT `exp`.

use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::api::{ApiClient, Parent};
use crate::credentials::Credentials;
use crate::error::{ClientError, Result};

/// Overrides the personal host (hostname or base URL) for every idTag, e.g. a local dev server
/// where `cl-o.{idTag}` does not resolve. Not shown in `--help`.
pub const HOST_ENV: &str = "CLOUDILLO_API_HOST";

/// Personal-host client for `id_tag`: `https://cl-o.{idTag}`, or [`HOST_ENV`] when set.
pub(crate) fn home_client(id_tag: &str, bearer: Option<String>) -> Result<ApiClient> {
	match std::env::var(HOST_ENV) {
		Ok(host) if !host.is_empty() => ApiClient::new(&host, bearer),
		_ => ApiClient::for_id_tag(id_tag, bearer),
	}
}

/// Proxy-token lifetime assumed when its `exp` cannot be decoded.
const TOKEN_LIFETIME: i64 = 3600;
/// Re-broker this long before `exp`.
const REFRESH_MARGIN: i64 = 60;

#[derive(Debug)]
pub struct Session {
	creds: Credentials,
	/// Personal host, the `cl_` key as bearer.
	home: ApiClient,
	/// Context host client + its token's `exp` (unix s); only with `contextIdTag`.
	brokered: Mutex<Option<(ApiClient, i64)>>,
}

impl Session {
	pub fn new(creds: Credentials) -> Result<Self> {
		let home = home_client(&creds.key.id_tag, Some(creds.key.api_key.clone()))?;
		Ok(Self { creds, home, brokered: Mutex::new(None) })
	}

	/// A session on an explicit home client (a local stub server).
	#[cfg(any(test, feature = "test-util"))]
	pub(crate) fn with_home(creds: Credentials, home: ApiClient) -> Self {
		Self { creds, home, brokered: Mutex::new(None) }
	}

	pub fn credentials(&self) -> &Credentials {
		&self.creds
	}

	/// Personal host with the key itself as bearer (e.g. `self_revoke`).
	pub fn home(&self) -> &ApiClient {
		&self.home
	}

	/// Client for the target host, brokering a proxy token if needed or near expiry.
	pub async fn client(&self) -> Result<ApiClient> {
		let Some(ctx) = &self.creds.key.context_id_tag else {
			return Ok(self.home.clone());
		};
		let mut cached = self.brokered.lock().await;
		if let Some((api, exp)) = cached.as_ref()
			&& now() + REFRESH_MARGIN < *exp
		{
			return Ok(api.clone());
		}
		let token = self.home.proxy_token(ctx).await?.token;
		let exp = jwt_exp(&token).unwrap_or_else(|| now() + TOKEN_LIFETIME);
		let api = ApiClient::for_id_tag(ctx, Some(token))?;
		*cached = Some((api.clone(), exp));
		Ok(api)
	}

	/// Drop the cached proxy token; the next [`Self::client`] re-brokers.
	pub async fn invalidate(&self) {
		*self.brokered.lock().await = None;
	}

	/// Run `op` against the target host. A 401 on a brokered token re-brokers once and
	/// retries; 403 is returned as-is (the dead-key probe is the caller's).
	pub async fn call<T, F, Fut>(&self, op: F) -> Result<T>
	where
		F: Fn(ApiClient) -> Fut,
		Fut: Future<Output = Result<T>>,
	{
		match op(self.client().await?).await {
			Err(ClientError::Unauthorized) if self.creds.key.context_id_tag.is_some() => {
				self.invalidate().await;
				op(self.client().await?).await
			}
			other => other,
		}
	}

	/// After a 403 other than cross-room: probe `root`; `Ok(true)` = 403 again, the key is dead.
	pub async fn key_dead(&self, root: &Parent) -> Result<bool> {
		match self.call(|api| async move { api.probe(root).await }).await {
			Ok(()) => Ok(false),
			Err(ClientError::Forbidden { .. }) => Ok(true),
			Err(e) => Err(e),
		}
	}
}

fn now() -> i64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

/// `exp` of a JWT, read without verifying (the server is the verifier).
fn jwt_exp(token: &str) -> Option<i64> {
	#[derive(Deserialize)]
	struct Claims {
		exp: i64,
	}
	let payload = token.split('.').nth(1)?;
	let bytes = URL_SAFE_NO_PAD.decode(payload.trim_end_matches('=')).ok()?;
	serde_json::from_slice::<Claims>(&bytes).ok().map(|c| c.exp)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn reads_jwt_exp() {
		let payload = URL_SAFE_NO_PAD.encode(br#"{"sub":"a","exp":1700000000}"#);
		assert_eq!(jwt_exp(&format!("h.{payload}.sig")), Some(1_700_000_000));
		assert_eq!(jwt_exp("not-a-jwt"), None);
	}
}
