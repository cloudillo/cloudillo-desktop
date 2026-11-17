// SPDX-FileCopyrightText: Szilárd Hajba
// SPDX-License-Identifier: MPL-2.0

//! Client-side error type, mapped from HTTP status + the server's error body.

use serde::Deserialize;

pub type Result<T> = std::result::Result<T, ClientError>;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
	/// Any non-success status without a dedicated variant (e.g. 428, 416, 5xx).
	#[error("HTTP {status} {code}: {message}")]
	Http { status: u16, code: String, message: String },
	/// 412: `If-Match` did not match the current content id.
	#[error("precondition failed")]
	PreconditionFailed,
	/// 401: expired/invalid token.
	#[error("unauthorized")]
	Unauthorized,
	/// 403: `E-AUTH-FORBIDDEN` (also a dead `cl_` key), `E-FILE-CROSSROOM`, …
	#[error("forbidden ({code})")]
	Forbidden { code: String },
	/// 400 `E-CORE-INVALID` "upload exceeds maximum file size".
	#[error("too large")]
	TooLarge,
	/// 413 `E-FILE-QUOTA`.
	#[error("quota exceeded")]
	QuotaExceeded,
	#[error("not found")]
	NotFound,
	#[error("I/O: {0}")]
	Io(#[from] std::io::Error),
	#[error("decode: {0}")]
	Decode(String),
	/// Transport failure (connect, TLS, timeout, body read).
	#[error("network: {0}")]
	Network(#[from] reqwest::Error),
}

#[derive(Deserialize)]
struct ErrorBody {
	error: ErrorDetails,
}

#[derive(Deserialize)]
struct ErrorDetails {
	#[serde(default)]
	code: String,
	#[serde(default)]
	message: String,
}

impl ClientError {
	/// Map a non-success status and its (possibly non-JSON) body.
	pub fn from_status(status: u16, body: &[u8]) -> Self {
		let (code, message) = serde_json::from_slice::<ErrorBody>(body)
			.map(|b| (b.error.code, b.error.message))
			.unwrap_or_default();
		match status {
			401 => Self::Unauthorized,
			403 => Self::Forbidden { code },
			404 => Self::NotFound,
			412 => Self::PreconditionFailed,
			413 => Self::QuotaExceeded,
			// message match is the only signal the server gives for this case
			400 if code == "E-CORE-INVALID" && message.contains("exceeds maximum file size") => {
				Self::TooLarge
			}
			_ => Self::Http { status, code, message },
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn maps_server_errors() {
		let too_large =
			br#"{"error":{"code":"E-CORE-INVALID","message":"upload exceeds maximum file size"}}"#;
		assert!(matches!(ClientError::from_status(400, too_large), ClientError::TooLarge));
		let invalid = br#"{"error":{"code":"E-CORE-INVALID","message":"bad name"}}"#;
		assert!(
			matches!(ClientError::from_status(400, invalid), ClientError::Http { status: 400, code, .. } if code == "E-CORE-INVALID")
		);
		let cross = br#"{"error":{"code":"E-FILE-CROSSROOM","message":"x"}}"#;
		assert!(
			matches!(ClientError::from_status(403, cross), ClientError::Forbidden { code } if code == "E-FILE-CROSSROOM")
		);
		assert!(matches!(ClientError::from_status(413, b""), ClientError::QuotaExceeded));
		assert!(matches!(ClientError::from_status(412, b"oops"), ClientError::PreconditionFailed));
		assert!(matches!(
			ClientError::from_status(502, b"<html>"),
			ClientError::Http { status: 502, .. }
		));
	}
}
