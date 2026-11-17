// SPDX-FileCopyrightText: Szilárd Hajba
// SPDX-License-Identifier: MPL-2.0

//! Terminal QR code for the pairing payload.

use qrcode::QrCode;
use qrcode::render::unicode::Dense1x2;

/// Print `payload` (`cloudillo:qr-pair:<sessionId>:<code>`) as Unicode half-blocks, plus `url`
/// (the same approval screen in a browser) and the verification `code` to type there.
pub fn print(payload: &str, url: &str, code: &str) {
	match QrCode::new(payload) {
		// inverted colours: light modules on a dark terminal scan as the dark ones
		Ok(code) => eprintln!(
			"{}",
			code.render::<Dense1x2>()
				.dark_color(Dense1x2::Light)
				.light_color(Dense1x2::Dark)
				.quiet_zone(true)
				.build()
		),
		Err(e) => eprintln!("cannot render QR code: {e}"),
	}
	eprintln!("Scan with the Cloudillo app on your phone, or open in a browser:\n  {url}");
	eprintln!("Verification code: {}", group(code));
	eprintln!("Waiting for approval (expires in 5 min, Ctrl-C to cancel)…");
}

/// `123456` → `123 456`, for reading aloud / typing.
fn group(code: &str) -> String {
	code.split_at_checked(3)
		.map_or_else(|| code.to_owned(), |(a, b)| format!("{a} {b}"))
}
