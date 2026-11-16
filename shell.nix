{ pkgs ? import <nixpkgs> {} }:
	pkgs.mkShell {
		nativeBuildInputs = with pkgs.buildPackages; [
			cargo rustc rustfmt clippy
			pkg-config fuse3 dbus
		];
		# mkShell sets CC/CXX/AR/PKG_CONFIG*, which C build scripts fingerprint;
		# a separate target dir stops rebuilds when switching shells.
		shellHook = ''
			export CARGO_TARGET_DIR="$PWD/target/nix-shell"
		'';
	}

# vim: ts=4
