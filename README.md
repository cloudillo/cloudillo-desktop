<p align="center">
  <img src="https://cloudillo.org/images/logo-full.svg" alt="Cloudillo" width="400">
</p>

<h3 align="center">Your files, mounted on your desktop</h3>

## What is this?

The desktop client for [Cloudillo](https://github.com/cloudillo/cloudillo-rs), a federated,
self-hosted collaboration platform. The `cloudillo` binary mounts a Cloudillo identity's files
as a local folder (FUSE, Linux), so any app can open and save them directly.

- **QR pairing** — log in by scanning a code with your phone; no password typed on the desktop
- **Temporary or persistent mounts** — a throwaway mount that revokes its key on Ctrl-C, or a
  daemon-owned mount whose key lives in the OS keyring
- **Offline cache** — `off`, `on-demand` (cache on open) or `pinned` (keep everything cached)
- **Versions** — list and restore earlier versions of any file
- **Conflict-safe writes** — concurrent edits become conflict copies, never silent overwrites

## Building

Requires Rust 1.89+ (edition 2024) and FUSE 3. Build inside the provided nix shell:

```bash
nix-shell --run 'cargo build --release'
```

The binary lands in `target/nix-shell/release/cloudillo`.

## Usage

```bash
# Temporary mount: pair by QR, runs in the foreground until Ctrl-C
cloudillo mount alice.example.com ~/Cloudillo

# Persistent mount, owned by the background daemon, with an offline cache
cloudillo mount alice.example.com ~/Cloudillo --persist --mode on-demand

cloudillo status                         # list the daemon's mounts
cloudillo pin ~/Cloudillo/Projects       # keep a folder available offline
cloudillo versions ~/Cloudillo/notes.md  # list earlier versions
cloudillo restore ~/Cloudillo/notes.md <version-id> [--as-copy]
cloudillo unmount ~/Cloudillo
cloudillo logout alice.example.com       # revoke this machine's key
```

Run `cloudillo --help` (or `cloudillo <command> --help`) for every option.

### Configuration

Persistent mounts are stored in `$XDG_CONFIG_HOME/cloudillo/mounts.toml` and caches in
`$XDG_CACHE_HOME/cloudillo/<mount-id>/`.

For `on-demand` and `pinned` mounts, each mount entry can also set:

- `ignore`: extra file name patterns (on top of built-in editor/OS litter such as `.*.swp`,
  `~$*`, `.DS_Store`). New files matching them are never uploaded.
- `max_file_size_mb` (default 50): files larger than this, whether new or grown past it, are
  not uploaded; the server keeps the last version that fit.

Such files still appear in the mount but exist only on this machine, and `cloudillo status`
counts them.

The top-level `cache_max_mb` (default 10240, i.e. 10 GiB) caps each mount's local cache
separately. When a cache grows past it, the least recently used files are dropped first (they
are downloaded again on next open); open and pinned files are always kept.

Set `RUST_LOG=debug` for verbose logs.

## Architecture

| Crate | Purpose |
|-------|---------|
| `cloudillo-desktop-core` | API client, pairing, credentials, tree model, sync, cache, uploads (platform-independent) |
| `cloudillo-desktop-fs-fuse` | FUSE frontend (Linux) |
| `cloudillo-desktop-cli` | The `cloudillo` binary and mount daemon |

The codebase enforces `#![forbid(unsafe_code)]`.

## License

[Mozilla Public License 2.0](LICENSE)
