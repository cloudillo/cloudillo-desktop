//! Cloudillo desktop client core. Free of FUSE types: frontends build on top of it.
#![forbid(unsafe_code)]

pub mod api;
pub mod atomic;
pub mod cache;
pub mod config;
pub mod conflict;
pub mod credentials;
pub mod db;
pub mod error;
pub mod feed;
pub mod ignore;
pub mod ipc;
pub mod journal;
pub mod launcher;
pub mod names;
pub mod pair;
pub mod pin;
pub mod seal;
pub mod session;
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub mod stub;
pub mod sync;
pub mod tree;
pub mod upload;

pub use cache::BlobCache;
pub use config::{Config, MountEntry};
pub use credentials::Credentials;
pub use db::{Db, EntryKind, EntryRow};
pub use error::{ClientError, Result};
pub use feed::FeedPoller;
pub use journal::{Journal, OpKind, PendingOp};
pub use pair::pair;
pub use session::Session;
pub use sync::SyncWorker;
pub use tree::{Child, ChildKind, DbTree, Layout, RemoteTree};
pub use upload::{Replaced, Uploader};

/// Lock `m`, ignoring poison (a panicked holder left consistent-enough state).
pub fn lock<T>(m: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
	m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}
