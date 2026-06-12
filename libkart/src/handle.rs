//! Opaque integer handle tables for objects passed across the C boundary.
//!
//! Each live `Repo` / `Dataset` is stored here and referenced from C by a `u64` id
//! (0 is never a valid id). The table itself is `Mutex`-protected, and each stored
//! value has its own `Mutex` so the ABI is thread-safe: concurrent calls on
//! *different* handles run in parallel, while calls on the *same* handle are
//! serialized (required since the wrapped types, e.g. `git2::Repository`, are
//! `Send` but not `Sync`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

use crate::dataset::Dataset;
use crate::repo::Repo;

pub struct Registry<T> {
    map: Mutex<HashMap<u64, Arc<Mutex<T>>>>,
    next: AtomicU64,
}

impl<T> Registry<T> {
    pub fn new() -> Self {
        Registry {
            map: Mutex::new(HashMap::new()),
            next: AtomicU64::new(1),
        }
    }

    /// Store `v`, returning its new handle id.
    pub fn insert(&self, v: T) -> u64 {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        self.map
            .lock()
            .unwrap()
            .insert(id, Arc::new(Mutex::new(v)));
        id
    }

    /// Run `f` against the stored value for `id`, if present. Only the per-object lock
    /// is held while `f` runs (the table lock is released first), so calls on other
    /// handles proceed in parallel. Do not re-enter the same handle from within `f`.
    pub fn with<R>(&self, id: u64, f: impl FnOnce(&T) -> R) -> Option<R> {
        let obj = self.map.lock().unwrap().get(&id).cloned()?;
        let guard = obj.lock().unwrap();
        Some(f(&guard))
    }

    /// Drop the value for `id` (no-op if absent).
    pub fn remove(&self, id: u64) {
        self.map.lock().unwrap().remove(&id);
    }
}

pub static REPOS: LazyLock<Registry<Repo>> = LazyLock::new(Registry::new);
pub static DATASETS: LazyLock<Registry<Dataset>> = LazyLock::new(Registry::new);
