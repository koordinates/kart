//! Opaque integer handle tables for objects passed across the C boundary.
//!
//! Each live `Repo` / `Dataset` is stored here and referenced from C by a `u64` id
//! (0 is never a valid id). Tables are `Mutex`-protected so the ABI is thread-safe.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};

use crate::dataset::Dataset;
use crate::repo::Repo;

pub struct Registry<T> {
    map: Mutex<HashMap<u64, T>>,
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
        self.map.lock().unwrap().insert(id, v);
        id
    }

    /// Run `f` against the stored value for `id`, if present. The table lock is held for
    /// the duration of `f`, so keep `f` short and do not re-enter the same registry.
    pub fn with<R>(&self, id: u64, f: impl FnOnce(&T) -> R) -> Option<R> {
        let guard = self.map.lock().unwrap();
        guard.get(&id).map(f)
    }

    /// Drop the value for `id` (no-op if absent).
    pub fn remove(&self, id: u64) {
        self.map.lock().unwrap().remove(&id);
    }
}

pub static REPOS: LazyLock<Registry<Repo>> = LazyLock::new(Registry::new);
pub static DATASETS: LazyLock<Registry<Dataset>> = LazyLock::new(Registry::new);
