//! libkart - a dependency-light native library over Kart's on-disk repository format.
//!
//! Two layers:
//!  * the pure-Rust core (modules `repo`, `dataset`, `feature`, `tile`, `gpkg`) which
//!    reads Kart's format and returns plain Rust data, and
//!  * the C ABI (`capi`) which marshals that core across a stable `extern "C"` boundary
//!    so external processes (initially `cave`, a Python service) can call it in-process
//!    instead of shelling out to `kart ext-run`.
//!
//! Design rule: the boundary only passes plain bytes / strings / scalars / JSON. libkart
//! needs only libgit2 + a msgpack decoder + Kart format knowledge; it does NOT depend on
//! Python, GDAL, S2 or PDAL. Callers reconstruct geometry/CRS objects on their own side
//! from the GPKG/WKB bytes and WKT strings libkart returns.

// Scaffold-wide allowances; tighten as modules are implemented.
#![allow(dead_code, unused_variables)]

mod capi;
mod error;
mod handle;

pub mod dataset;
pub mod feature;
pub mod gpkg;
pub mod repo;
pub mod tile;

pub use error::{Error, Result};

/// Test-only helpers shared across module test suites.
#[cfg(test)]
pub(crate) mod test_support {
    /// Disable libgit2's repository-ownership check for the test process.
    ///
    /// Tests extract fixture repos under the system temp dir and open them with
    /// libgit2. In CI containers that run as root, libgit2 rejects those repos with
    /// "repository path ... is not owned by current user" (error code -36). The
    /// production Python test suite works around this by writing `safe.directory = *`
    /// to a temp git config (see tests/conftest.py); the cargo tests have no such
    /// config, so we disable the ownership check directly. Idempotent and process-global.
    pub(crate) fn disable_owner_validation() {
        // SAFETY: sets a process-global libgit2 option; safe to call repeatedly.
        unsafe {
            let _ = git2::opts::set_verify_owner_validation(false);
        }
    }
}
