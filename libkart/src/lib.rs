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
