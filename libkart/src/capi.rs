//! The C ABI. Every fallible function returns `int` rc: 0 = ok, -1 = error (message via
//! `kart_last_error`). Handles are `uint64_t` (0 invalid). Returned strings/bytes are
//! malloc'd by libkart and must be released by the caller with `kart_free`; a None/absent
//! result yields rc 0 with `*out == NULL` and `*out_len == 0`.

use std::ffi::CStr;
use std::os::raw::{c_char, c_int};

use crate::error::{last_error_ptr, set_last_error, Error, Result};
use crate::handle::{DATASETS, REPOS};
use crate::{dataset, feature, gpkg, tile};

// ---- small helpers ----------------------------------------------------------

fn ok() -> c_int {
    0
}

fn fail(e: Error) -> c_int {
    set_last_error(&e.to_string());
    -1
}

/// Decode a required, non-NULL C string argument as UTF-8.
unsafe fn cstr<'a>(p: *const c_char) -> Result<&'a str> {
    if p.is_null() {
        return Err(Error::Format("unexpected NULL string argument".into()));
    }
    CStr::from_ptr(p).to_str().map_err(Error::from)
}

/// View a (ptr, len) byte argument as a slice (empty if NULL/zero).
unsafe fn bytes<'a>(p: *const u8, n: usize) -> &'a [u8] {
    if p.is_null() || n == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(p, n)
    }
}

/// malloc + copy `data` into a caller-owned buffer. Empty/None -> (NULL, 0).
unsafe fn emit(data: Option<&[u8]>, out: *mut *mut u8, out_len: *mut usize) {
    match data {
        Some(b) if !b.is_empty() => {
            let p = libc::malloc(b.len()) as *mut u8;
            std::ptr::copy_nonoverlapping(b.as_ptr(), p, b.len());
            *out = p;
            *out_len = b.len();
        }
        _ => {
            *out = std::ptr::null_mut();
            *out_len = 0;
        }
    }
}

// ---- repo -------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn kart_repo_open(path: *const c_char, out_repo: *mut u64) -> c_int {
    let path = match cstr(path) {
        Ok(p) => p,
        Err(e) => return fail(e),
    };
    match crate::repo::Repo::open(path) {
        Ok(r) => {
            *out_repo = REPOS.insert(r);
            ok()
        }
        Err(e) => fail(e),
    }
}

#[no_mangle]
pub unsafe extern "C" fn kart_repo_free(repo: u64) {
    REPOS.remove(repo);
}

#[no_mangle]
pub unsafe extern "C" fn kart_repo_table_dataset_version(
    repo: u64,
    out_version: *mut c_int,
) -> c_int {
    match REPOS.with(repo, |r| r.table_dataset_version()) {
        Some(Ok(v)) => {
            *out_version = v;
            ok()
        }
        Some(Err(e)) => fail(e),
        None => fail(Error::NotFound("repo handle".into())),
    }
}

#[no_mangle]
pub unsafe extern "C" fn kart_repo_list_datasets(
    repo: u64,
    refish: *const c_char,
    out_json: *mut *mut u8,
    out_len: *mut usize,
) -> c_int {
    let refish = match cstr(refish) {
        Ok(s) => s,
        Err(e) => return fail(e),
    };
    let result = REPOS.with(repo, |r| {
        r.list_datasets(refish)
            .and_then(|paths| serde_json::to_vec(&paths).map_err(Error::from))
    });
    match result {
        Some(Ok(bytes_vec)) => {
            emit(Some(&bytes_vec), out_json, out_len);
            ok()
        }
        Some(Err(e)) => fail(e),
        None => fail(Error::NotFound("repo handle".into())),
    }
}

// ---- dataset ----------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn kart_dataset_open(
    repo: u64,
    refish: *const c_char,
    path: *const c_char,
    out_ds: *mut u64,
) -> c_int {
    let refish = match cstr(refish) {
        Ok(s) => s,
        Err(e) => return fail(e),
    };
    let path = match cstr(path) {
        Ok(s) => s,
        Err(e) => return fail(e),
    };
    let opened = REPOS.with(repo, |r| dataset::Dataset::open(r, refish, path));
    match opened {
        Some(Ok(ds)) => {
            *out_ds = DATASETS.insert(ds);
            ok()
        }
        Some(Err(e)) => fail(e),
        None => fail(Error::NotFound("repo handle".into())),
    }
}

#[no_mangle]
pub unsafe extern "C" fn kart_dataset_free(ds: u64) {
    DATASETS.remove(ds);
}

#[no_mangle]
pub unsafe extern "C" fn kart_dataset_type(
    ds: u64,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> c_int {
    match DATASETS.with(ds, |d| d.dataset_type.clone()) {
        Some(t) => {
            emit(Some(t.as_bytes()), out, out_len);
            ok()
        }
        None => fail(Error::NotFound("dataset handle".into())),
    }
}

#[no_mangle]
pub unsafe extern "C" fn kart_dataset_schema_json(
    ds: u64,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> c_int {
    match DATASETS.with(ds, |d| d.schema_json()) {
        Some(Ok(b)) => {
            emit(Some(&b), out, out_len);
            ok()
        }
        Some(Err(e)) => fail(e),
        None => fail(Error::NotFound("dataset handle".into())),
    }
}

#[no_mangle]
pub unsafe extern "C" fn kart_dataset_crs_wkt(
    ds: u64,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> c_int {
    match DATASETS.with(ds, |d| d.crs_wkt()) {
        Some(Ok(opt)) => {
            emit(opt.as_ref().map(|s| s.as_bytes()), out, out_len);
            ok()
        }
        Some(Err(e)) => fail(e),
        None => fail(Error::NotFound("dataset handle".into())),
    }
}

#[no_mangle]
pub unsafe extern "C" fn kart_dataset_meta_item(
    ds: u64,
    name: *const c_char,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> c_int {
    let name = match cstr(name) {
        Ok(s) => s,
        Err(e) => return fail(e),
    };
    match DATASETS.with(ds, |d| d.meta_item(name)) {
        Some(Ok(opt)) => {
            emit(opt.as_deref(), out, out_len);
            ok()
        }
        Some(Err(e)) => fail(e),
        None => fail(Error::NotFound("dataset handle".into())),
    }
}

// ---- feature / tile ---------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn kart_feature_geometry(
    ds: u64,
    blob: *const u8,
    blob_len: usize,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> c_int {
    let blob = bytes(blob, blob_len);
    match DATASETS.with(ds, |d| feature::feature_geometry(d, blob)) {
        Some(Ok(opt)) => {
            emit(opt.as_deref(), out, out_len);
            ok()
        }
        Some(Err(e)) => fail(e),
        None => fail(Error::NotFound("dataset handle".into())),
    }
}

#[no_mangle]
pub unsafe extern "C" fn kart_tile_summary_json(
    ds: u64,
    blob: *const u8,
    blob_len: usize,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> c_int {
    let blob = bytes(blob, blob_len);
    match DATASETS.with(ds, |d| tile::tile_summary_json(d, blob)) {
        Some(Ok(b)) => {
            emit(Some(&b), out, out_len);
            ok()
        }
        Some(Err(e)) => fail(e),
        None => fail(Error::NotFound("dataset handle".into())),
    }
}

// ---- gpkg geometry ----------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn kart_gpkg_is_empty(g: *const u8, n: usize, out: *mut c_int) -> c_int {
    match gpkg::is_empty(bytes(g, n)) {
        Ok(b) => {
            *out = b as c_int;
            ok()
        }
        Err(e) => fail(e),
    }
}

#[no_mangle]
pub unsafe extern "C" fn kart_gpkg_geometry_type(
    g: *const u8,
    n: usize,
    out: *mut c_int,
) -> c_int {
    match gpkg::geometry_type(bytes(g, n)) {
        Ok(t) => {
            *out = t;
            ok()
        }
        Err(e) => fail(e),
    }
}

/// Envelope into `out6` (caller-provided array of 6 doubles); `*out_count` is set to the
/// number of valid doubles (0 if no envelope, 4 for XY, 6 for XYZ).
#[no_mangle]
pub unsafe extern "C" fn kart_gpkg_envelope(
    g: *const u8,
    n: usize,
    only_2d: c_int,
    calc: c_int,
    out6: *mut f64,
    out_count: *mut c_int,
) -> c_int {
    match gpkg::envelope(bytes(g, n), only_2d != 0, calc != 0) {
        Ok(Some(v)) => {
            let count = v.len().min(6);
            for (i, x) in v.iter().take(count).enumerate() {
                *out6.add(i) = *x;
            }
            *out_count = count as c_int;
            ok()
        }
        Ok(None) => {
            *out_count = 0;
            ok()
        }
        Err(e) => fail(e),
    }
}

#[no_mangle]
pub unsafe extern "C" fn kart_gpkg_to_wkb(
    g: *const u8,
    n: usize,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> c_int {
    match gpkg::to_wkb(bytes(g, n)) {
        Ok(b) => {
            emit(Some(&b), out, out_len);
            ok()
        }
        Err(e) => fail(e),
    }
}

// ---- misc -------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn kart_last_error() -> *const c_char {
    last_error_ptr()
}

#[no_mangle]
pub unsafe extern "C" fn kart_free(ptr: *mut std::os::raw::c_void) {
    if !ptr.is_null() {
        libc::free(ptr);
    }
}
