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

#[cfg(test)]
mod tests {
    use super::*;
    use git2::{ObjectType, Tree};
    use std::ffi::CString;
    use std::process::Command;

    const POINTS_TGZ: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../tests/data/points.tgz");

    /// Extract a fixture tgz into a fresh temp dir, returning the repo root path.
    fn extract_fixture(tgz: &str, subdir: &str) -> std::path::PathBuf {
        crate::test_support::disable_owner_validation();
        let base = std::env::temp_dir().join(format!(
            "libkart-capitest-{}-{}",
            subdir,
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let status = Command::new("tar")
            .arg("xzf")
            .arg(tgz)
            .arg("-C")
            .arg(&base)
            .status()
            .expect("run tar");
        assert!(status.success(), "tar failed for {tgz}");
        base.join(subdir)
    }

    /// Consume a buffer-returning call's out-params: assert rc 0, copy the bytes out, and
    /// free the libkart-owned buffer. Returns None for the absent (NULL) case.
    unsafe fn read_out(rc: c_int, ptr: *mut u8, len: usize) -> Option<Vec<u8>> {
        assert_eq!(rc, 0, "expected rc 0, last_error: {}", last_error_str());
        if ptr.is_null() {
            assert_eq!(len, 0, "NULL buffer must have zero length");
            return None;
        }
        let v = std::slice::from_raw_parts(ptr, len).to_vec();
        kart_free(ptr as *mut std::os::raw::c_void);
        Some(v)
    }

    unsafe fn last_error_str() -> String {
        CStr::from_ptr(kart_last_error())
            .to_str()
            .unwrap()
            .to_string()
    }

    /// Find the first feature blob bytes under the dataset's inner `feature/` tree.
    fn first_feature_blob(repo: &crate::repo::Repo, dataset_path: &str) -> Vec<u8> {
        let root = repo.resolve_tree("HEAD").unwrap();
        let ds_entry = root
            .get_path(std::path::Path::new(dataset_path))
            .unwrap();
        let ds_tree = ds_entry.to_object(&repo.git).unwrap().peel_to_tree().unwrap();
        let inner = ds_tree
            .iter()
            .find(|e| e.name() == Some(".table-dataset"))
            .unwrap();
        let inner_tree = inner.to_object(&repo.git).unwrap().peel_to_tree().unwrap();
        let feat_entry = inner_tree.get_name("feature").unwrap();
        let feat_tree = feat_entry.to_object(&repo.git).unwrap().peel_to_tree().unwrap();

        let mut out: Option<Vec<u8>> = None;
        find_first_blob(repo, &feat_tree, &mut out);
        out.expect("no feature blob found")
    }

    fn find_first_blob(repo: &crate::repo::Repo, tree: &Tree<'_>, out: &mut Option<Vec<u8>>) {
        if out.is_some() {
            return;
        }
        for entry in tree.iter() {
            match entry.kind() {
                Some(ObjectType::Blob) => {
                    let obj = entry.to_object(&repo.git).unwrap();
                    if let Some(blob) = obj.as_blob() {
                        *out = Some(blob.content().to_vec());
                        return;
                    }
                }
                Some(ObjectType::Tree) => {
                    let obj = entry.to_object(&repo.git).unwrap();
                    if let Some(child) = obj.as_tree() {
                        find_first_blob(repo, child, out);
                        if out.is_some() {
                            return;
                        }
                    }
                }
                _ => {}
            }
        }
    }

    #[test]
    fn test_capi_full_flow() {
        let root = extract_fixture(POINTS_TGZ, "points");
        let path_c = CString::new(root.to_str().unwrap()).unwrap();
        let head = CString::new("HEAD").unwrap();

        unsafe {
            // ---- kart_repo_open ------------------------------------------------
            // Bogus path => rc -1 with a non-empty error.
            let bogus = CString::new("/no/such/kart/repo/here").unwrap();
            let mut bogus_handle: u64 = 0;
            let rc = kart_repo_open(bogus.as_ptr(), &mut bogus_handle);
            assert_eq!(rc, -1);
            assert!(!last_error_str().is_empty(), "expected an error message");

            // NULL path pointer => rc -1.
            let mut null_handle: u64 = 0;
            let rc = kart_repo_open(std::ptr::null(), &mut null_handle);
            assert_eq!(rc, -1);

            // Valid fixture => rc 0 and a non-zero handle.
            let mut repo: u64 = 0;
            let rc = kart_repo_open(path_c.as_ptr(), &mut repo);
            assert_eq!(rc, 0, "open failed: {}", last_error_str());
            assert_ne!(repo, 0);

            // ---- kart_repo_table_dataset_version -------------------------------
            let mut version: c_int = -1;
            let rc = kart_repo_table_dataset_version(repo, &mut version);
            assert_eq!(rc, 0);
            assert_eq!(version, 3);

            // ---- kart_repo_list_datasets ---------------------------------------
            let mut ptr: *mut u8 = std::ptr::null_mut();
            let mut len: usize = 0;
            let rc = kart_repo_list_datasets(repo, head.as_ptr(), &mut ptr, &mut len);
            let json = read_out(rc, ptr, len).expect("datasets buffer is non-NULL");
            let datasets: Vec<String> = serde_json::from_slice(&json).unwrap();
            assert!(
                datasets.contains(&"nz_pa_points_topo_150k".to_string()),
                "datasets: {datasets:?}"
            );

            // Unknown repo handle => rc -1 "not found: repo handle".
            let mut p2: *mut u8 = std::ptr::null_mut();
            let mut l2: usize = 0;
            let rc = kart_repo_list_datasets(99999, head.as_ptr(), &mut p2, &mut l2);
            assert_eq!(rc, -1);
            assert!(
                last_error_str().contains("not found: repo handle"),
                "got: {}",
                last_error_str()
            );

            // ---- kart_dataset_open ---------------------------------------------
            let ds_path = CString::new("nz_pa_points_topo_150k").unwrap();
            let mut ds: u64 = 0;
            let rc = kart_dataset_open(repo, head.as_ptr(), ds_path.as_ptr(), &mut ds);
            assert_eq!(rc, 0, "dataset open failed: {}", last_error_str());
            assert_ne!(ds, 0);

            // dataset_type
            let mut ptr: *mut u8 = std::ptr::null_mut();
            let mut len: usize = 0;
            let rc = kart_dataset_type(ds, &mut ptr, &mut len);
            let t = read_out(rc, ptr, len).expect("type buffer");
            assert_eq!(String::from_utf8(t).unwrap(), "table");

            // schema_json
            let mut ptr: *mut u8 = std::ptr::null_mut();
            let mut len: usize = 0;
            let rc = kart_dataset_schema_json(ds, &mut ptr, &mut len);
            let schema = read_out(rc, ptr, len).expect("schema buffer");
            let schema: serde_json::Value = serde_json::from_slice(&schema).unwrap();
            assert_eq!(schema["has_geometry"], true);

            // crs_wkt
            let mut ptr: *mut u8 = std::ptr::null_mut();
            let mut len: usize = 0;
            let rc = kart_dataset_crs_wkt(ds, &mut ptr, &mut len);
            let wkt = read_out(rc, ptr, len).expect("crs buffer");
            assert!(String::from_utf8(wkt).unwrap().starts_with("GEOGCS"));

            // meta_item: a MISSING key => rc 0 with absent (NULL/0) result.
            let missing = CString::new("does-not-exist").unwrap();
            let mut ptr: *mut u8 = std::ptr::null_mut();
            let mut len: usize = 0;
            let rc = kart_dataset_meta_item(ds, missing.as_ptr(), &mut ptr, &mut len);
            assert_eq!(read_out(rc, ptr, len), None);

            // ---- feature path --------------------------------------------------
            // Fetch a raw feature blob via git2, decode its geometry through the ABI.
            let repo_rust = crate::repo::Repo::open(root.to_str().unwrap()).unwrap();
            let blob = first_feature_blob(&repo_rust, "nz_pa_points_topo_150k");

            let mut ptr: *mut u8 = std::ptr::null_mut();
            let mut len: usize = 0;
            let rc =
                kart_feature_geometry(ds, blob.as_ptr(), blob.len(), &mut ptr, &mut len);
            let geom = read_out(rc, ptr, len).expect("geometry buffer");
            assert!(
                geom.starts_with(b"GP"),
                "geometry should start with GPKG magic, got {:?}",
                &geom[..geom.len().min(4)]
            );

            // GPKG -> WKB.
            let mut ptr: *mut u8 = std::ptr::null_mut();
            let mut len: usize = 0;
            let rc = kart_gpkg_to_wkb(geom.as_ptr(), geom.len(), &mut ptr, &mut len);
            let wkb = read_out(rc, ptr, len).expect("wkb buffer");
            assert!(!wkb.is_empty());

            // GPKG geometry type code is non-negative.
            let mut gtype: c_int = -1;
            let rc = kart_gpkg_geometry_type(geom.as_ptr(), geom.len(), &mut gtype);
            assert_eq!(rc, 0);
            assert!(gtype >= 0, "geometry type code {gtype}");

            // ---- memory / handle hygiene --------------------------------------
            // kart_free(NULL) is a no-op.
            kart_free(std::ptr::null_mut());
            // freeing unknown handles is a no-op.
            kart_repo_free(99999);
            kart_dataset_free(99999);

            // After freeing the dataset handle, calls return rc -1 "not found".
            kart_dataset_free(ds);
            let mut ptr: *mut u8 = std::ptr::null_mut();
            let mut len: usize = 0;
            let rc = kart_dataset_type(ds, &mut ptr, &mut len);
            assert_eq!(rc, -1);
            assert!(last_error_str().contains("not found"), "got: {}", last_error_str());

            // After freeing the repo handle, calls return rc -1 "not found".
            kart_repo_free(repo);
            let mut version: c_int = -1;
            let rc = kart_repo_table_dataset_version(repo, &mut version);
            assert_eq!(rc, -1);
            assert!(last_error_str().contains("not found"), "got: {}", last_error_str());
        }

        let _ = std::fs::remove_dir_all(root.parent().unwrap());
    }
}
