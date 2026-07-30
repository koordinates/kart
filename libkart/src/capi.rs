//! The C ABI. Every fallible function returns `int` rc: 0 = ok, -1 = error (message via
//! `kart_last_error`). Handles are `uint64_t` (0 invalid). Returned strings/bytes are
//! malloc'd by libkart and must be released by the caller with `kart_free`; a None/absent
//! result yields rc 0 with `*out == NULL` and `*out_len == 0`.

use std::collections::HashMap;
use std::ffi::CStr;
use std::os::raw::{c_char, c_int};

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use git2::Oid;

use crate::error::{last_error_ptr, set_last_error, Error, Result};
use crate::handle::{DATASETS, REPOS};
use crate::query::{find_feature_blobs, FeatureQuery};
use crate::rewrite::rewrite_history;
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

/// malloc + copy `data` into a caller-owned buffer, returning the function's rc.
/// Empty/None -> (NULL, 0) with rc 0; malloc failure -> (NULL, 0) with rc -1.
unsafe fn emit(data: Option<&[u8]>, out: *mut *mut u8, out_len: *mut usize) -> c_int {
    match data {
        Some(b) if !b.is_empty() => {
            let p = libc::malloc(b.len()) as *mut u8;
            if p.is_null() {
                *out = std::ptr::null_mut();
                *out_len = 0;
                return fail(Error::Format(format!(
                    "out of memory allocating {} byte out-buffer",
                    b.len()
                )));
            }
            std::ptr::copy_nonoverlapping(b.as_ptr(), p, b.len());
            *out = p;
            *out_len = b.len();
            ok()
        }
        _ => {
            *out = std::ptr::null_mut();
            *out_len = 0;
            ok()
        }
    }
}

/// Convert one JSON primary-key value to msgpack: integers and strings only.
fn json_pk_to_msgpack(v: &serde_json::Value) -> Result<rmpv::Value> {
    match v {
        serde_json::Value::Number(n) => n.as_i64().map(rmpv::Value::from).ok_or_else(|| {
            Error::Format(format!("pk value must be an integer or string, got {v}"))
        }),
        serde_json::Value::String(s) => Ok(rmpv::Value::from(s.as_str())),
        _ => Err(Error::Format(format!(
            "pk value must be an integer or string, got {v}"
        ))),
    }
}

/// Parse a JSON array of primary-key values (e.g. `[3]` or `["abc"]`) to msgpack values.
fn parse_pk_values(json: &str) -> Result<Vec<rmpv::Value>> {
    let parsed: serde_json::Value = serde_json::from_str(json)?;
    let arr = parsed
        .as_array()
        .ok_or_else(|| Error::Format(format!("pk values must be a JSON array, got {parsed}")))?;
    arr.iter().map(json_pk_to_msgpack).collect()
}

/// Parse a JSON object holding column-name -> value pairs (updates or filter).
fn parse_json_object(json: &str, what: &str) -> Result<serde_json::Map<String, serde_json::Value>> {
    let parsed: serde_json::Value = serde_json::from_str(json)?;
    match parsed {
        serde_json::Value::Object(map) => Ok(map),
        other => Err(Error::Format(format!(
            "{what} must be a JSON object, got {other}"
        ))),
    }
}

/// Parse a 40-hex git object id.
fn parse_oid(hex: &str) -> Result<Oid> {
    if hex.len() != 40 {
        return Err(Error::Format(format!(
            "expected a 40-hex object id, got '{hex}'"
        )));
    }
    Oid::from_str(hex).map_err(|_| Error::Format(format!("invalid object id '{hex}'")))
}

/// Parse a JSON array of 40-hex commit oids.
fn parse_commits(json: &str) -> Result<Vec<Oid>> {
    let parsed: serde_json::Value = serde_json::from_str(json)?;
    let arr = parsed
        .as_array()
        .ok_or_else(|| Error::Format(format!("commits must be a JSON array, got {parsed}")))?;
    arr.iter()
        .map(|v| {
            let hex = v
                .as_str()
                .ok_or_else(|| Error::Format(format!("commit must be a 40-hex string, got {v}")))?;
            parse_oid(hex)
        })
        .collect()
}

/// Parse a feature query: a JSON object with exactly one of "pk" (a JSON array of pk
/// values) or "filter" (a JSON object of column-name -> value).
fn parse_feature_query(json: &str) -> Result<FeatureQuery> {
    let mut map = parse_json_object(json, "query")?;
    if map.len() != 1 {
        return Err(Error::Format(format!(
            "query must be an object with exactly one of \"pk\" or \"filter\", got {}",
            serde_json::Value::Object(map)
        )));
    }
    let (key, value) = map.iter_mut().next().expect("len checked above");
    match key.as_str() {
        "pk" => {
            let arr = value.as_array().ok_or_else(|| {
                Error::Format(format!("query \"pk\" must be a JSON array, got {value}"))
            })?;
            Ok(FeatureQuery::Pk(
                arr.iter().map(json_pk_to_msgpack).collect::<Result<_>>()?,
            ))
        }
        "filter" => match value.take() {
            serde_json::Value::Object(filter) => Ok(FeatureQuery::Filter(filter)),
            other => Err(Error::Format(format!(
                "query \"filter\" must be a JSON object, got {other}"
            ))),
        },
        other => Err(Error::Format(format!(
            "query must be an object with exactly one of \"pk\" or \"filter\", got key \"{other}\""
        ))),
    }
}

/// Parse blob substitutions: a JSON object of 40-hex old blob oid -> base64 new content.
fn parse_substitutions(json: &str) -> Result<HashMap<Oid, Vec<u8>>> {
    let map = parse_json_object(json, "substitutions")?;
    let mut subs = HashMap::with_capacity(map.len());
    for (hex, value) in &map {
        let oid = parse_oid(hex)?;
        let b64 = value.as_str().ok_or_else(|| {
            Error::Format(format!(
                "substitution for '{hex}' must be a base64 string, got {value}"
            ))
        })?;
        let content = BASE64.decode(b64).map_err(|e| {
            Error::Format(format!("substitution for '{hex}' is not valid base64: {e}"))
        })?;
        subs.insert(oid, content);
    }
    Ok(subs)
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
        Some(Ok(bytes_vec)) => emit(Some(&bytes_vec), out_json, out_len),
        Some(Err(e)) => fail(e),
        None => fail(Error::NotFound("repo handle".into())),
    }
}

/// Find feature blobs matching a query at each of the given commits. `commits_json` is a
/// JSON array of 40-hex commit oids; `query_json` is `{"pk": [..]}` or `{"filter": {col:
/// val}}`. Out: JSON `[{"commit": hex, "path": "...", "oid": hex}, ...]` (path is
/// repo-root-relative).
#[no_mangle]
pub unsafe extern "C" fn kart_repo_find_feature_blobs(
    repo: u64,
    commits_json: *const c_char,
    dataset_path: *const c_char,
    query_json: *const c_char,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> c_int {
    let commits = match cstr(commits_json).and_then(parse_commits) {
        Ok(c) => c,
        Err(e) => return fail(e),
    };
    let dataset_path = match cstr(dataset_path) {
        Ok(s) => s,
        Err(e) => return fail(e),
    };
    let query = match cstr(query_json).and_then(parse_feature_query) {
        Ok(q) => q,
        Err(e) => return fail(e),
    };
    let result = REPOS.with(repo, |r| {
        let hits = find_feature_blobs(r, &commits, dataset_path, &query)?;
        let json: Vec<serde_json::Value> = hits
            .iter()
            .map(|h| {
                serde_json::json!({
                    "commit": h.commit.to_string(),
                    "path": h.path,
                    "oid": h.blob_oid.to_string(),
                })
            })
            .collect();
        serde_json::to_vec(&json).map_err(Error::from)
    });
    match result {
        Some(Ok(b)) => emit(Some(&b), out, out_len),
        Some(Err(e)) => fail(e),
        None => fail(Error::NotFound("repo handle".into())),
    }
}

/// Rewrite history substituting blobs. `substitutions_json` is a JSON object of
/// `{"<40-hex old blob oid>": "<base64 new content>"}`; `backup_ref_prefix` is nullable
/// (non-NULL prefixes must start with "refs/"). Out: JSON `{"commits_rewritten": n,
/// "blobs_written": n, "refs_updated": [...], "backup_refs": [...]}`.
#[no_mangle]
pub unsafe extern "C" fn kart_repo_rewrite_history(
    repo: u64,
    substitutions_json: *const c_char,
    backup_ref_prefix: *const c_char,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> c_int {
    let substitutions = match cstr(substitutions_json).and_then(parse_substitutions) {
        Ok(s) => s,
        Err(e) => return fail(e),
    };
    // NULL prefix means "no backup refs".
    let prefix = if backup_ref_prefix.is_null() {
        None
    } else {
        match cstr(backup_ref_prefix) {
            Ok(s) => Some(s),
            Err(e) => return fail(e),
        }
    };
    let result = REPOS.with(repo, |r| {
        let stats = rewrite_history(r, &substitutions, prefix)?;
        let json = serde_json::json!({
            "commits_rewritten": stats.commits_rewritten,
            "blobs_written": stats.blobs_written,
            "refs_updated": stats.refs_updated,
            "backup_refs": stats.backup_refs,
        });
        serde_json::to_vec(&json).map_err(Error::from)
    });
    match result {
        Some(Ok(b)) => emit(Some(&b), out, out_len),
        Some(Err(e)) => fail(e),
        None => fail(Error::NotFound("repo handle".into())),
    }
}

/// Raw content of the blob with the given 40-hex oid (e.g. to read generated-pks.json).
#[no_mangle]
pub unsafe extern "C" fn kart_repo_blob(
    repo: u64,
    oid_hex: *const c_char,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> c_int {
    let oid = match cstr(oid_hex).and_then(parse_oid) {
        Ok(o) => o,
        Err(e) => return fail(e),
    };
    let result = REPOS.with(repo, |r| {
        r.git
            .find_blob(oid)
            .map(|b| b.content().to_vec())
            .map_err(Error::from)
    });
    match result {
        Some(Ok(b)) => emit(Some(&b), out, out_len),
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
        Some(t) => emit(Some(t.as_bytes()), out, out_len),
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
        Some(Ok(b)) => emit(Some(&b), out, out_len),
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
        Some(Ok(opt)) => emit(opt.as_ref().map(|s| s.as_bytes()), out, out_len),
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
        Some(Ok(opt)) => emit(opt.as_deref(), out, out_len),
        Some(Err(e)) => fail(e),
        None => fail(Error::NotFound("dataset handle".into())),
    }
}

/// Path of the feature with the given primary key values, relative to the dataset dir.
/// `pk_values_json` is a JSON array, e.g. `[3]` or `["abc"]` (integers and strings only).
#[no_mangle]
pub unsafe extern "C" fn kart_dataset_feature_path(
    ds: u64,
    pk_values_json: *const c_char,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> c_int {
    let pk_values = match cstr(pk_values_json).and_then(parse_pk_values) {
        Ok(v) => v,
        Err(e) => return fail(e),
    };
    match DATASETS.with(ds, |d| d.feature_path(&pk_values)) {
        Some(Ok(p)) => emit(Some(p.as_bytes()), out, out_len),
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
        Some(Ok(opt)) => emit(opt.as_deref(), out, out_len),
        Some(Err(e)) => fail(e),
        None => fail(Error::NotFound("dataset handle".into())),
    }
}

/// Copy of a feature blob with the values of the named columns replaced. `updates_json`
/// is a JSON object of `{column_name: new_value}`. Out: the new blob bytes.
#[no_mangle]
pub unsafe extern "C" fn kart_feature_update_blob(
    ds: u64,
    blob: *const u8,
    blob_len: usize,
    updates_json: *const c_char,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> c_int {
    let updates = match cstr(updates_json).and_then(|s| parse_json_object(s, "updates")) {
        Ok(m) => m,
        Err(e) => return fail(e),
    };
    let blob = bytes(blob, blob_len);
    match DATASETS.with(ds, |d| feature::update_feature_blob(d, blob, &updates)) {
        Some(Ok(b)) => emit(Some(&b), out, out_len),
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
        Some(Ok(b)) => emit(Some(&b), out, out_len),
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
pub unsafe extern "C" fn kart_gpkg_geometry_type(g: *const u8, n: usize, out: *mut c_int) -> c_int {
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
        Ok(b) => emit(Some(&b), out, out_len),
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
    const EDITING_TGZ: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../tests/data/editing.tgz");

    /// Extract a fixture tgz into a fresh temp dir, returning the repo root path.
    /// `label` is a per-test tag so parallel tests on the same fixture don't collide.
    fn extract_fixture(tgz: &str, subdir: &str, label: &str) -> std::path::PathBuf {
        crate::test_support::disable_owner_validation();
        let base = std::env::temp_dir().join(format!(
            "libkart-capitest-{}-{}-{}",
            label,
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
        let ds_entry = root.get_path(std::path::Path::new(dataset_path)).unwrap();
        let ds_tree = ds_entry
            .to_object(&repo.git)
            .unwrap()
            .peel_to_tree()
            .unwrap();
        let inner = ds_tree
            .iter()
            .find(|e| e.name() == Some(".table-dataset"))
            .unwrap();
        let inner_tree = inner.to_object(&repo.git).unwrap().peel_to_tree().unwrap();
        let feat_entry = inner_tree.get_name("feature").unwrap();
        let feat_tree = feat_entry
            .to_object(&repo.git)
            .unwrap()
            .peel_to_tree()
            .unwrap();

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
        let root = extract_fixture(POINTS_TGZ, "points", "flow");
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
            let rc = kart_feature_geometry(ds, blob.as_ptr(), blob.len(), &mut ptr, &mut len);
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
            assert!(
                last_error_str().contains("not found"),
                "got: {}",
                last_error_str()
            );

            // After freeing the repo handle, calls return rc -1 "not found".
            kart_repo_free(repo);
            let mut version: c_int = -1;
            let rc = kart_repo_table_dataset_version(repo, &mut version);
            assert_eq!(rc, -1);
            assert!(
                last_error_str().contains("not found"),
                "got: {}",
                last_error_str()
            );
        }

        let _ = std::fs::remove_dir_all(root.parent().unwrap());
    }

    /// Open the editing fixture via the ABI: (root, repo handle, dataset handle,
    /// [head, head~1] commit hex). The fixture has one branch `main` with two commits
    /// over dataset `editing` (int pk `id`, text `value`); pk 3 has value "c" at both.
    unsafe fn open_editing_capi(label: &str) -> (std::path::PathBuf, u64, u64, [String; 2]) {
        let root = extract_fixture(EDITING_TGZ, "editing", label);
        let path_c = CString::new(root.to_str().unwrap()).unwrap();
        let mut repo: u64 = 0;
        let rc = kart_repo_open(path_c.as_ptr(), &mut repo);
        assert_eq!(rc, 0, "open failed: {}", last_error_str());

        let head = CString::new("HEAD").unwrap();
        let ds_path = CString::new("editing").unwrap();
        let mut ds: u64 = 0;
        let rc = kart_dataset_open(repo, head.as_ptr(), ds_path.as_ptr(), &mut ds);
        assert_eq!(rc, 0, "dataset open failed: {}", last_error_str());

        let git = crate::repo::Repo::open(root.to_str().unwrap()).unwrap();
        let head_oid = git.git.revparse_single("HEAD").unwrap().id().to_string();
        let prev_oid = git.git.revparse_single("HEAD~1").unwrap().id().to_string();
        (root, repo, ds, [head_oid, prev_oid])
    }

    /// Call a buffer-returning ABI function, asserting rc 0 and a non-NULL result.
    unsafe fn call_out(f: impl FnOnce(*mut *mut u8, *mut usize) -> c_int) -> Vec<u8> {
        let mut ptr: *mut u8 = std::ptr::null_mut();
        let mut len: usize = 0;
        let rc = f(&mut ptr, &mut len);
        read_out(rc, ptr, len).expect("non-NULL out buffer")
    }

    /// Call a buffer-returning ABI function, asserting rc -1 (out-params untouched
    /// semantics aside, the buffer must not need freeing) and a non-empty last error.
    unsafe fn call_err(f: impl FnOnce(*mut *mut u8, *mut usize) -> c_int) -> String {
        let mut ptr: *mut u8 = std::ptr::null_mut();
        let mut len: usize = 0;
        let rc = f(&mut ptr, &mut len);
        assert_eq!(rc, -1, "expected rc -1");
        let msg = last_error_str();
        assert!(!msg.is_empty(), "expected an error message");
        msg
    }

    #[test]
    fn test_capi_feature_path() {
        let (root, repo, ds, _commits) = unsafe { open_editing_capi("featpath") };
        unsafe {
            // Happy path: pk 3 -> dataset-relative feature path.
            let pks = CString::new("[3]").unwrap();
            let path = call_out(|o, n| kart_dataset_feature_path(ds, pks.as_ptr(), o, n));
            assert_eq!(
                String::from_utf8(path).unwrap(),
                ".table-dataset/feature/A/A/A/A/kQM="
            );

            // NULL pk json => rc -1.
            call_err(|o, n| kart_dataset_feature_path(ds, std::ptr::null(), o, n));
            // Invalid JSON => rc -1.
            let bad = CString::new("not json").unwrap();
            call_err(|o, n| kart_dataset_feature_path(ds, bad.as_ptr(), o, n));
            // Non-array JSON => rc -1.
            let obj = CString::new("{}").unwrap();
            call_err(|o, n| kart_dataset_feature_path(ds, obj.as_ptr(), o, n));
            // Float pk value => rc -1 (only integers and strings are supported).
            let float = CString::new("[1.5]").unwrap();
            let msg = call_err(|o, n| kart_dataset_feature_path(ds, float.as_ptr(), o, n));
            assert!(msg.contains("integer or string"), "got: {msg}");
            // Unknown dataset handle => rc -1 "not found".
            let pks2 = CString::new("[3]").unwrap();
            let msg = call_err(|o, n| kart_dataset_feature_path(99999, pks2.as_ptr(), o, n));
            assert!(msg.contains("not found"), "got: {msg}");

            kart_dataset_free(ds);
            kart_repo_free(repo);
        }
        let _ = std::fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn test_capi_find_blobs_and_repo_blob_and_update() {
        let (root, repo, ds, [head_hex, prev_hex]) = unsafe { open_editing_capi("findupd") };
        let ds_path = CString::new("editing").unwrap();
        unsafe {
            // ---- kart_repo_find_feature_blobs: pk query over both commits --------
            let commits = CString::new(format!("[\"{head_hex}\", \"{prev_hex}\"]")).unwrap();
            let query = CString::new("{\"pk\": [3]}").unwrap();
            let out = call_out(|o, n| {
                kart_repo_find_feature_blobs(
                    repo,
                    commits.as_ptr(),
                    ds_path.as_ptr(),
                    query.as_ptr(),
                    o,
                    n,
                )
            });
            let hits: serde_json::Value = serde_json::from_slice(&out).unwrap();
            let hits = hits.as_array().unwrap();
            assert_eq!(hits.len(), 2);
            assert_eq!(hits[0]["commit"], head_hex.as_str());
            assert_eq!(hits[1]["commit"], prev_hex.as_str());
            for hit in hits {
                assert_eq!(hit["path"], "editing/.table-dataset/feature/A/A/A/A/kQM=");
            }
            let blob_hex = hits[0]["oid"].as_str().unwrap().to_string();
            assert_eq!(blob_hex.len(), 40);

            // A filter query finds the same feature (pk 3 has value "c").
            let filter_q = CString::new("{\"filter\": {\"value\": \"c\"}}").unwrap();
            let out2 = call_out(|o, n| {
                kart_repo_find_feature_blobs(
                    repo,
                    commits.as_ptr(),
                    ds_path.as_ptr(),
                    filter_q.as_ptr(),
                    o,
                    n,
                )
            });
            assert_eq!(out, out2);

            // ---- kart_repo_blob round-trip ---------------------------------------
            let blob_hex_c = CString::new(blob_hex.clone()).unwrap();
            let content = call_out(|o, n| kart_repo_blob(repo, blob_hex_c.as_ptr(), o, n));
            let git = crate::repo::Repo::open(root.to_str().unwrap()).unwrap();
            let expected = git
                .git
                .find_blob(git2::Oid::from_str(&blob_hex).unwrap())
                .unwrap()
                .content()
                .to_vec();
            assert_eq!(content, expected);

            // ---- kart_feature_update_blob ----------------------------------------
            let updates = CString::new("{\"value\": \"updated!\"}").unwrap();
            let new_blob = call_out(|o, n| {
                kart_feature_update_blob(
                    ds,
                    content.as_ptr(),
                    content.len(),
                    updates.as_ptr(),
                    o,
                    n,
                )
            });
            assert_ne!(new_blob, content);
            let (_, values) = crate::feature::decode_feature(&new_blob).unwrap();
            assert_eq!(values, vec![rmpv::Value::from("updated!")]);

            // ---- error paths -----------------------------------------------------
            // find: query with both keys / neither / bad commits / bad hex.
            for bad_query in ["{\"pk\": [3], \"filter\": {}}", "{}", "[]", "{\"nope\": 1}"] {
                let q = CString::new(bad_query).unwrap();
                call_err(|o, n| {
                    kart_repo_find_feature_blobs(
                        repo,
                        commits.as_ptr(),
                        ds_path.as_ptr(),
                        q.as_ptr(),
                        o,
                        n,
                    )
                });
            }
            let bad_commits = CString::new("[\"zzzz\"]").unwrap();
            call_err(|o, n| {
                kart_repo_find_feature_blobs(
                    repo,
                    bad_commits.as_ptr(),
                    ds_path.as_ptr(),
                    query.as_ptr(),
                    o,
                    n,
                )
            });
            let not_array = CString::new("{}").unwrap();
            call_err(|o, n| {
                kart_repo_find_feature_blobs(
                    repo,
                    not_array.as_ptr(),
                    ds_path.as_ptr(),
                    query.as_ptr(),
                    o,
                    n,
                )
            });
            call_err(|o, n| {
                kart_repo_find_feature_blobs(
                    repo,
                    std::ptr::null(),
                    ds_path.as_ptr(),
                    query.as_ptr(),
                    o,
                    n,
                )
            });

            // repo_blob: malformed hex and missing object.
            let bad_hex = CString::new("nothex").unwrap();
            call_err(|o, n| kart_repo_blob(repo, bad_hex.as_ptr(), o, n));
            let zeros = CString::new("0000000000000000000000000000000000000000").unwrap();
            call_err(|o, n| kart_repo_blob(repo, zeros.as_ptr(), o, n));

            // update: non-object updates / unknown column / NULL updates.
            let arr = CString::new("[1]").unwrap();
            call_err(|o, n| {
                kart_feature_update_blob(ds, content.as_ptr(), content.len(), arr.as_ptr(), o, n)
            });
            let unknown = CString::new("{\"no_such_col\": 1}").unwrap();
            call_err(|o, n| {
                kart_feature_update_blob(
                    ds,
                    content.as_ptr(),
                    content.len(),
                    unknown.as_ptr(),
                    o,
                    n,
                )
            });
            call_err(|o, n| {
                kart_feature_update_blob(
                    ds,
                    content.as_ptr(),
                    content.len(),
                    std::ptr::null(),
                    o,
                    n,
                )
            });

            kart_dataset_free(ds);
            kart_repo_free(repo);
        }
        let _ = std::fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn test_capi_rewrite_history() {
        use base64::{engine::general_purpose::STANDARD as B64, Engine as _};

        let (root, repo, ds, [head_hex, _prev_hex]) = unsafe { open_editing_capi("rewrite") };
        let ds_path = CString::new("editing").unwrap();
        unsafe {
            // Find pk 3's blob oid at HEAD via the ABI.
            let commits = CString::new(format!("[\"{head_hex}\"]")).unwrap();
            let query = CString::new("{\"pk\": [3]}").unwrap();
            let out = call_out(|o, n| {
                kart_repo_find_feature_blobs(
                    repo,
                    commits.as_ptr(),
                    ds_path.as_ptr(),
                    query.as_ptr(),
                    o,
                    n,
                )
            });
            let hits: serde_json::Value = serde_json::from_slice(&out).unwrap();
            let old_hex = hits[0]["oid"].as_str().unwrap().to_string();
            let path = hits[0]["path"].as_str().unwrap().to_string();

            // Error paths first (nothing must be mutated by these).
            let bad_b64 = CString::new(format!("{{\"{old_hex}\": \"!!not-base64!!\"}}")).unwrap();
            let prefix = CString::new("refs/backup/t1").unwrap();
            call_err(|o, n| {
                kart_repo_rewrite_history(repo, bad_b64.as_ptr(), prefix.as_ptr(), o, n)
            });
            let bad_oid = CString::new("{\"nothex\": \"aGk=\"}").unwrap();
            call_err(|o, n| {
                kart_repo_rewrite_history(repo, bad_oid.as_ptr(), prefix.as_ptr(), o, n)
            });
            let not_obj = CString::new("[]").unwrap();
            call_err(|o, n| {
                kart_repo_rewrite_history(repo, not_obj.as_ptr(), prefix.as_ptr(), o, n)
            });
            call_err(|o, n| {
                kart_repo_rewrite_history(repo, std::ptr::null(), prefix.as_ptr(), o, n)
            });
            // Bad backup prefix (doesn't start with refs/) => rc -1.
            let subs_json = format!(
                "{{\"{old_hex}\": \"{}\"}}",
                B64.encode(b"substituted content")
            );
            let subs = CString::new(subs_json).unwrap();
            let bad_prefix = CString::new("backup/t1").unwrap();
            call_err(|o, n| {
                kart_repo_rewrite_history(repo, subs.as_ptr(), bad_prefix.as_ptr(), o, n)
            });

            // Happy path, with a backup prefix. pk 3 is untouched between the two
            // commits, so both get rewritten.
            let out = call_out(|o, n| {
                kart_repo_rewrite_history(repo, subs.as_ptr(), prefix.as_ptr(), o, n)
            });
            let stats: serde_json::Value = serde_json::from_slice(&out).unwrap();
            assert_eq!(stats["commits_rewritten"], 2);
            assert_eq!(stats["blobs_written"], 1);
            assert_eq!(
                stats["refs_updated"],
                serde_json::json!(["refs/heads/main"])
            );
            assert_eq!(
                stats["backup_refs"],
                serde_json::json!(["refs/backup/t1/refs/heads/main"])
            );

            // Verify via git2: new tip's blob at `path` holds the new content, and the
            // backup ref preserves the old tip.
            let git = crate::repo::Repo::open(root.to_str().unwrap()).unwrap();
            let new_head = git.git.refname_to_id("refs/heads/main").unwrap();
            assert_ne!(new_head.to_string(), head_hex);
            let tree = git.git.find_commit(new_head).unwrap().tree().unwrap();
            let entry = tree.get_path(std::path::Path::new(&path)).unwrap();
            let blob = git.git.find_blob(entry.id()).unwrap();
            assert_eq!(blob.content(), b"substituted content");
            assert_eq!(
                git.git
                    .refname_to_id("refs/backup/t1/refs/heads/main")
                    .unwrap()
                    .to_string(),
                head_hex
            );

            // NULL backup prefix: substitute the NEW blob again => refs move, but no
            // backup refs are created.
            let subs2_json = format!(
                "{{\"{}\": \"{}\"}}",
                entry.id(),
                B64.encode(b"substituted again")
            );
            let subs2 = CString::new(subs2_json).unwrap();
            let out = call_out(|o, n| {
                kart_repo_rewrite_history(repo, subs2.as_ptr(), std::ptr::null(), o, n)
            });
            let stats: serde_json::Value = serde_json::from_slice(&out).unwrap();
            assert_eq!(stats["commits_rewritten"], 2);
            assert!(stats["refs_updated"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("refs/heads/main")));
            assert_eq!(stats["backup_refs"], serde_json::json!([]));

            kart_dataset_free(ds);
            kart_repo_free(repo);
        }
        let _ = std::fs::remove_dir_all(root.parent().unwrap());
    }
}
