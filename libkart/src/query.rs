//! Multi-commit feature blob search (table datasets).
//!
//! Given a list of commits, find the (path, blob oid) of a feature at each commit —
//! either by primary key (direct tree lookup) or by attribute-equality filter (feature
//! tree scan + blob decode). Work is memoized across commits: a feature tree (or blob)
//! already seen at an earlier commit is not re-examined.

use std::collections::HashMap;
use std::path::Path;

use git2::{ObjectType, Oid, Tree};
use rmpv::Value as MpValue;
use serde_json::Value as JsonValue;

use crate::dataset::Dataset;
use crate::error::{Error, Result};
use crate::feature::decode_feature;
use crate::repo::Repo;

/// How to locate a feature within a dataset.
pub enum FeatureQuery {
    /// Match the feature with exactly these primary key values.
    Pk(Vec<MpValue>),
    /// Match every feature whose named columns all equal the given JSON values.
    /// Filtering on geometry, blob or primary-key columns is an error (use `Pk`
    /// for the latter). A column name that doesn't exist in a commit's schema, or
    /// isn't present in a feature's legend, simply matches nothing at that commit /
    /// for that feature.
    Filter(serde_json::Map<String, JsonValue>),
}

/// One matching feature blob at one commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureHit {
    pub commit: Oid,
    /// Blob path relative to the repo root,
    /// e.g. "mylayer/.table-dataset/feature/A/A/A/A/kQE=".
    pub path: String,
    pub blob_oid: Oid,
}

/// Find the feature blob(s) matching `query` in the dataset at `dataset_path`, at each
/// of `commits`. Returns hits in `commits` order (for a filter, multiple hits per
/// commit in feature-tree order). Commits where the dataset or feature is absent
/// contribute no hits.
pub fn find_feature_blobs(
    repo: &Repo,
    commits: &[Oid],
    dataset_path: &str,
    query: &FeatureQuery,
) -> Result<Vec<FeatureHit>> {
    // Schema-independent query validation: a malformed query always errors, even
    // with no commits or no features to look at.
    if let FeatureQuery::Filter(filter) = query {
        if filter.is_empty() {
            return Err(Error::Format("empty feature filter".to_string()));
        }
        for (name, expected) in filter {
            if !matches!(
                expected,
                JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_)
            ) {
                return Err(Error::Format(format!(
                    "filter value for column '{name}' must be a JSON scalar, got {expected}"
                )));
            }
        }
    }

    let mut hits: Vec<FeatureHit> = Vec::new();
    // Pk memo: (feature tree oid, feature-tree-relative path) -> blob oid (if present).
    let mut pk_memo: HashMap<(Oid, String), Option<Oid>> = HashMap::new();
    let mut filter_memo = FilterMemo::default();

    for &commit in commits {
        let refish = commit.to_string();
        let ds = match Dataset::open(repo, &refish, dataset_path) {
            Ok(ds) => ds,
            // Dataset doesn't exist at this commit: no hits for it.
            Err(Error::NotFound(_)) => continue,
            Err(e) => return Err(e),
        };
        if ds.dataset_type != "table" {
            return Err(Error::Format(format!(
                "feature queries only apply to table datasets, not {}",
                ds.dataset_type
            )));
        }

        let root = repo.resolve_tree(&refish)?;
        let feature_prefix = format!("{dataset_path}/{}/feature", ds.inner_name);
        let feature_tree = match root.get_path(Path::new(&feature_prefix)) {
            Ok(entry) if entry.kind() == Some(ObjectType::Tree) => {
                entry.to_object(&repo.git)?.peel_to_tree()?
            }
            // No feature tree (e.g. empty dataset): no hits for this commit.
            Ok(_) => continue,
            Err(e) if e.code() == git2::ErrorCode::NotFound => continue,
            Err(e) => return Err(e.into()),
        };

        match query {
            FeatureQuery::Pk(pk_values) => {
                // "<inner>/feature/<tree path>/<filename>" -> "<tree path>/<filename>".
                let ds_rel = ds.feature_path(pk_values)?;
                let in_feature = ds_rel
                    .strip_prefix(&format!("{}/feature/", ds.inner_name))
                    .ok_or_else(|| {
                        Error::Format(format!("unexpected feature path form: {ds_rel}"))
                    })?
                    .to_string();
                let key = (feature_tree.id(), in_feature.clone());
                let blob_oid = match pk_memo.get(&key) {
                    Some(cached) => *cached,
                    None => {
                        let found = lookup_blob(&feature_tree, &in_feature)?;
                        pk_memo.insert(key, found);
                        found
                    }
                };
                if let Some(blob_oid) = blob_oid {
                    hits.push(FeatureHit {
                        commit,
                        path: format!("{dataset_path}/{ds_rel}"),
                        blob_oid,
                    });
                }
            }
            FeatureQuery::Filter(filter) => {
                let matches =
                    filter_matches_at_commit(repo, &ds, &feature_tree, filter, &mut filter_memo)?;
                for (rel, blob_oid) in matches {
                    hits.push(FeatureHit {
                        commit,
                        path: format!("{feature_prefix}/{rel}"),
                        blob_oid,
                    });
                }
            }
        }
    }
    Ok(hits)
}

/// Look up `path` under `tree`, returning the blob oid if it names a blob.
fn lookup_blob(tree: &Tree<'_>, path: &str) -> Result<Option<Oid>> {
    match tree.get_path(Path::new(path)) {
        Ok(entry) if entry.kind() == Some(ObjectType::Blob) => Ok(Some(entry.id())),
        Ok(_) => Ok(None),
        Err(e) if e.code() == git2::ErrorCode::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Cross-commit memoization state for filter queries.
///
/// Both maps are only valid for a particular column-name -> column-id resolution;
/// if a commit's schema resolves the filter names to different ids (e.g. a column
/// was dropped and re-added), the memos are cleared.
#[derive(Default)]
struct FilterMemo {
    /// The column ids the memoized entries were computed with.
    ids: Vec<String>,
    /// feature tree oid -> matches within it, as (feature-tree-relative path, blob oid).
    trees: HashMap<Oid, Vec<(String, Oid)>>,
    /// feature blob oid -> whether it matches the filter.
    blobs: HashMap<Oid, bool>,
}

/// Evaluate `filter` against every feature in `feature_tree`, using and updating
/// `memo`. Returns (feature-tree-relative path, blob oid) for each match.
fn filter_matches_at_commit(
    repo: &Repo,
    ds: &Dataset,
    feature_tree: &Tree<'_>,
    filter: &serde_json::Map<String, JsonValue>,
    memo: &mut FilterMemo,
) -> Result<Vec<(String, Oid)>> {
    // Resolve column names to ids for this commit's schema; validate column types.
    // (Schema-independent validation already happened in `find_feature_blobs`.)
    let mut cols: Vec<(String, &JsonValue)> = Vec::with_capacity(filter.len());
    for (name, expected) in filter {
        let col = match ds.column_by_name(name)? {
            Some(col) => col,
            // Column doesn't exist in this commit's schema: nothing can match.
            None => return Ok(Vec::new()),
        };
        if col.data_type == "geometry" || col.data_type == "blob" {
            return Err(Error::Format(format!(
                "cannot filter on {} column '{name}'",
                col.data_type
            )));
        }
        if col.is_pk {
            return Err(Error::Format(format!(
                "cannot filter on primary key column '{name}'; use a pk query instead"
            )));
        }
        cols.push((col.id, expected));
    }

    // The memos assume a fixed name->id resolution; reset them if it changed.
    let ids: Vec<String> = cols.iter().map(|(id, _)| id.clone()).collect();
    if memo.ids != ids {
        memo.ids = ids;
        memo.trees.clear();
        memo.blobs.clear();
    }

    if let Some(cached) = memo.trees.get(&feature_tree.id()) {
        return Ok(cached.clone());
    }

    // Per-commit cache: legend hash -> index of each filter column within the legend's
    // non-pk values (None = some filter column absent from that legend => no match).
    let mut legend_indices: HashMap<String, Option<Vec<usize>>> = HashMap::new();

    let mut matches: Vec<(String, Oid)> = Vec::new();
    walk_filter(
        repo,
        ds,
        feature_tree,
        "",
        &cols,
        &mut legend_indices,
        &mut memo.blobs,
        &mut matches,
    )?;
    memo.trees.insert(feature_tree.id(), matches.clone());
    Ok(matches)
}

/// Recursively walk `tree` (a feature tree or subtree), appending each matching blob
/// as (`prefix`-relative path, blob oid) to `matches`, in tree order.
#[allow(clippy::too_many_arguments)]
fn walk_filter(
    repo: &Repo,
    ds: &Dataset,
    tree: &Tree<'_>,
    prefix: &str,
    cols: &[(String, &JsonValue)],
    legend_indices: &mut HashMap<String, Option<Vec<usize>>>,
    blob_memo: &mut HashMap<Oid, bool>,
    matches: &mut Vec<(String, Oid)>,
) -> Result<()> {
    for entry in tree.iter() {
        let name = match entry.name() {
            Some(n) => n,
            None => continue,
        };
        let rel = if prefix.is_empty() {
            name.to_string()
        } else {
            format!("{prefix}/{name}")
        };
        match entry.kind() {
            Some(ObjectType::Blob) => {
                let blob_oid = entry.id();
                let is_match = match blob_memo.get(&blob_oid) {
                    Some(&m) => m,
                    None => {
                        let obj = entry.to_object(&repo.git)?;
                        let blob = obj
                            .as_blob()
                            .ok_or_else(|| Error::Format("blob entry is not a blob".to_string()))?;
                        let m = blob_matches(ds, blob.content(), cols, legend_indices)?;
                        blob_memo.insert(blob_oid, m);
                        m
                    }
                };
                if is_match {
                    matches.push((rel, blob_oid));
                }
            }
            Some(ObjectType::Tree) => {
                let child = entry.to_object(&repo.git)?.peel_to_tree()?;
                walk_filter(
                    repo,
                    ds,
                    &child,
                    &rel,
                    cols,
                    legend_indices,
                    blob_memo,
                    matches,
                )?;
            }
            _ => {}
        }
    }
    Ok(())
}

/// Decode one feature blob and test it against the filter columns.
fn blob_matches(
    ds: &Dataset,
    blob: &[u8],
    cols: &[(String, &JsonValue)],
    legend_indices: &mut HashMap<String, Option<Vec<usize>>>,
) -> Result<bool> {
    let (legend_hash, values) = decode_feature(blob)?;

    let indices = match legend_indices.get(&legend_hash) {
        Some(cached) => cached.clone(),
        None => {
            let (_, non_pk_ids) = ds.legend(&legend_hash)?;
            let resolved: Option<Vec<usize>> = cols
                .iter()
                .map(|(id, _)| non_pk_ids.iter().position(|nid| nid == id))
                .collect();
            legend_indices.insert(legend_hash.clone(), resolved.clone());
            resolved
        }
    };
    // A legend lacking one of the filter columns means the feature can't match.
    let indices = match indices {
        Some(idx) => idx,
        None => return Ok(false),
    };

    for (i, (_, expected)) in indices.iter().zip(cols) {
        let actual = values.get(*i).ok_or_else(|| {
            Error::Format(format!(
                "feature value index {i} out of range ({} values)",
                values.len()
            ))
        })?;
        if !value_matches(actual, expected) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Compare a decoded msgpack feature value against a JSON filter value.
/// Numbers cross-compare via f64 when the msgpack/JSON numeric types differ.
fn value_matches(actual: &MpValue, expected: &JsonValue) -> bool {
    match (actual, expected) {
        (MpValue::Nil, JsonValue::Null) => true,
        (MpValue::Boolean(a), JsonValue::Bool(e)) => a == e,
        (MpValue::String(a), JsonValue::String(e)) => a.as_str() == Some(e.as_str()),
        (MpValue::Integer(a), JsonValue::Number(e)) => {
            if let (Some(a), Some(e)) = (a.as_i64(), e.as_i64()) {
                a == e
            } else if let (Some(a), Some(e)) = (a.as_u64(), e.as_u64()) {
                a == e
            } else {
                // Mixed numeric types (e.g. msgpack int vs JSON float): compare as f64.
                match (a.as_f64(), e.as_f64()) {
                    (Some(a), Some(e)) => a == e,
                    _ => false,
                }
            }
        }
        (MpValue::F64(a), JsonValue::Number(e)) => e.as_f64() == Some(*a),
        (MpValue::F32(a), JsonValue::Number(e)) => e.as_f64() == Some(f64::from(*a)),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::extract_fixture;
    use serde_json::json;

    const EDITING_TGZ: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../tests/data/editing.tgz");
    const POINTS_TGZ: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../tests/data/points.tgz");

    /// The editing fixture has two commits on the `editing` dataset (columns:
    /// integer pk `id`, text `value`): the import, then "R1 edits" which modifies
    /// pks 1/2/4/5, deletes 6/7/12 and adds 8/9/10. pk 3 (value "c") is untouched.
    fn open_editing(label: &str) -> (std::path::PathBuf, Repo, [Oid; 2]) {
        let root = extract_fixture(EDITING_TGZ, "editing", label);
        let repo = Repo::open(root.to_str().unwrap()).unwrap();
        let head = repo.git.revparse_single("HEAD").unwrap().id();
        let prev = repo.git.revparse_single("HEAD~1").unwrap().id();
        (root, repo, [head, prev])
    }

    fn filter(pairs: &[(&str, JsonValue)]) -> FeatureQuery {
        let mut map = serde_json::Map::new();
        for (k, v) in pairs {
            map.insert(k.to_string(), v.clone());
        }
        FeatureQuery::Filter(map)
    }

    #[test]
    fn test_find_by_pk_across_commits() {
        let (root, repo, commits) = open_editing("pk");

        // pk 3 exists at both commits and is untouched between them.
        let hits = find_feature_blobs(
            &repo,
            &commits,
            "editing",
            &FeatureQuery::Pk(vec![MpValue::from(3)]),
        )
        .unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].commit, commits[0]);
        assert_eq!(hits[1].commit, commits[1]);
        for hit in &hits {
            assert_eq!(hit.path, "editing/.table-dataset/feature/A/A/A/A/kQM=");
        }
        assert_eq!(hits[0].blob_oid, hits[1].blob_oid);

        // pk 1 was modified by "R1 edits": same path, different blob at each commit.
        let hits = find_feature_blobs(
            &repo,
            &commits,
            "editing",
            &FeatureQuery::Pk(vec![MpValue::from(1)]),
        )
        .unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].path, "editing/.table-dataset/feature/A/A/A/A/kQE=");
        assert_eq!(hits[1].path, hits[0].path);
        assert_ne!(hits[0].blob_oid, hits[1].blob_oid);

        // pk 8 was added by "R1 edits": present at HEAD only.
        let hits = find_feature_blobs(
            &repo,
            &commits,
            "editing",
            &FeatureQuery::Pk(vec![MpValue::from(8)]),
        )
        .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].commit, commits[0]);

        let _ = std::fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn test_find_by_filter() {
        let (root, repo, commits) = open_editing("filter");

        // value "c" is pk 3's value at both commits; the filter result must equal
        // the pk lookup result exactly.
        let by_filter = find_feature_blobs(
            &repo,
            &commits,
            "editing",
            &filter(&[("value", json!("c"))]),
        )
        .unwrap();
        let by_pk = find_feature_blobs(
            &repo,
            &commits,
            "editing",
            &FeatureQuery::Pk(vec![MpValue::from(3)]),
        )
        .unwrap();
        assert_eq!(by_filter, by_pk);
        assert_eq!(by_filter.len(), 2);

        // value "a" only exists at the older commit (pk 1 was edited to "a1" at HEAD).
        let hits = find_feature_blobs(
            &repo,
            &commits,
            "editing",
            &filter(&[("value", json!("a"))]),
        )
        .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].commit, commits[1]);
        assert_eq!(hits[0].path, "editing/.table-dataset/feature/A/A/A/A/kQE=");

        // A filter matching nothing anywhere.
        let hits = find_feature_blobs(
            &repo,
            &commits,
            "editing",
            &filter(&[("value", json!("no-such-value"))]),
        )
        .unwrap();
        assert!(hits.is_empty());

        // A column that exists in no commit's schema matches nothing (not an error).
        let hits = find_feature_blobs(
            &repo,
            &commits,
            "editing",
            &filter(&[("no_such_column", json!(1))]),
        )
        .unwrap();
        assert!(hits.is_empty());

        let _ = std::fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn test_find_missing_pk_returns_empty() {
        let (root, repo, commits) = open_editing("missing");
        let hits = find_feature_blobs(
            &repo,
            &commits,
            "editing",
            &FeatureQuery::Pk(vec![MpValue::from(999999)]),
        )
        .unwrap();
        assert!(hits.is_empty());
        let _ = std::fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn test_filter_on_geometry_or_pk_column_errors() {
        let root = extract_fixture(POINTS_TGZ, "points", "badfilter");
        let repo = Repo::open(root.to_str().unwrap()).unwrap();
        let head = repo.git.revparse_single("HEAD").unwrap().id();
        let commits = [head];
        let ds_path = "nz_pa_points_topo_150k";

        // Geometry column.
        assert!(
            find_feature_blobs(&repo, &commits, ds_path, &filter(&[("geom", json!(null))]))
                .is_err()
        );
        // Primary key column.
        assert!(
            find_feature_blobs(&repo, &commits, ds_path, &filter(&[("fid", json!(1))])).is_err()
        );
        // Non-scalar filter value.
        assert!(find_feature_blobs(
            &repo,
            &commits,
            ds_path,
            &filter(&[("name_ascii", json!(["a", "b"]))])
        )
        .is_err());
        // Empty filter.
        assert!(find_feature_blobs(&repo, &commits, ds_path, &filter(&[])).is_err());

        // Schema-independent validation applies even with an empty commits list.
        assert!(find_feature_blobs(&repo, &[], ds_path, &filter(&[])).is_err());
        assert!(find_feature_blobs(
            &repo,
            &[],
            ds_path,
            &filter(&[("name_ascii", json!({"nested": 1}))])
        )
        .is_err());

        let _ = std::fs::remove_dir_all(root.parent().unwrap());
    }
}
