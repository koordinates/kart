//! Dataset-level metadata and the in-memory snapshot used to decode features/tiles.
//!
//! `Dataset::open` eagerly reads the dataset's `meta/` subtree (schema, legends, CRS,
//! format, ...) into memory so that subsequent calls — including `feature_geometry`,
//! which needs the legend referenced by each feature blob — require no further git
//! access and no live `Repo`.

use std::collections::HashMap;
use std::sync::Mutex;

use git2::{ObjectType, Tree};
use serde_json::Value;

use crate::error::{Error, Result};
use crate::repo::{dataset_type_for_dirname, Repo};

pub struct Dataset {
    /// Kart dataset type, e.g. "table" or "point-cloud".
    pub(crate) dataset_type: String,
    /// Dataset path within the repo, e.g. "mylayer".
    pub(crate) path: String,
    /// Raw contents of the dataset's `meta/` subtree, keyed by path relative to `meta/`
    /// (e.g. "schema.json", "crs/EPSG:4326.wkt", "legend/<hash>").
    pub(crate) meta: HashMap<String, Vec<u8>>,
    /// Name of the geometry column, if this dataset has one.
    pub(crate) geom_column_name: Option<String>,
    /// Kart column id of the geometry column, if any (used to locate geometry in a feature).
    pub(crate) geom_column_id: Option<String>,
    /// Primary key column name, if a single-column PK.
    pub(crate) primary_key: Option<String>,
    /// Cache: legend hash -> index of the geometry value within that legend's non-pk values.
    pub(crate) legend_geom_index: Mutex<HashMap<String, Option<usize>>>,
}

impl Dataset {
    /// Open the dataset at `path` as it exists at `refish` in `repo`.
    pub fn open(repo: &Repo, refish: &str, path: &str) -> Result<Dataset> {
        let root = repo.resolve_tree(refish)?;

        // Navigate to the dataset's parent tree at `path`.
        let dataset_tree = if path.is_empty() {
            return Err(Error::NotFound("empty dataset path".to_string()));
        } else {
            let entry = root
                .get_path(std::path::Path::new(path))
                .map_err(|_| Error::NotFound(format!("dataset path not found: {path}")))?;
            let obj = entry.to_object(&repo.git)?;
            obj.peel_to_tree()
                .map_err(|_| Error::NotFound(format!("dataset path is not a tree: {path}")))?
        };

        // Find the inner `.*-dataset*` dir.
        let mut inner_name: Option<String> = None;
        for entry in dataset_tree.iter() {
            if entry.kind() == Some(ObjectType::Tree) {
                if let Some(name) = entry.name() {
                    if dataset_type_for_dirname(name).is_some() {
                        inner_name = Some(name.to_string());
                        break;
                    }
                }
            }
        }
        let inner_name = inner_name
            .ok_or_else(|| Error::NotFound(format!("no dataset dir under path: {path}")))?;
        let dataset_type = dataset_type_for_dirname(&inner_name).unwrap().to_string();

        let inner_entry = dataset_tree
            .get_name(&inner_name)
            .ok_or_else(|| Error::NotFound(format!("inner dir vanished: {inner_name}")))?;
        let inner_tree = inner_entry.to_object(&repo.git)?.peel_to_tree()?;

        // Load the meta/ subtree recursively, keyed relative to meta/.
        let mut meta: HashMap<String, Vec<u8>> = HashMap::new();
        if let Some(meta_entry) = inner_tree.get_name("meta") {
            if let Ok(meta_tree) = meta_entry.to_object(&repo.git)?.peel_to_tree() {
                load_tree_blobs(repo, &meta_tree, "", &mut meta)?;
            }
        }

        // Parse schema.json (if present) for geom + pk metadata.
        let (geom_column_name, geom_column_id, primary_key) = match meta.get("schema.json") {
            Some(bytes) => parse_schema(bytes)?,
            None => (None, None, None),
        };

        Ok(Dataset {
            dataset_type,
            path: path.to_string(),
            meta,
            geom_column_name,
            geom_column_id,
            primary_key,
            legend_geom_index: Mutex::new(HashMap::new()),
        })
    }

    /// JSON describing this dataset: path, type, has_geometry, primary_key,
    /// geom_column_name, and columns (id, name, dataType, and geometry details).
    pub fn schema_json(&self) -> Result<Vec<u8>> {
        let columns: Value = match self.meta.get("schema.json") {
            Some(bytes) => serde_json::from_slice(bytes)?,
            None => Value::Array(vec![]),
        };

        let out = serde_json::json!({
            "path": self.path,
            "type": self.dataset_type,
            "has_geometry": self.geom_column_id.is_some(),
            "primary_key": self.primary_key,
            "geom_column_name": self.geom_column_name,
            "columns": columns,
        });
        Ok(serde_json::to_vec(&out)?)
    }

    /// The source CRS of this dataset's geometry as WKT, or None if it has no CRS.
    pub fn crs_wkt(&self) -> Result<Option<String>> {
        // Find the geometry column's geometryCRS in schema.json.
        let bytes = match self.meta.get("schema.json") {
            Some(b) => b,
            None => return Ok(None),
        };
        let cols: Value = serde_json::from_slice(bytes)?;
        let crs_name = cols
            .as_array()
            .and_then(|arr| {
                arr.iter()
                    .find(|c| c.get("dataType").and_then(Value::as_str) == Some("geometry"))
            })
            .and_then(|c| c.get("geometryCRS"))
            .and_then(Value::as_str);

        let crs_name = match crs_name {
            Some(n) => n,
            None => return Ok(None),
        };
        let key = format!("crs/{crs_name}.wkt");
        match self.meta.get(&key) {
            Some(b) => Ok(Some(String::from_utf8(b.clone()).map_err(|e| {
                Error::Utf8(format!("crs wkt is not valid utf-8: {e}"))
            })?)),
            None => Ok(None),
        }
    }

    /// Raw bytes of the named meta item (e.g. "schema.json", "format.json"), or None.
    pub fn meta_item(&self, name: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.meta.get(name).cloned())
    }
}

/// Recursively load all blobs under `tree` into `out`, keyed by path relative to the
/// initial tree (using `prefix` to accumulate the relative path).
fn load_tree_blobs(
    repo: &Repo,
    tree: &Tree<'_>,
    prefix: &str,
    out: &mut HashMap<String, Vec<u8>>,
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
                let obj = entry.to_object(&repo.git)?;
                if let Some(blob) = obj.as_blob() {
                    out.insert(rel, blob.content().to_vec());
                }
            }
            Some(ObjectType::Tree) => {
                let obj = entry.to_object(&repo.git)?;
                if let Some(child) = obj.as_tree() {
                    load_tree_blobs(repo, child, &rel, out)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Parse schema.json bytes, returning (geom_column_name, geom_column_id, primary_key).
fn parse_schema(bytes: &[u8]) -> Result<(Option<String>, Option<String>, Option<String>)> {
    let cols: Value = serde_json::from_slice(bytes)?;
    let arr = cols
        .as_array()
        .ok_or_else(|| Error::Format("schema.json is not an array".to_string()))?;

    let mut geom_name = None;
    let mut geom_id = None;
    for col in arr {
        if col.get("dataType").and_then(Value::as_str) == Some("geometry") {
            geom_id = col.get("id").and_then(Value::as_str).map(str::to_string);
            geom_name = col.get("name").and_then(Value::as_str).map(str::to_string);
            break;
        }
    }

    // Primary key column(s): those with a primaryKeyIndex, sorted by it. Single PK only.
    let mut pks: Vec<(i64, String)> = Vec::new();
    for col in arr {
        if let Some(idx) = col.get("primaryKeyIndex").and_then(Value::as_i64) {
            if let Some(name) = col.get("name").and_then(Value::as_str) {
                pks.push((idx, name.to_string()));
            }
        }
    }
    pks.sort_by_key(|(idx, _)| *idx);
    let primary_key = if pks.len() == 1 {
        Some(pks.into_iter().next().unwrap().1)
    } else {
        None
    };

    Ok((geom_name, geom_id, primary_key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::disable_owner_validation;
    use std::process::Command;

    /// Extract a fixture tgz into a fresh temp dir, returning the repo root path.
    fn extract_fixture(tgz: &str, subdir: &str) -> std::path::PathBuf {
        disable_owner_validation();
        let base =
            std::env::temp_dir().join(format!("libkart-test-{}-{}", subdir, std::process::id()));
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

    const POINTS_TGZ: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../tests/data/points.tgz");
    const AU_CENSUS_TGZ: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../tests/data/au-census.tgz");

    #[test]
    fn test_points_repo_and_dataset() {
        let root = extract_fixture(POINTS_TGZ, "points");
        let repo = Repo::open(root.to_str().unwrap()).unwrap();

        // Version.
        assert_eq!(repo.table_dataset_version().unwrap(), 3);

        // Dataset listing.
        let datasets = repo.list_datasets("HEAD").unwrap();
        assert!(
            datasets.contains(&"nz_pa_points_topo_150k".to_string()),
            "datasets: {datasets:?}"
        );

        // Open the dataset.
        let ds = Dataset::open(&repo, "HEAD", "nz_pa_points_topo_150k").unwrap();
        assert_eq!(ds.dataset_type, "table");
        assert_eq!(ds.geom_column_name.as_deref(), Some("geom"));
        assert_eq!(
            ds.geom_column_id.as_deref(),
            Some("f488ae9b-6e15-1fe3-0bda-e0d5d38ea69e")
        );
        assert_eq!(ds.primary_key.as_deref(), Some("fid"));

        // schema_json parses and reports the geometry column.
        let schema_bytes = ds.schema_json().unwrap();
        let schema: Value = serde_json::from_slice(&schema_bytes).unwrap();
        assert_eq!(schema["type"], "table");
        assert_eq!(schema["has_geometry"], true);
        assert_eq!(schema["primary_key"], "fid");
        assert_eq!(schema["geom_column_name"], "geom");
        let cols = schema["columns"].as_array().unwrap();
        assert!(cols
            .iter()
            .any(|c| c["dataType"] == "geometry" && c["name"] == "geom"));

        // meta_item passthrough.
        let title = ds.meta_item("title").unwrap().unwrap();
        assert_eq!(
            String::from_utf8(title).unwrap(),
            "NZ Pa Points (Topo, 1:50k)"
        );

        // crs_wkt.
        let wkt = ds.crs_wkt().unwrap().unwrap();
        assert!(wkt.starts_with("GEOGCS[\"WGS 84\""));

        let _ = std::fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn test_au_census_multi_dataset() {
        let root = extract_fixture(AU_CENSUS_TGZ, "au-census");
        let repo = Repo::open(root.to_str().unwrap()).unwrap();
        assert_eq!(repo.table_dataset_version().unwrap(), 3);

        let datasets = repo.list_datasets("HEAD").unwrap();
        assert!(datasets.contains(&"census2016_sdhca_ot_ra_short".to_string()));
        assert!(datasets.contains(&"census2016_sdhca_ot_sos_short".to_string()));

        let ds = Dataset::open(&repo, "HEAD", "census2016_sdhca_ot_ra_short").unwrap();
        assert_eq!(ds.dataset_type, "table");
        assert_eq!(ds.geom_column_name.as_deref(), Some("geom"));
        assert_eq!(
            ds.geom_column_id.as_deref(),
            Some("6cc2833f-f0c3-9437-4294-e6f4bb01e388")
        );

        let _ = std::fs::remove_dir_all(root.parent().unwrap());
    }
}
