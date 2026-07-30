//! Dataset-level metadata and the in-memory snapshot used to decode features/tiles.
//!
//! `Dataset::open` eagerly reads the dataset's `meta/` subtree (schema, legends, CRS,
//! format, ...) into memory so that subsequent calls — including `feature_geometry`,
//! which needs the legend referenced by each feature blob — require no further git
//! access and no live `Repo`.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use git2::{ObjectType, Tree};
use serde_json::Value;

use crate::error::{Error, Result};
use crate::repo::{dataset_type_for_dirname, Repo};

/// One column parsed from a dataset's schema.json.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    /// Kart column id (stable across renames; used in legends).
    pub id: String,
    pub name: String,
    /// Kart data type, e.g. "integer", "text", "geometry".
    pub data_type: String,
    /// True iff the column is part of the primary key.
    pub is_pk: bool,
}

pub struct Dataset {
    /// Kart dataset type, e.g. "table" or "point-cloud".
    pub(crate) dataset_type: String,
    /// Dataset path within the repo, e.g. "mylayer".
    pub(crate) path: String,
    /// Name of the inner dataset dir, e.g. ".table-dataset" or ".sno-dataset".
    pub(crate) inner_name: String,
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
    /// Lazily-built feature path encoder (see `feature_path`).
    pub(crate) path_encoder: OnceLock<crate::paths::PathEncoder>,
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
            inner_name,
            meta,
            geom_column_name,
            geom_column_id,
            primary_key,
            legend_geom_index: Mutex::new(HashMap::new()),
            path_encoder: OnceLock::new(),
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

    /// All columns from schema.json, paired with their primaryKeyIndex (if any),
    /// in schema order.
    fn schema_columns(&self) -> Result<Vec<(ColumnInfo, Option<i64>)>> {
        let bytes = self
            .meta
            .get("schema.json")
            .ok_or_else(|| Error::NotFound("schema.json not found in dataset meta".to_string()))?;
        parse_columns(bytes)
    }

    /// Look up a column by name in schema.json. Returns None if no such column.
    pub fn column_by_name(&self, name: &str) -> Result<Option<ColumnInfo>> {
        Ok(self
            .schema_columns()?
            .into_iter()
            .map(|(col, _)| col)
            .find(|col| col.name == name))
    }

    /// The primary key columns, in ascending primaryKeyIndex order.
    pub fn pk_columns(&self) -> Result<Vec<ColumnInfo>> {
        let mut pks: Vec<(i64, ColumnInfo)> = self
            .schema_columns()?
            .into_iter()
            .filter_map(|(col, idx)| idx.map(|i| (i, col)))
            .collect();
        pks.sort_by_key(|(idx, _)| *idx);
        Ok(pks.into_iter().map(|(_, col)| col).collect())
    }

    /// Decode the legend blob for `legend_hash` (`meta/legend/<hash>` =
    /// `msg_pack([pk_ids, non_pk_ids])`), returning (pk column ids, non-pk column ids).
    pub fn legend(&self, legend_hash: &str) -> Result<(Vec<String>, Vec<String>)> {
        let key = format!("legend/{legend_hash}");
        let bytes = self
            .meta
            .get(&key)
            .ok_or_else(|| Error::NotFound(format!("legend not found in meta: {key}")))?;

        let mut cur: &[u8] = bytes;
        let val = rmpv::decode::read_value(&mut cur)
            .map_err(|e| Error::Msgpack(format!("legend blob: {e}")))?;
        let arr = match val {
            rmpv::Value::Array(arr) => arr,
            _ => return Err(Error::Format("legend is not a msgpack array".to_string())),
        };
        if arr.len() != 2 {
            return Err(Error::Format(format!(
                "legend array has {} elements, expected 2",
                arr.len()
            )));
        }
        let mut it = arr.into_iter();
        let pk_ids = legend_id_list(it.next().unwrap(), "pk")?;
        let non_pk_ids = legend_id_list(it.next().unwrap(), "non-pk")?;
        Ok((pk_ids, non_pk_ids))
    }

    /// Path of the feature with the given pk values, relative to the dataset dir,
    /// e.g. ".table-dataset/feature/A/A/A/B/kUA=". The encoding is configured by
    /// the `path-structure.json` meta item (absent = legacy encoding).
    pub fn feature_path(&self, pk_values: &[rmpv::Value]) -> Result<String> {
        if self.dataset_type != "table" {
            return Err(Error::Format(format!(
                "feature paths only apply to table datasets, not {}",
                self.dataset_type
            )));
        }
        let encoder = match self.path_encoder.get() {
            Some(enc) => enc,
            None => {
                let enc = crate::paths::PathEncoder::from_path_structure_json(
                    self.meta.get("path-structure.json").map(Vec::as_slice),
                )?;
                self.path_encoder.get_or_init(|| enc)
            }
        };
        Ok(format!(
            "{}/feature/{}",
            self.inner_name,
            encoder.encode_pks_to_path(pk_values)?
        ))
    }
}

/// Convert one element of a legend array (a msgpack array of strings) to a Vec<String>.
fn legend_id_list(val: rmpv::Value, label: &str) -> Result<Vec<String>> {
    let arr = match val {
        rmpv::Value::Array(arr) => arr,
        _ => return Err(Error::Format(format!("legend {label} ids is not an array"))),
    };
    arr.into_iter()
        .map(|v| match v {
            rmpv::Value::String(s) => s
                .into_str()
                .ok_or_else(|| Error::Format(format!("legend {label} id is not a string"))),
            _ => Err(Error::Format(format!("legend {label} id is not a string"))),
        })
        .collect()
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

/// Parse schema.json bytes into columns paired with their primaryKeyIndex (if present
/// and non-null), in schema order.
fn parse_columns(bytes: &[u8]) -> Result<Vec<(ColumnInfo, Option<i64>)>> {
    let cols: Value = serde_json::from_slice(bytes)?;
    let arr = cols
        .as_array()
        .ok_or_else(|| Error::Format("schema.json is not an array".to_string()))?;

    let mut out = Vec::with_capacity(arr.len());
    for col in arr {
        let get_str = |field: &str| -> Result<String> {
            col.get(field)
                .and_then(Value::as_str)
                .map(str::to_string)
                .ok_or_else(|| Error::Format(format!("schema.json column missing {field}")))
        };
        let pk_index = col.get("primaryKeyIndex").and_then(Value::as_i64);
        out.push((
            ColumnInfo {
                id: get_str("id")?,
                name: get_str("name")?,
                data_type: get_str("dataType")?,
                is_pk: pk_index.is_some(),
            },
            pk_index,
        ));
    }
    Ok(out)
}

/// Parse schema.json bytes, returning (geom_column_name, geom_column_id, primary_key).
/// Lenient about missing fields: this runs eagerly at open time for every dataset type,
/// and non-table datasets (e.g. point-cloud) have columns without `id`. The strict
/// per-column parse (`parse_columns`) is reserved for the table-only helpers.
fn parse_schema(bytes: &[u8]) -> Result<(Option<String>, Option<String>, Option<String>)> {
    let cols: Value = serde_json::from_slice(bytes)?;
    let arr = cols
        .as_array()
        .ok_or_else(|| Error::Format("schema.json is not an array".to_string()))?;

    let geom = arr
        .iter()
        .find(|col| col.get("dataType").and_then(Value::as_str) == Some("geometry"));
    let get_str = |col: &Value, field: &str| -> Option<String> {
        col.get(field).and_then(Value::as_str).map(str::to_string)
    };
    let geom_name = geom.and_then(|col| get_str(col, "name"));
    let geom_id = geom.and_then(|col| get_str(col, "id"));

    // Primary key column(s): those with a primaryKeyIndex, sorted by it. Single PK only.
    let mut pks: Vec<(i64, String)> = arr
        .iter()
        .filter_map(|col| {
            let idx = col.get("primaryKeyIndex").and_then(Value::as_i64)?;
            Some((idx, get_str(col, "name")?))
        })
        .collect();
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
    use crate::test_support::extract_fixture;

    const POINTS_TGZ: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../tests/data/points.tgz");
    const AU_CENSUS_TGZ: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../tests/data/au-census.tgz");

    #[test]
    fn test_points_repo_and_dataset() {
        let root = extract_fixture(POINTS_TGZ, "points", "ds");
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

    /// Return the raw bytes of the first blob found (depth-first) under `tree`.
    fn first_blob(repo: &Repo, tree: &Tree<'_>) -> Option<Vec<u8>> {
        for entry in tree.iter() {
            match entry.kind() {
                Some(ObjectType::Blob) => {
                    let obj = entry.to_object(&repo.git).unwrap();
                    if let Some(blob) = obj.as_blob() {
                        return Some(blob.content().to_vec());
                    }
                }
                Some(ObjectType::Tree) => {
                    let child = entry.to_object(&repo.git).unwrap().peel_to_tree().unwrap();
                    if let Some(found) = first_blob(repo, &child) {
                        return Some(found);
                    }
                }
                _ => {}
            }
        }
        None
    }

    #[test]
    fn test_column_lookup_and_legend() {
        let root = extract_fixture(POINTS_TGZ, "points", "cols");
        let repo = Repo::open(root.to_str().unwrap()).unwrap();
        let ds = Dataset::open(&repo, "HEAD", "nz_pa_points_topo_150k").unwrap();

        // column_by_name
        let col = ds.column_by_name("name_ascii").unwrap().unwrap();
        assert_eq!(col.data_type, "text");
        assert!(!col.is_pk);
        let fid = ds.column_by_name("fid").unwrap().unwrap();
        assert!(fid.is_pk);
        assert!(ds.column_by_name("no_such_col").unwrap().is_none());

        // legend lookup, via the legend hash referenced by a real feature blob
        let tree = repo.resolve_tree("HEAD").unwrap();
        let feat_entry = tree
            .get_path(std::path::Path::new(
                "nz_pa_points_topo_150k/.table-dataset/feature",
            ))
            .unwrap();
        let feat_tree = feat_entry
            .to_object(&repo.git)
            .unwrap()
            .peel_to_tree()
            .unwrap();
        let blob = first_blob(&repo, &feat_tree).expect("no feature blob found");
        let (legend_hash, _) = crate::feature::decode_feature(&blob).unwrap();

        let (pk_ids, non_pk_ids) = ds.legend(&legend_hash).unwrap();
        assert_eq!(pk_ids.len(), 1);
        let fid_col = ds.column_by_name("fid").unwrap().unwrap();
        assert_eq!(pk_ids[0], fid_col.id);
        assert!(non_pk_ids.contains(ds.geom_column_id.as_ref().unwrap()));
        assert!(!non_pk_ids.contains(&pk_ids[0]));

        // unknown legend hash errors
        assert!(ds
            .legend("0000000000000000000000000000000000000000")
            .is_err());

        // pk_columns
        let pks = ds.pk_columns().unwrap();
        assert_eq!(pks.len(), 1);
        assert_eq!(pks[0].name, "fid");
        assert_eq!(pks[0].data_type, "integer");
        assert!(pks[0].is_pk);

        let _ = std::fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn test_au_census_multi_dataset() {
        let root = extract_fixture(AU_CENSUS_TGZ, "au-census", "ds");
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
