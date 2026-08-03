//! Feature blob decoding (table/vector datasets).

use rmpv::Value;

use crate::dataset::Dataset;
use crate::error::{Error, Result};

/// msgpack ext type code for a geometry value (ord('G')); payload is GPKG bytes.
const GEOM_EXT_CODE: i8 = 0x47;

/// Given the raw bytes of a table/vector feature blob, return the GPKG geometry bytes for
/// the dataset's geometry column, or None if the dataset has no geometry / the value is null.
///
/// Reference (see kart/serialise_util.py + kart/tabular/v3.py):
///   * `msg_unpack(blob)` yields `[legend_hash, non_pk_values]`.
///   * the geometry value is a msgpack ext (code 'G' / 0x47) carrying StandardGeoPackageBinary.
///   * the legend (`meta/legend/<hash>`) lists non-pk column ids; the geometry column's id
///     gives the index into `non_pk_values`.
pub fn feature_geometry(ds: &Dataset, blob: &[u8]) -> Result<Option<Vec<u8>>> {
    // Datasets without a geometry column never have a geometry value.
    let geom_id = match ds.geom_column_id.as_deref() {
        Some(id) => id,
        None => return Ok(None),
    };

    let (legend_hash, non_pk_values) = decode_feature(blob)?;

    // Resolve (and cache) the geometry value's index within non_pk_values for this legend.
    let geom_index = resolve_geom_index(ds, &legend_hash, geom_id)?;
    let geom_index = match geom_index {
        Some(i) => i,
        None => return Ok(None),
    };

    let value = non_pk_values.get(geom_index).ok_or_else(|| {
        Error::Format(format!(
            "geometry index {geom_index} out of range ({} values)",
            non_pk_values.len()
        ))
    })?;

    match value {
        Value::Nil => Ok(None),
        Value::Ext(code, payload) if *code == GEOM_EXT_CODE => Ok(Some(payload.clone())),
        other => Err(Error::Format(format!(
            "expected geometry ext (0x47) or nil, got {other:?}"
        ))),
    }
}

/// Split a feature blob (`msg_pack([legend_hash, non_pk_values])`) into its
/// (legend hash, non-pk values) parts. PK values are not in the blob; they're
/// encoded in the feature's tree path.
pub fn decode_feature(blob: &[u8]) -> Result<(String, Vec<Value>)> {
    let mut cur = blob;
    let top = rmpv::decode::read_value(&mut cur)
        .map_err(|e| Error::Msgpack(format!("feature blob: {e}")))?;
    if !cur.is_empty() {
        return Err(Error::Format(format!(
            "feature blob has {} trailing bytes after msgpack value",
            cur.len()
        )));
    }
    let arr = match top {
        Value::Array(arr) => arr,
        _ => {
            return Err(Error::Format(
                "feature blob is not a msgpack array".to_string(),
            ))
        }
    };
    if arr.len() != 2 {
        return Err(Error::Format(format!(
            "feature blob array has {} elements, expected 2",
            arr.len()
        )));
    }
    let mut it = arr.into_iter();
    let legend_hash = match it.next().unwrap() {
        Value::String(s) => s
            .into_str()
            .ok_or_else(|| Error::Format("feature legend hash is not a string".to_string()))?,
        _ => {
            return Err(Error::Format(
                "feature legend hash is not a string".to_string(),
            ))
        }
    };
    let values = match it.next().unwrap() {
        Value::Array(v) => v,
        _ => {
            return Err(Error::Format(
                "feature non-pk values is not an array".to_string(),
            ))
        }
    };
    Ok((legend_hash, values))
}

/// Inverse of `decode_feature`: serialize (legend_hash, values) back to feature blob
/// bytes. Must be byte-identical to what kart (msgspec) writes, so that re-encoding
/// an unmodified feature yields the same blob OID.
pub fn encode_feature(legend_hash: &str, values: &[Value]) -> Result<Vec<u8>> {
    let top = Value::Array(vec![
        Value::from(legend_hash),
        Value::Array(values.to_vec()),
    ]);
    let mut out = Vec::new();
    rmpv::encode::write_value(&mut out, &top)
        .map_err(|e| Error::Msgpack(format!("encode feature blob: {e}")))?;
    Ok(out)
}

/// Return a copy of `blob` with the values of the named columns replaced.
///
/// `updates` maps column name -> JSON value; each value is coerced to msgpack
/// according to the column's Kart data type (this is the only place JSON->msgpack
/// value coercion lives). Errors if a column is unknown, is part of the primary key,
/// isn't present in the feature's legend, or the JSON value doesn't fit the column's
/// data type. Geometry and blob columns can only be set to null (JSON can't express
/// their binary values).
pub fn update_feature_blob(
    ds: &Dataset,
    blob: &[u8],
    updates: &serde_json::Map<String, serde_json::Value>,
) -> Result<Vec<u8>> {
    let (legend_hash, mut values) = decode_feature(blob)?;
    let (_, non_pk_ids) = ds.legend(&legend_hash)?;

    for (name, json_val) in updates {
        let col = ds
            .column_by_name(name)?
            .ok_or_else(|| Error::NotFound(format!("no column named '{name}' in schema")))?;
        if col.is_pk {
            return Err(Error::Format(format!(
                "cannot update primary key column '{name}'"
            )));
        }
        let index = non_pk_ids
            .iter()
            .position(|id| *id == col.id)
            .ok_or_else(|| {
                Error::Format(format!(
                    "column '{name}' is not in this feature's legend {legend_hash}"
                ))
            })?;
        let n_values = values.len();
        let slot = values.get_mut(index).ok_or_else(|| {
            Error::Format(format!(
                "column '{name}' index {index} out of range ({n_values} values)"
            ))
        })?;
        *slot = json_to_msgpack_value(name, &col.data_type, json_val)?;
    }

    encode_feature(&legend_hash, &values)
}

/// Coerce a JSON value to the msgpack value kart would store for a column of
/// `data_type`. Null maps to Nil for any type; otherwise the JSON type must match.
fn json_to_msgpack_value(name: &str, data_type: &str, val: &serde_json::Value) -> Result<Value> {
    if val.is_null() {
        return Ok(Value::Nil);
    }
    match data_type {
        "geometry" => Err(Error::Format(format!(
            "geometry column '{name}' can only be set to null"
        ))),
        "blob" => Err(Error::Format(format!(
            "blob column '{name}' can only be set to null"
        ))),
        "integer" => val.as_i64().map(Value::from).ok_or_else(|| {
            Error::Format(format!(
                "integer column '{name}' requires a JSON integer, got {val}"
            ))
        }),
        "float" => val.as_f64().map(Value::F64).ok_or_else(|| {
            Error::Format(format!(
                "float column '{name}' requires a JSON number, got {val}"
            ))
        }),
        "boolean" => val.as_bool().map(Value::from).ok_or_else(|| {
            Error::Format(format!(
                "boolean column '{name}' requires a JSON boolean, got {val}"
            ))
        }),
        "text" | "date" | "time" | "timestamp" | "interval" | "numeric" => {
            val.as_str().map(Value::from).ok_or_else(|| {
                Error::Format(format!(
                    "{data_type} column '{name}' requires a JSON string, got {val}"
                ))
            })
        }
        other => Err(Error::Format(format!(
            "unsupported data type '{other}' for column '{name}'"
        ))),
    }
}

/// Look up the index of the geometry column within the non-pk values list for a given legend,
/// using the cache in `ds.legend_geom_index`. Returns None if the legend has no geometry column.
fn resolve_geom_index(ds: &Dataset, legend_hash: &str, geom_id: &str) -> Result<Option<usize>> {
    if let Some(cached) = ds
        .legend_geom_index
        .lock()
        .unwrap()
        .get(legend_hash)
        .copied()
    {
        return Ok(cached);
    }

    let key = format!("legend/{legend_hash}");
    let legend_bytes = ds
        .meta
        .get(&key)
        .ok_or_else(|| Error::NotFound(format!("legend not found in meta: {key}")))?;

    let index = geom_index_from_legend(legend_bytes, geom_id)?;
    ds.legend_geom_index
        .lock()
        .unwrap()
        .insert(legend_hash.to_string(), index);
    Ok(index)
}

/// Parse a legend blob (`[pk_ids, non_pk_ids]`) and return the index of `geom_id` within
/// the non-pk id list, or None if absent.
fn geom_index_from_legend(bytes: &[u8], geom_id: &str) -> Result<Option<usize>> {
    let mut cur = bytes;
    let val = rmpv::decode::read_value(&mut cur)
        .map_err(|e| Error::Msgpack(format!("legend blob: {e}")))?;
    let arr = val
        .as_array()
        .ok_or_else(|| Error::Format("legend is not a msgpack array".to_string()))?;
    if arr.len() < 2 {
        return Err(Error::Format(format!(
            "legend array has {} elements, expected 2",
            arr.len()
        )));
    }
    let non_pk_ids = arr[1]
        .as_array()
        .ok_or_else(|| Error::Format("legend non-pk ids is not an array".to_string()))?;

    for (i, id) in non_pk_ids.iter().enumerate() {
        if id.as_str() == Some(geom_id) {
            return Ok(Some(i));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::Repo;
    use crate::test_support::extract_fixture;
    use git2::{ObjectType, Tree};

    const POINTS_TGZ: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../tests/data/points.tgz");
    const POLYGONS_TGZ: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../tests/data/polygons.tgz");
    const STRING_PKS_TGZ: &str =
        concat!(env!("CARGO_MANIFEST_DIR"), "/../tests/data/string-pks.tgz");

    /// Find the first feature blob bytes under the dataset's inner `feature/` tree.
    fn first_feature_blob(repo: &Repo, dataset_path: &str) -> Vec<u8> {
        let root = repo.resolve_tree("HEAD").unwrap();
        let ds_entry = root.get_path(std::path::Path::new(dataset_path)).unwrap();
        let ds_tree = ds_entry
            .to_object(&repo.git)
            .unwrap()
            .peel_to_tree()
            .unwrap();
        // inner .table-dataset tree
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

    fn find_first_blob(repo: &Repo, tree: &Tree<'_>, out: &mut Option<Vec<u8>>) {
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

    /// Collect (path, bytes) of every feature blob under the dataset's inner `feature/` tree
    /// at HEAD.
    fn all_feature_blobs(repo: &Repo, dataset_path: &str) -> Vec<(String, Vec<u8>)> {
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

        let mut out = Vec::new();
        collect_blobs(repo, &feat_tree, "feature", &mut out);
        out
    }

    fn collect_blobs(repo: &Repo, tree: &Tree<'_>, prefix: &str, out: &mut Vec<(String, Vec<u8>)>) {
        for entry in tree.iter() {
            let name = entry.name().unwrap_or("?");
            let path = format!("{prefix}/{name}");
            match entry.kind() {
                Some(ObjectType::Blob) => {
                    let obj = entry.to_object(&repo.git).unwrap();
                    if let Some(blob) = obj.as_blob() {
                        out.push((path, blob.content().to_vec()));
                    }
                }
                Some(ObjectType::Tree) => {
                    let obj = entry.to_object(&repo.git).unwrap();
                    if let Some(child) = obj.as_tree() {
                        collect_blobs(repo, child, &path, out);
                    }
                }
                _ => {}
            }
        }
    }

    #[test]
    fn test_decode_encode_roundtrip_all_fixtures() {
        for (tgz, subdir, ds_name) in [
            (POINTS_TGZ, "points", Some("nz_pa_points_topo_150k")),
            (POLYGONS_TGZ, "polygons", Some("nz_waca_adjustments")),
            (STRING_PKS_TGZ, "string-pks", None), // discover via list_datasets
        ] {
            let root = extract_fixture(tgz, subdir, "roundtrip");
            let repo = Repo::open(root.to_str().unwrap()).unwrap();
            let datasets = repo.list_datasets("HEAD").unwrap();
            let ds_name = match ds_name {
                Some(n) => n.to_string(),
                None => {
                    assert_eq!(datasets.len(), 1, "expected single dataset in {subdir}");
                    datasets[0].clone()
                }
            };
            assert!(datasets.contains(&ds_name));

            let blobs = all_feature_blobs(&repo, &ds_name);
            assert!(!blobs.is_empty(), "no feature blobs in {subdir}");
            for (path, blob) in &blobs {
                let (legend_hash, values) = decode_feature(blob).unwrap();
                assert_eq!(legend_hash.len(), 40, "{subdir} {path}: bad legend hash");
                let encoded = encode_feature(&legend_hash, &values).unwrap();
                assert_eq!(
                    &encoded, blob,
                    "{subdir} {path}: re-encode not byte-identical"
                );
            }

            let _ = std::fs::remove_dir_all(root.parent().unwrap());
        }
    }

    #[test]
    fn test_decode_feature_rejects_wrong_element_count() {
        let legend = "0123456789abcdef0123456789abcdef01234567";
        // 3-element top-level array: would round-trip lossily, must error.
        let top = Value::Array(vec![
            Value::from(legend),
            Value::Array(vec![Value::from(1)]),
            Value::from("extra"),
        ]);
        let mut blob = Vec::new();
        rmpv::encode::write_value(&mut blob, &top).unwrap();
        let err = decode_feature(&blob).unwrap_err();
        assert!(
            err.to_string().contains("3 elements, expected 2"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_decode_feature_rejects_trailing_bytes() {
        let legend = "0123456789abcdef0123456789abcdef01234567";
        let mut blob = encode_feature(legend, &[Value::from(1)]).unwrap();
        blob.push(0x00);
        let err = decode_feature(&blob).unwrap_err();
        assert!(
            err.to_string().contains("trailing bytes"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_update_feature_blob_values() {
        let root = extract_fixture(POINTS_TGZ, "points", "update");
        let repo = Repo::open(root.to_str().unwrap()).unwrap();
        let ds = Dataset::open(&repo, "HEAD", "nz_pa_points_topo_150k").unwrap();
        let blob = first_feature_blob(&repo, "nz_pa_points_topo_150k");

        let updates = serde_json::json!({"name_ascii": "REDACTED", "t50_fid": null});
        let updates = updates.as_object().unwrap();
        let new_blob = update_feature_blob(&ds, &blob, updates).unwrap();
        assert_ne!(new_blob, blob);

        // Legend unchanged; updated indexes hold new values; all others byte-identical.
        let (old_lh, old_values) = decode_feature(&blob).unwrap();
        let (new_lh, new_values) = decode_feature(&new_blob).unwrap();
        assert_eq!(new_lh, old_lh);
        assert_eq!(new_values.len(), old_values.len());
        let (_, non_pk_ids) = ds.legend(&old_lh).unwrap();
        let idx_of = |name: &str| {
            let col = ds.column_by_name(name).unwrap().unwrap();
            non_pk_ids.iter().position(|id| *id == col.id).unwrap()
        };
        let name_idx = idx_of("name_ascii");
        let t50_idx = idx_of("t50_fid");
        assert_eq!(new_values[name_idx], Value::from("REDACTED"));
        assert_eq!(new_values[t50_idx], Value::Nil);
        for (i, (old, new)) in old_values.iter().zip(new_values.iter()).enumerate() {
            if i != name_idx && i != t50_idx {
                assert_eq!(old, new, "value {i} changed unexpectedly");
            }
        }

        // Idempotency: applying the same updates to new_blob returns new_blob.
        let again = update_feature_blob(&ds, &new_blob, updates).unwrap();
        assert_eq!(again, new_blob);

        // Integer column accepts a JSON integer, rejects string / non-integer number.
        let int_ok = serde_json::json!({"t50_fid": 99});
        let int_blob = update_feature_blob(&ds, &blob, int_ok.as_object().unwrap()).unwrap();
        let (_, int_values) = decode_feature(&int_blob).unwrap();
        assert_eq!(int_values[t50_idx], Value::from(99));
        let int_str = serde_json::json!({"t50_fid": "hello"});
        assert!(update_feature_blob(&ds, &blob, int_str.as_object().unwrap()).is_err());
        let int_frac = serde_json::json!({"t50_fid": 1.5});
        assert!(update_feature_blob(&ds, &blob, int_frac.as_object().unwrap()).is_err());

        // Geometry column: null accepted, non-null rejected.
        let geom_null = serde_json::json!({"geom": null});
        let geom_blob = update_feature_blob(&ds, &blob, geom_null.as_object().unwrap()).unwrap();
        assert_eq!(feature_geometry(&ds, &geom_blob).unwrap(), None);
        let geom_str = serde_json::json!({"geom": "not allowed"});
        assert!(update_feature_blob(&ds, &blob, geom_str.as_object().unwrap()).is_err());

        // Unknown column and pk column rejected.
        let unknown = serde_json::json!({"nope": 1});
        assert!(update_feature_blob(&ds, &blob, unknown.as_object().unwrap()).is_err());
        let pk = serde_json::json!({"fid": 1});
        assert!(update_feature_blob(&ds, &blob, pk.as_object().unwrap()).is_err());

        // Empty updates round-trip byte-identically.
        let empty = serde_json::Map::new();
        assert_eq!(update_feature_blob(&ds, &blob, &empty).unwrap(), blob);

        let _ = std::fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn test_feature_geometry_polygons() {
        let root = extract_fixture(POLYGONS_TGZ, "polygons", "geom");
        let repo = Repo::open(root.to_str().unwrap()).unwrap();
        let ds = Dataset::open(&repo, "HEAD", "nz_waca_adjustments").unwrap();
        assert_eq!(
            ds.geom_column_id.as_deref(),
            Some("c1d4dea1-c0ad-0255-7857-b5695e3ba2e9")
        );

        let blob = first_feature_blob(&repo, "nz_waca_adjustments");
        let geom = feature_geometry(&ds, &blob).unwrap().expect("geometry");
        assert!(
            geom.starts_with(b"GP"),
            "geometry should start with GPKG magic 'GP', got {:?}",
            &geom[..geom.len().min(4)]
        );

        // The cache should now hold the legend->index mapping.
        assert!(!ds.legend_geom_index.lock().unwrap().is_empty());

        let _ = std::fs::remove_dir_all(root.parent().unwrap());
    }
}
