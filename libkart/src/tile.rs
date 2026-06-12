//! Tile pointer decoding (point-cloud datasets).

use crate::dataset::Dataset;
use crate::error::{Error, Result};
use rmpv::Value as MpValue;
use serde_json::{Map, Value as JsonValue};

const EXT_PREFIX: &str = "ext-0-kart-encoded.";

/// Given the raw bytes of a point-cloud tile's Git-LFS pointer blob, return a JSON object
/// summarising the tile (keys include pointCount, nativeExtent, name, oid, format, size).
///
/// Reference: kart/lfs_util.py `pointer_file_bytes_to_dict` (text key/value plus
/// `ext-0-kart-encoded.*` msgpack-encoded extended values), and the name/format
/// post-processing in `TileDataset.get_tile_summary_from_pointer_blob`.
pub fn tile_summary_json(ds: &Dataset, blob: &[u8]) -> Result<Vec<u8>> {
    let text = std::str::from_utf8(blob)?;
    let mut map: Map<String, JsonValue> = Map::new();

    // Seed with the tile name = blob base name (no extension). The blob path is
    // tile/<hh>/<tilename>; we derive the base name from the dataset's tile blob name.
    // The C ABI passes only blob bytes, so the name must come from the pointer's own
    // data via post-processing of `format` plus the supplied seed name.
    if let Some(name) = tile_name_seed(ds) {
        map.insert("name".to_string(), JsonValue::String(name));
    }

    for line in text.lines() {
        if line.is_empty() {
            continue;
        }
        let Some((key, value)) = line.split_once(' ') else {
            // line with no space: skip (matches Python warn+skip)
            continue;
        };

        if let Some(encoded) = key.strip_prefix(EXT_PREFIX) {
            let decoded = decode_extra_values(encoded)?;
            for (k, v) in decoded {
                map.insert(k, v);
            }
        } else if key == "size" {
            let n: i64 = value
                .parse()
                .map_err(|_| Error::Format(format!("invalid tile pointer size: {value}")))?;
            map.insert("size".to_string(), JsonValue::from(n));
        } else {
            map.insert(key.to_string(), JsonValue::String(value.to_string()));
        }
    }

    // Post-processing (TileDataset.get_tile_summary_from_pointer_blob):
    // derive the tile name extension from `format`, then drop `version`.
    let format = map
        .get("format")
        .and_then(JsonValue::as_str)
        .map(str::to_string);
    if let Some(JsonValue::String(name)) = map.get("name") {
        let new_name = set_tile_extension(name, format.as_deref());
        map.insert("name".to_string(), JsonValue::String(new_name));
    }
    map.remove("version");

    let out = serde_json::to_vec(&JsonValue::Object(map))?;
    Ok(out)
}

/// The base tile name (no extension) to seed the summary with. The pointer blob bytes do
/// not carry it; it comes from the blob's git name, which the dataset records when reading
/// the tile tree. Falls back to None if unavailable.
fn tile_name_seed(ds: &Dataset) -> Option<String> {
    // Stored under meta as a transient hint key set by the caller when reading a tile blob.
    // If absent, return None (callers that need a name supply it; tests inject it).
    ds.meta
        .get("__tile_name__")
        .and_then(|b| std::str::from_utf8(b).ok())
        .map(str::to_string)
}

/// Append the correct tile-file extension based on the format summary string.
/// e.g. format "laz-1.4/copc-1.0" -> base + ".copc.laz".
/// Mirrors kart/point_cloud/tilename_util.py set_tile_extension/remove_tile_extension.
fn set_tile_extension(name: &str, tile_format: Option<&str>) -> String {
    let base = remove_tile_extension(name);
    let Some(fmt) = tile_format else {
        return base.to_string();
    };
    let ext = &fmt[..fmt.len().min(3)];
    let mut out = base.to_string();
    if fmt.contains("copc") {
        out.push_str(".copc");
    }
    out.push('.');
    out.push_str(ext);
    out
}

/// Strip a trailing `(.copc)?.la[sz]` extension (case-insensitive).
fn remove_tile_extension(name: &str) -> &str {
    let lower = name.to_ascii_lowercase();
    for suffix in [".copc.laz", ".copc.las", ".laz", ".las"] {
        if lower.ends_with(suffix) {
            return &name[..name.len() - suffix.len()];
        }
    }
    name
}

/// Decode an `ext-0-kart-encoded.<ENC>` value: base64 (altchars '.-', padding stripped)
/// then msgpack map -> JSON object entries.
fn decode_extra_values(encoded: &str) -> Result<Vec<(String, JsonValue)>> {
    let packed = b64_decode_altchars(encoded)?;
    let mut cursor = &packed[..];
    let value = rmpv::decode::read_value(&mut cursor)
        .map_err(|e| Error::Msgpack(format!("tile ext-encoded msgpack: {e}")))?;
    let MpValue::Map(entries) = value else {
        return Err(Error::Msgpack(
            "tile ext-encoded value is not a msgpack map".to_string(),
        ));
    };
    let mut out = Vec::with_capacity(entries.len());
    for (k, v) in entries {
        let key = k
            .as_str()
            .ok_or_else(|| Error::Msgpack("tile ext-encoded map key is not a string".to_string()))?
            .to_string();
        out.push((key, mp_to_json(&v)?));
    }
    Ok(out)
}

/// Convert a msgpack value to JSON (for the extended-values map: plain scalars only).
fn mp_to_json(v: &MpValue) -> Result<JsonValue> {
    Ok(match v {
        MpValue::Nil => JsonValue::Null,
        MpValue::Boolean(b) => JsonValue::Bool(*b),
        MpValue::Integer(i) => {
            if let Some(u) = i.as_u64() {
                JsonValue::from(u)
            } else if let Some(s) = i.as_i64() {
                JsonValue::from(s)
            } else {
                JsonValue::Null
            }
        }
        MpValue::F32(f) => JsonValue::from(*f),
        MpValue::F64(f) => JsonValue::from(*f),
        MpValue::String(s) => JsonValue::String(
            s.as_str()
                .ok_or_else(|| Error::Msgpack("invalid utf-8 in tile ext string".to_string()))?
                .to_string(),
        ),
        MpValue::Array(arr) => {
            JsonValue::Array(arr.iter().map(mp_to_json).collect::<Result<Vec<_>>>()?)
        }
        MpValue::Map(entries) => {
            let mut m = Map::new();
            for (k, val) in entries {
                let key = k.as_str().ok_or_else(|| {
                    Error::Msgpack("tile ext nested map key is not a string".to_string())
                })?;
                m.insert(key.to_string(), mp_to_json(val)?);
            }
            JsonValue::Object(m)
        }
        MpValue::Binary(_) | MpValue::Ext(_, _) => {
            return Err(Error::Msgpack(
                "unexpected binary/ext in tile ext-encoded value".to_string(),
            ))
        }
    })
}

/// Standard base64 decode using altchars '+'->'.', '/'->'-', tolerating missing padding.
fn b64_decode_altchars(s: &str) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(s.len() * 3 / 4 + 3);
    let mut buf: u32 = 0;
    let mut bits: u32 = 0;
    for ch in s.bytes() {
        let val: u32 = match ch {
            b'A'..=b'Z' => (ch - b'A') as u32,
            b'a'..=b'z' => (ch - b'a' + 26) as u32,
            b'0'..=b'9' => (ch - b'0' + 52) as u32,
            b'.' => 62,
            b'-' => 63,
            b'=' => break,
            _ => return Err(Error::Format(format!("invalid base64 char: {ch:#x}"))),
        };
        buf = (buf << 6) | val;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // Build a minimal Dataset for testing. We only rely on ds.meta in tile.rs.
    fn make_dataset(meta: HashMap<String, Vec<u8>>) -> Dataset {
        Dataset {
            dataset_type: "point-cloud".to_string(),
            path: "auckland".to_string(),
            meta,
            geom_column_name: None,
            geom_column_id: None,
            primary_key: None,
            legend_geom_index: std::sync::Mutex::new(HashMap::new()),
        }
    }

    const AUCKLAND_POINTER: &str = concat!(
        "version https://git-lfs.github.com/spec/v1\n",
        "ext-0-kart-encoded.hatjcnM4NEV4dGVudNmCUE9MWUdPTigoMTc0Ljc0OTI2NTEgLTM2",
        "LjgyNDAzMDYsMTc0Ljc0OTI2MzMgLTM2LjgyMzk1MDIsMTc0Ljc1OTE2MTMgLTM2LjgyMzgw",
        "NDEsMTc0Ljc1OTE2MzEgLTM2LjgyMzg4NDYsMTc0Ljc0OTI2NTEgLTM2LjgyNDAzMDYpKaZm",
        "b3JtYXSwbGF6LTEuNC9jb3BjLTEuMKxuYXRpdmVFeHRlbnTZNTE3NTYwMDcuNTUsMTc1Njg5",
        "MC42OCw1OTIzMjIwLjU3LDU5MjMyMjkuNSwtMS4yOCwzMC40qnBvaW50Q291bnQRqXNvdXJj",
        "ZU9pZNlHc2hhMjU2OjA2YmQxNWZiYjY2MTZjZjYzYTRhNDEwYzViYTQ2NjZkYWI3NjE3N2E1",
        "OGNiOTljM2ZhMmFmYjQ2YzlkZDYzNzk",
        " sha256:0000000000000000000000000000000000000000000000000000000000000000\n",
        "oid sha256:32b5fe23040b236dfe469456dd8f7ebbb4dcb3326305ba3e183714a32e4dd1ac\n",
        "size 2137\n"
    );

    #[test]
    fn tile_summary_auckland() {
        let mut meta = HashMap::new();
        meta.insert("__tile_name__".to_string(), b"auckland_1_3".to_vec());
        let ds = make_dataset(meta);

        let out = tile_summary_json(&ds, AUCKLAND_POINTER.as_bytes()).unwrap();
        let v: JsonValue = serde_json::from_slice(&out).unwrap();

        assert_eq!(v["name"], "auckland_1_3.copc.laz");
        assert_eq!(v["format"], "laz-1.4/copc-1.0");
        assert_eq!(v["pointCount"], 17);
        assert_eq!(
            v["nativeExtent"],
            "1756007.55,1756890.68,5923220.57,5923229.5,-1.28,30.4"
        );
        assert_eq!(
            v["crs84Extent"],
            "POLYGON((174.7492651 -36.8240306,174.7492633 -36.8239502,174.7591613 -36.8238041,174.7591631 -36.8238846,174.7492651 -36.8240306))"
        );
        assert_eq!(
            v["sourceOid"],
            "sha256:06bd15fbb6616cf63a4a410c5ba4666dab76177a58cb99c3fa2afb46c9dd6379"
        );
        assert_eq!(
            v["oid"],
            "sha256:32b5fe23040b236dfe469456dd8f7ebbb4dcb3326305ba3e183714a32e4dd1ac"
        );
        assert_eq!(v["size"], 2137);
        // version line must be dropped
        assert!(v.get("version").is_none());
    }

    #[test]
    fn b64_altchars_roundtrip_basic() {
        // "Man" -> "TWFu"
        assert_eq!(b64_decode_altchars("TWFu").unwrap(), b"Man");
        // no padding tolerated: "Ma" -> "TWE"
        assert_eq!(b64_decode_altchars("TWE").unwrap(), b"Ma");
    }
}
