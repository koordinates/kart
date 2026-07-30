//! Feature path encoding (table datasets).
//!
//! A feature's blob lives at `<ds_path>/<inner>/feature/<tree path>/<filename>` where
//! the tree path and filename are computed from the primary key values. The encoding is
//! configured by the dataset's `meta/path-structure.json`; datasets without that meta
//! item use the legacy encoding. Reference: kart/tabular/v3_paths.py.

use rmpv::Value;
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};

const HEX_ALPHABET: &[u8] = b"0123456789abcdef";
// RFC 3548 urlsafe base64 alphabet.
const BASE64_URLSAFE_ALPHABET: &[u8] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scheme {
    /// Single integer pk: tree path is `(pk // branches) % branches**levels` in
    /// fixed-length base-`alphabet` digits.
    Int,
    /// Any pks: tree path is the first `levels` groups of the encoded
    /// `sha256(msg_pack(pk_values))`.
    MsgpackHash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Encoding {
    Hex,
    Base64,
}

/// Computes the feature path (tree path + filename) for a set of pk values.
pub struct PathEncoder {
    scheme: Scheme,
    branches: u64,
    levels: usize,
    encoding: Encoding,
    alphabet: &'static [u8],
    /// Number of alphabet chars per tree level (1 for base64/64, 2 for hex/256).
    group_length: usize,
}

impl PathEncoder {
    /// Build an encoder from the raw bytes of `meta/path-structure.json`.
    /// `None` (meta item absent) means the legacy encoding: msgpack/hash,
    /// 256 branches, 2 levels, hex.
    pub fn from_path_structure_json(bytes: Option<&[u8]>) -> Result<PathEncoder> {
        let (scheme, branches, levels, encoding) = match bytes {
            None => ("msgpack/hash".to_string(), 256, 2, "hex".to_string()),
            Some(bytes) => {
                let val: serde_json::Value = serde_json::from_slice(bytes)?;
                let get_str = |field: &str| -> Result<String> {
                    val.get(field)
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string)
                        .ok_or_else(|| {
                            Error::Format(format!("path-structure.json missing {field}"))
                        })
                };
                let get_u64 = |field: &str| -> Result<u64> {
                    val.get(field)
                        .and_then(serde_json::Value::as_u64)
                        .ok_or_else(|| {
                            Error::Format(format!("path-structure.json missing {field}"))
                        })
                };
                (
                    get_str("scheme")?,
                    get_u64("branches")?,
                    get_u64("levels")? as usize,
                    get_str("encoding")?,
                )
            }
        };

        let scheme = match scheme.as_str() {
            "int" => Scheme::Int,
            "msgpack/hash" => Scheme::MsgpackHash,
            other => {
                return Err(Error::Format(format!(
                    "unsupported feature path scheme: {other:?}"
                )))
            }
        };
        let (encoding, alphabet) = match encoding.as_str() {
            "hex" => (Encoding::Hex, HEX_ALPHABET),
            "base64" => (Encoding::Base64, BASE64_URLSAFE_ALPHABET),
            other => {
                return Err(Error::Format(format!(
                    "unsupported feature path encoding: {other:?}"
                )))
            }
        };

        // group_length: how many base-`alphabet` digits make up one level (branches
        // must be an exact power of the alphabet size).
        let base = alphabet.len() as u64;
        let group_length = (1..=8)
            .find(|gl| base.checked_pow(*gl as u32) == Some(branches))
            .ok_or_else(|| {
                Error::Format(format!(
                    "invalid path specification: {encoding:?} encoding and {branches} branches are incompatible"
                ))
            })?;

        // Sanity-bound levels so later arithmetic/slicing can't overflow or panic.
        // (Kart only ever writes levels=2 or levels=4; hash tree paths can't be longer
        // than the encoded hash.)
        let max_levels = match encoding {
            Encoding::Hex => 64 / group_length,    // sha256 hexdigest
            Encoding::Base64 => 27 / group_length, // b64 of 20 digest bytes, minus padding
        };
        if levels == 0 || levels > max_levels {
            return Err(Error::Format(format!(
                "invalid path specification: {levels} levels not in range 1..={max_levels}"
            )));
        }

        Ok(PathEncoder {
            scheme,
            branches,
            levels,
            encoding,
            alphabet,
            group_length,
        })
    }

    /// The path (tree path + `/` + filename) for a feature with the given pk values,
    /// relative to the dataset's `feature/` tree. e.g. "A/A/A/B/kUA=".
    pub fn encode_pks_to_path(&self, pk_values: &[Value]) -> Result<String> {
        let packed = pack_pk_values(pk_values)?;
        let filename = b64_urlsafe_encode(&packed);
        let tree_path = match self.scheme {
            Scheme::Int => self.int_tree_path(pk_values)?,
            Scheme::MsgpackHash => self.hash_tree_path(&packed),
        };
        Ok(format!("{tree_path}/{filename}"))
    }

    /// Tree path for the int scheme: requires exactly one integer pk.
    fn int_tree_path(&self, pk_values: &[Value]) -> Result<String> {
        let pk: i128 = match pk_values {
            [Value::Integer(i)] => i
                .as_i64()
                .map(i128::from)
                .or_else(|| i.as_u64().map(i128::from))
                .ok_or_else(|| {
                    Error::Format("int path scheme: pk out of integer range".to_string())
                })?,
            _ => {
                return Err(Error::Format(
                    "int path scheme can only encode a single integer pk value".to_string(),
                ))
            }
        };
        // Python semantics: (pk // branches) % branches**levels (floor div, non-negative mod).
        let branches = self.branches as i128;
        let max_trees = branches.checked_pow(self.levels as u32).ok_or_else(|| {
            Error::Format(format!(
                "invalid path specification: {}**{} overflows",
                self.branches, self.levels
            ))
        })?;
        let n = pk.div_euclid(branches).rem_euclid(max_trees) as u128;
        Ok(self.encode_fixed_int(n))
    }

    /// Render `n` as `levels * group_length` base-`alphabet` digits (most significant
    /// first), `/`-joined in groups of `group_length` chars.
    fn encode_fixed_int(&self, mut n: u128) -> String {
        let base = self.alphabet.len() as u128;
        let total = self.levels * self.group_length;
        let mut digits = vec![0u8; total];
        for slot in digits.iter_mut().rev() {
            *slot = self.alphabet[(n % base) as usize];
            n /= base;
        }
        digits
            .chunks(self.group_length)
            .map(|g| std::str::from_utf8(g).unwrap())
            .collect::<Vec<_>>()
            .join("/")
    }

    /// Tree path for the msgpack/hash scheme: the first `levels` groups of
    /// `group_length` chars of the encoded `sha256(packed_pks)`.
    fn hash_tree_path(&self, packed: &[u8]) -> String {
        let digest = Sha256::digest(packed);
        let encoded = match self.encoding {
            // hexhash: hexdigest()[:40]; only levels*group_length chars are used.
            Encoding::Hex => digest
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>(),
            // b64hash: urlsafe base64 of the first 20 digest bytes.
            Encoding::Base64 => b64_urlsafe_encode(&digest[..20]),
        };
        (0..self.levels)
            .map(|i| &encoded[i * self.group_length..(i + 1) * self.group_length])
            .collect::<Vec<_>>()
            .join("/")
    }
}

/// msg_pack the pk values array with the canonical msgpack encoding (identical to
/// what kart writes; see `encode_feature`).
fn pack_pk_values(pk_values: &[Value]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    rmpv::encode::write_value(&mut out, &Value::Array(pk_values.to_vec()))
        .map_err(|e| Error::Msgpack(format!("pack pk values: {e}")))?;
    Ok(out)
}

/// urlsafe base64 with `=` padding (python `base64.urlsafe_b64encode`).
fn b64_urlsafe_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).map_or(0, |&b| b as u32);
        let b2 = chunk.get(2).map_or(0, |&b| b as u32);
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(BASE64_URLSAFE_ALPHABET[(n >> 18) as usize & 63] as char);
        out.push(BASE64_URLSAFE_ALPHABET[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            BASE64_URLSAFE_ALPHABET[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            BASE64_URLSAFE_ALPHABET[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::Dataset;
    use crate::repo::Repo;
    use crate::test_support::extract_fixture;
    use git2::{ObjectType, Tree};

    const POINTS_TGZ: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../tests/data/points.tgz");
    const STRING_PKS_TGZ: &str =
        concat!(env!("CARGO_MANIFEST_DIR"), "/../tests/data/string-pks.tgz");

    const INT_PK_JSON: &[u8] =
        br#"{"scheme": "int", "branches": 64, "levels": 4, "encoding": "base64"}"#;
    const GENERAL_JSON: &[u8] =
        br#"{"scheme": "msgpack/hash", "branches": 64, "levels": 4, "encoding": "base64"}"#;

    fn int_pk_encoder() -> PathEncoder {
        PathEncoder::from_path_structure_json(Some(INT_PK_JSON)).unwrap()
    }

    fn general_encoder() -> PathEncoder {
        PathEncoder::from_path_structure_json(Some(GENERAL_JSON)).unwrap()
    }

    fn legacy_encoder() -> PathEncoder {
        PathEncoder::from_path_structure_json(None).unwrap()
    }

    fn path(enc: &PathEncoder, pks: &[Value]) -> String {
        enc.encode_pks_to_path(pks).unwrap()
    }

    // Golden values generated with kart's Python implementation
    // (kart.tabular.v3_paths.PathEncoder.*.encode_pks_to_path).

    #[test]
    fn test_int_pk_encoder_golden() {
        let enc = int_pk_encoder();
        assert_eq!(path(&enc, &[Value::from(1)]), "A/A/A/A/kQE=");
        assert_eq!(path(&enc, &[Value::from(5)]), "A/A/A/A/kQU=");
        assert_eq!(path(&enc, &[Value::from(64)]), "A/A/A/B/kUA=");
        assert_eq!(path(&enc, &[Value::from(4096)]), "A/A/B/A/kc0QAA==");
        assert_eq!(path(&enc, &[Value::from(-3)]), "_/_/_/_/kf0=");

        // Non-integer or multiple pks are rejected.
        assert!(enc.encode_pks_to_path(&[Value::from("abc")]).is_err());
        assert!(enc.encode_pks_to_path(&[Value::from("ID-0042")]).is_err());
        assert!(enc
            .encode_pks_to_path(&[Value::from(1), Value::from(2)])
            .is_err());
        assert!(enc.encode_pks_to_path(&[]).is_err());
    }

    #[test]
    fn test_general_encoder_golden() {
        let enc = general_encoder();
        assert_eq!(path(&enc, &[Value::from(1)]), "z/c/q/L/kQE=");
        assert_eq!(path(&enc, &[Value::from(5)]), "d/a/-/3/kQU=");
        assert_eq!(path(&enc, &[Value::from(64)]), "c/u/2/I/kUA=");
        assert_eq!(path(&enc, &[Value::from(4096)]), "-/T/Y/0/kc0QAA==");
        assert_eq!(path(&enc, &[Value::from(-3)]), "A/0/x/i/kf0=");
        assert_eq!(path(&enc, &[Value::from("abc")]), "b/9/t/Y/kaNhYmM=");
        assert_eq!(
            path(&enc, &[Value::from("ID-0042")]),
            "7/I/4/S/kadJRC0wMDQy"
        );
    }

    #[test]
    fn test_legacy_encoder_golden() {
        let enc = legacy_encoder();
        assert_eq!(path(&enc, &[Value::from(1)]), "cd/ca/kQE=");
        assert_eq!(path(&enc, &[Value::from(5)]), "75/af/kQU=");
        assert_eq!(path(&enc, &[Value::from(64)]), "72/ed/kUA=");
        assert_eq!(path(&enc, &[Value::from(4096)]), "f9/36/kc0QAA==");
        assert_eq!(path(&enc, &[Value::from(-3)]), "03/4c/kf0=");
        assert_eq!(path(&enc, &[Value::from("abc")]), "6f/db/kaNhYmM=");
        assert_eq!(path(&enc, &[Value::from("ID-0042")]), "ec/8e/kadJRC0wMDQy");
    }

    #[test]
    fn test_invalid_path_structures() {
        // unknown scheme
        assert!(PathEncoder::from_path_structure_json(Some(
            br#"{"scheme": "nope", "branches": 64, "levels": 4, "encoding": "base64"}"#
        ))
        .is_err());
        // unknown encoding
        assert!(PathEncoder::from_path_structure_json(Some(
            br#"{"scheme": "int", "branches": 64, "levels": 4, "encoding": "base32"}"#
        ))
        .is_err());
        // branches not a power of the alphabet size
        assert!(PathEncoder::from_path_structure_json(Some(
            br#"{"scheme": "int", "branches": 100, "levels": 4, "encoding": "base64"}"#
        ))
        .is_err());
        // missing field
        assert!(PathEncoder::from_path_structure_json(Some(
            br#"{"scheme": "int", "levels": 4, "encoding": "base64"}"#
        ))
        .is_err());
        // levels out of range (0, or longer than the encoded hash)
        assert!(PathEncoder::from_path_structure_json(Some(
            br#"{"scheme": "int", "branches": 64, "levels": 0, "encoding": "base64"}"#
        ))
        .is_err());
        assert!(PathEncoder::from_path_structure_json(Some(
            br#"{"scheme": "msgpack/hash", "branches": 64, "levels": 28, "encoding": "base64"}"#
        ))
        .is_err());
    }

    // ---- fixture round-trip: re-encode every feature path in real repos ----

    /// Collect the dataset-relative path of every feature blob (e.g.
    /// ".table-dataset/feature/A/A/A/B/kUA=").
    fn all_feature_blob_paths(repo: &Repo, dataset_path: &str, inner_name: &str) -> Vec<String> {
        let root = repo.resolve_tree("HEAD").unwrap();
        let feat_entry = root
            .get_path(std::path::Path::new(&format!(
                "{dataset_path}/{inner_name}/feature"
            )))
            .unwrap();
        let feat_tree = feat_entry
            .to_object(&repo.git)
            .unwrap()
            .peel_to_tree()
            .unwrap();
        let mut out = Vec::new();
        collect_blob_paths(repo, &feat_tree, &format!("{inner_name}/feature"), &mut out);
        out
    }

    fn collect_blob_paths(repo: &Repo, tree: &Tree<'_>, prefix: &str, out: &mut Vec<String>) {
        for entry in tree.iter() {
            let name = entry.name().unwrap();
            let path = format!("{prefix}/{name}");
            match entry.kind() {
                Some(ObjectType::Blob) => out.push(path),
                Some(ObjectType::Tree) => {
                    let child = entry.to_object(&repo.git).unwrap().peel_to_tree().unwrap();
                    collect_blob_paths(repo, &child, &path, out);
                }
                _ => {}
            }
        }
    }

    /// Inverse of `b64_urlsafe_encode` (test-only).
    fn b64_urlsafe_decode(s: &str) -> Vec<u8> {
        let val = |c: u8| -> u32 {
            BASE64_URLSAFE_ALPHABET
                .iter()
                .position(|&a| a == c)
                .unwrap_or_else(|| panic!("bad base64 char {:?}", c as char)) as u32
        };
        let chars: Vec<u8> = s.bytes().filter(|&c| c != b'=').collect();
        let mut out = Vec::new();
        for chunk in chars.chunks(4) {
            let mut n: u32 = 0;
            for &c in chunk {
                n = (n << 6) | val(c);
            }
            match chunk.len() {
                4 => out.extend_from_slice(&[(n >> 16) as u8, (n >> 8) as u8, n as u8]),
                3 => out.extend_from_slice(&[(n >> 10) as u8, (n >> 2) as u8]),
                2 => out.push((n >> 4) as u8),
                _ => panic!("bad base64 length"),
            }
        }
        out
    }

    /// Decode a feature filename back into its pk values.
    fn decode_filename_to_pks(filename: &str) -> Vec<Value> {
        let packed = b64_urlsafe_decode(filename);
        let mut cur: &[u8] = &packed;
        let val = rmpv::decode::read_value(&mut cur).unwrap();
        match val {
            Value::Array(arr) => arr,
            other => panic!("pk msgpack is not an array: {other:?}"),
        }
    }

    #[test]
    fn test_feature_path_matches_fixture_tree() {
        for (tgz, subdir, ds_name) in [
            (POINTS_TGZ, "points", "nz_pa_points_topo_150k"), // int pk scheme
            (STRING_PKS_TGZ, "string-pks", "nz_waca_adjustments"), // msgpack/hash base64
        ] {
            let root = extract_fixture(tgz, subdir, "tree");
            let repo = Repo::open(root.to_str().unwrap()).unwrap();
            let ds = Dataset::open(&repo, "HEAD", ds_name).unwrap();

            let paths = all_feature_blob_paths(&repo, ds_name, ".table-dataset");
            assert!(!paths.is_empty(), "no feature blobs in {subdir}");
            for expected_path in &paths {
                let filename = expected_path.rsplit('/').next().unwrap();
                let pks = decode_filename_to_pks(filename);
                let actual = ds.feature_path(&pks).unwrap();
                assert_eq!(&actual, expected_path, "{subdir}: pks {pks:?}");
            }

            let _ = std::fs::remove_dir_all(root.parent().unwrap());
        }
    }
}
