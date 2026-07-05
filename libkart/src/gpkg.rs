//! StandardGeoPackageBinary ("GPKG geometry") helpers.
//!
//! These parse the GeoPackage binary header Kart stores (magic "GP", version, flags,
//! optional envelope, then WKB). See kart/geometry.py for the reference implementation.

use crate::error::{Error, Result};

const GPKG_LE_BIT: u8 = 0b0000_0001;
const GPKG_ENVELOPE_BITS: u8 = 0b0000_1110;
const GPKG_EMPTY_BIT: u8 = 0b0001_0000;
const GPKG_EXTENDED_BIT: u8 = 0b0010_0000;

/// Parsed view of a GPKG geometry header.
struct GpkgHeader {
    flags: u8,
    /// number of envelope doubles (0/4/6/8)
    envelope_doubles: usize,
    /// byte offset where the WKB begins
    wkb_offset: usize,
    /// true if header fields (srs_id + envelope) are little-endian
    header_le: bool,
}

/// Number of envelope doubles for an envelope-contents indicator.
fn envelope_doubles(flags: u8) -> Result<usize> {
    let indicator = (flags & GPKG_ENVELOPE_BITS) >> 1;
    match indicator {
        0 => Ok(0),
        1 => Ok(4),
        2 => Ok(6),
        3 => Ok(6),
        4 => Ok(8),
        _ => Err(Error::Format(format!(
            "Invalid envelope contents indicator: {indicator}"
        ))),
    }
}

/// Validate magic/version/flags and compute layout.
fn parse_header(gpkg: &[u8]) -> Result<GpkgHeader> {
    if gpkg.len() < 8 {
        return Err(Error::Format(
            "GPKG geometry too short for header".to_string(),
        ));
    }
    if &gpkg[0..2] != b"GP" {
        return Err(Error::Format(
            "Expected GeoPackage Binary Geometry".to_string(),
        ));
    }
    let version = gpkg[2];
    if version != 0 {
        return Err(Error::Format(format!(
            "Unsupported GPKG geometry version: {version}"
        )));
    }
    let flags = gpkg[3];
    if flags & GPKG_EXTENDED_BIT != 0 {
        return Err(Error::Format(
            "ExtendedGeoPackageBinary is not supported".to_string(),
        ));
    }
    let n = envelope_doubles(flags)?;
    let wkb_offset = 8 + n * 8;
    Ok(GpkgHeader {
        flags,
        envelope_doubles: n,
        wkb_offset,
        header_le: flags & GPKG_LE_BIT != 0,
    })
}

fn read_u32(buf: &[u8], le: bool) -> u32 {
    let b = [buf[0], buf[1], buf[2], buf[3]];
    if le {
        u32::from_le_bytes(b)
    } else {
        u32::from_be_bytes(b)
    }
}

fn read_f64(buf: &[u8], le: bool) -> f64 {
    let b = [
        buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
    ];
    if le {
        f64::from_le_bytes(b)
    } else {
        f64::from_be_bytes(b)
    }
}

/// True if the geometry is flagged empty.
pub fn is_empty(gpkg: &[u8]) -> Result<bool> {
    let h = parse_header(gpkg)?;
    Ok(h.flags & GPKG_EMPTY_BIT != 0)
}

/// The OGR/ISO WKB geometry type code (e.g. 1 = Point, 3 = Polygon, 1003 = Polygon Z).
pub fn geometry_type(gpkg: &[u8]) -> Result<i32> {
    let h = parse_header(gpkg)?;
    // WKB: 1 endian byte then uint32 type, read with the WKB's own endianness.
    if gpkg.len() < h.wkb_offset + 5 {
        return Err(Error::Format("GPKG geometry truncated WKB".to_string()));
    }
    let wkb_le = gpkg[h.wkb_offset] == 1;
    let t = read_u32(&gpkg[h.wkb_offset + 1..], wkb_le);
    Ok(t as i32)
}

/// The stored envelope as (minx, maxx, miny, maxy[, minz, maxz][, minm, maxm]), or None if
/// absent. `only_2d` drops Z/M (returns just the first 4). `calculate_if_missing` would
/// require GDAL to compute from the WKB; libkart does not support it.
pub fn envelope(
    gpkg: &[u8],
    only_2d: bool,
    calculate_if_missing: bool,
) -> Result<Option<Vec<f64>>> {
    let h = parse_header(gpkg)?;
    if h.flags & GPKG_EMPTY_BIT != 0 {
        return Ok(None);
    }
    if h.envelope_doubles == 0 {
        if calculate_if_missing {
            return Err(Error::NotImplemented(
                "gpkg.envelope calculate_if_missing (needs GDAL)",
            ));
        }
        return Ok(None);
    }
    let end = 8 + h.envelope_doubles * 8;
    if gpkg.len() < end {
        return Err(Error::Format(
            "GPKG geometry truncated envelope".to_string(),
        ));
    }
    let mut vals: Vec<f64> = Vec::with_capacity(h.envelope_doubles);
    for i in 0..h.envelope_doubles {
        let off = 8 + i * 8;
        vals.push(read_f64(&gpkg[off..], h.header_le));
    }
    // If any value is NaN, kart treats the envelope as missing.
    if vals.iter().any(|v| v.is_nan()) {
        return Ok(None);
    }
    if only_2d {
        vals.truncate(4);
    }
    Ok(Some(vals))
}

/// The plain WKB geometry (GPKG header stripped). Output is little-endian WKB.
pub fn to_wkb(gpkg: &[u8]) -> Result<Vec<u8>> {
    let h = parse_header(gpkg)?;
    if gpkg.len() < h.wkb_offset + 1 {
        return Err(Error::Format("GPKG geometry truncated WKB".to_string()));
    }
    let wkb = &gpkg[h.wkb_offset..];
    if wkb[0] == 1 {
        // already little-endian
        Ok(wkb.to_vec())
    } else if wkb[0] == 0 {
        byte_swap_wkb_to_le(wkb)
    } else {
        Err(Error::Format(format!(
            "Invalid WKB byte-order marker: {}",
            wkb[0]
        )))
    }
}

/// Convert a big-endian WKB geometry to little-endian, recursively.
/// Returns the LE WKB bytes.
fn byte_swap_wkb_to_le(wkb: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(wkb.len());
    let mut pos = 0usize;
    swap_geometry(wkb, &mut pos, &mut out)?;
    Ok(out)
}

/// Read one WKB geometry (any endianness) starting at `pos`, append its LE form to `out`,
/// advancing `pos` past it.
fn swap_geometry(wkb: &[u8], pos: &mut usize, out: &mut Vec<u8>) -> Result<()> {
    if *pos + 5 > wkb.len() {
        return Err(Error::Format(
            "WKB truncated at geometry header".to_string(),
        ));
    }
    let src_le = wkb[*pos] == 1;
    *pos += 1;
    let gtype = read_u32(&wkb[*pos..], src_le);
    *pos += 4;

    out.push(1); // little-endian marker
    out.extend_from_slice(&gtype.to_le_bytes());

    // ISO encoding: type = base + 1000*Z + 2000*M.
    let base = gtype % 1000;
    let dims = wkb_dims(gtype);

    match base {
        1 => {
            // Point: exactly `dims` doubles (POINT EMPTY is NaNs, still dims doubles).
            swap_doubles(wkb, pos, out, dims, src_le)?;
        }
        2 => {
            // LineString: u32 count, then count * dims doubles
            let n = swap_count(wkb, pos, out, src_le)?;
            swap_doubles(wkb, pos, out, dims * n as usize, src_le)?;
        }
        3 => {
            // Polygon: u32 ring count, each ring: u32 point count + count*dims doubles
            let rings = swap_count(wkb, pos, out, src_le)?;
            for _ in 0..rings {
                let n = swap_count(wkb, pos, out, src_le)?;
                swap_doubles(wkb, pos, out, dims * n as usize, src_le)?;
            }
        }
        4 | 5 | 6 | 7 => {
            // Multi* / GeometryCollection: u32 sub count, each is a full sub-geometry
            let subs = swap_count(wkb, pos, out, src_le)?;
            for _ in 0..subs {
                swap_geometry(wkb, pos, out)?;
            }
        }
        _ => {
            return Err(Error::Format(format!(
                "Unsupported WKB geometry type: {gtype}"
            )));
        }
    }
    Ok(())
}

/// Number of coordinate doubles per point for an ISO WKB type code.
fn wkb_dims(gtype: u32) -> usize {
    // ISO: base + 1000*Z + 2000*M => high = gtype/1000: 0=XY,1=Z,2=M,3=ZM.
    match gtype / 1000 {
        0 => 2,
        1 => 3,
        2 => 3,
        3 => 4,
        _ => 2,
    }
}

fn swap_count(wkb: &[u8], pos: &mut usize, out: &mut Vec<u8>, src_le: bool) -> Result<u32> {
    if *pos + 4 > wkb.len() {
        return Err(Error::Format("WKB truncated at count".to_string()));
    }
    let n = read_u32(&wkb[*pos..], src_le);
    *pos += 4;
    out.extend_from_slice(&n.to_le_bytes());
    Ok(n)
}

fn swap_doubles(
    wkb: &[u8],
    pos: &mut usize,
    out: &mut Vec<u8>,
    count: usize,
    src_le: bool,
) -> Result<()> {
    let need = count * 8;
    if *pos + need > wkb.len() {
        return Err(Error::Format("WKB truncated at coordinates".to_string()));
    }
    for _ in 0..count {
        let v = read_f64(&wkb[*pos..], src_le);
        out.extend_from_slice(&v.to_le_bytes());
        *pos += 8;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    // POINT(1 2) - no envelope, LE header, LE WKB
    const POINT_1_2: &str = "47500001000000000101000000000000000000f03f0000000000000040";
    // LINESTRING(0 0,10 20) - XY envelope
    const LINESTRING: &str = "475000030000000000000000000000000000000000002440000000000000000000000000000034400102000000020000000000000000000000000000000000000000000000000024400000000000003440";
    // POLYGON((0 0,1 0,1 1,0 1,0 0)) - XY envelope
    const POLYGON: &str = "47500003000000000000000000000000000000000000f03f0000000000000000000000000000f03f0103000000010000000500000000000000000000000000000000000000000000000000f03f0000000000000000000000000000f03f000000000000f03f0000000000000000000000000000f03f00000000000000000000000000000000";
    // POINT EMPTY - empty flag set
    const POINT_EMPTY: &str = "47500011000000000101000000000000000000f87f000000000000f87f";
    // POLYGON Z - XYZ envelope, ISO-Z WKB type 1003
    const POLYGON_Z: &str = "47500005000000000000000000000000000000000000f03f0000000000000000000000000000f03f0000000000001440000000000000204001eb0300000100000005000000000000000000000000000000000000000000000000001440000000000000f03f00000000000000000000000000001840000000000000f03f000000000000f03f0000000000001c400000000000000000000000000000f03f0000000000002040000000000000000000000000000000000000000000001440";
    // LINESTRING with BIG-ENDIAN WKB (header LE)
    const LINESTRING_BE_WKB: &str = "475000030000000000000000000000000000000000002440000000000000000000000000000034400000000002000000020000000000000000000000000000000040240000000000004034000000000000";
    // LINESTRING with crs 4326
    const LINESTRING_CRS: &str = "47500003e610000000000000000000000000000000002440000000000000000000000000000034400102000000020000000000000000000000000000000000000000000000000024400000000000003440";

    #[test]
    fn point_is_not_empty() {
        let g = hex(POINT_1_2);
        assert!(!is_empty(&g).unwrap());
        assert_eq!(geometry_type(&g).unwrap(), 1);
        assert_eq!(envelope(&g, false, false).unwrap(), None);
        let wkb = to_wkb(&g).unwrap();
        assert_eq!(wkb[0], 1);
        assert_eq!(wkb, hex("0101000000000000000000f03f0000000000000040"));
    }

    #[test]
    fn linestring_envelope_and_type() {
        let g = hex(LINESTRING);
        assert!(!is_empty(&g).unwrap());
        assert_eq!(geometry_type(&g).unwrap(), 2);
        assert_eq!(
            envelope(&g, false, false).unwrap(),
            Some(vec![0.0, 10.0, 0.0, 20.0])
        );
        let wkb = to_wkb(&g).unwrap();
        assert_eq!(wkb[0], 1);
        assert_eq!(
            wkb,
            hex("0102000000020000000000000000000000000000000000000000000000000024400000000000003440")
        );
    }

    #[test]
    fn polygon_type_and_envelope() {
        let g = hex(POLYGON);
        assert_eq!(geometry_type(&g).unwrap(), 3);
        assert_eq!(
            envelope(&g, false, false).unwrap(),
            Some(vec![0.0, 1.0, 0.0, 1.0])
        );
        let wkb = to_wkb(&g).unwrap();
        assert_eq!(wkb[0], 1);
        assert_eq!(&wkb[1..5], &hex("03000000")[..]);
    }

    #[test]
    fn point_empty() {
        let g = hex(POINT_EMPTY);
        assert!(is_empty(&g).unwrap());
        assert_eq!(geometry_type(&g).unwrap(), 1);
        assert_eq!(envelope(&g, false, false).unwrap(), None);
    }

    #[test]
    fn polygon_z() {
        let g = hex(POLYGON_Z);
        assert_eq!(geometry_type(&g).unwrap(), 1003);
        assert_eq!(
            envelope(&g, false, false).unwrap(),
            Some(vec![0.0, 1.0, 0.0, 1.0, 5.0, 8.0])
        );
        assert_eq!(
            envelope(&g, true, false).unwrap(),
            Some(vec![0.0, 1.0, 0.0, 1.0])
        );
        let wkb = to_wkb(&g).unwrap();
        assert_eq!(wkb[0], 1);
        assert_eq!(&wkb[1..5], &hex("eb030000")[..]); // 1003 LE
    }

    #[test]
    fn big_endian_wkb_converted_to_le() {
        let g = hex(LINESTRING_BE_WKB);
        assert_eq!(geometry_type(&g).unwrap(), 2);
        let wkb = to_wkb(&g).unwrap();
        assert_eq!(wkb[0], 1);
        assert_eq!(
            wkb,
            hex("0102000000020000000000000000000000000000000000000000000000000024400000000000003440")
        );
    }

    #[test]
    fn crs_id_does_not_affect_parsing() {
        let g = hex(LINESTRING_CRS);
        assert_eq!(geometry_type(&g).unwrap(), 2);
        assert_eq!(
            envelope(&g, false, false).unwrap(),
            Some(vec![0.0, 10.0, 0.0, 20.0])
        );
    }

    #[test]
    fn bad_magic_errors() {
        let g = hex("00000001000000000101000000000000000000f03f0000000000000040");
        assert!(matches!(is_empty(&g), Err(Error::Format(_))));
    }
}
