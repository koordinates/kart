import base64
from enum import IntFlag
import logging
import json
from pathlib import Path
import re

from osgeo import osr

from kart.crs_util import normalise_wkt, wkt_equal, make_crs
from kart.exceptions import (
    InvalidOperation,
    INVALID_FILE_FORMAT,
)
from kart.list_of_conflicts import ListOfConflicts
from kart.lfs_util import get_oid_and_size_of_file, prefix_sha256
from kart.geometry import ring_as_wkt
from kart.point_cloud.schema_util import (
    get_schema_from_pdrf_and_vlr,
    get_record_length_from_pdrf,
    equivalent_copc_pdrf,
)
from kart import subprocess_util as subprocess


L = logging.getLogger(__name__)


class RewriteMetadata(IntFlag):
    """Different ways to interpret metadata depending on the type of import."""

    NO_REWRITE = 0x0

    # We're about to convert this file to COPC - update the metadata to be as if we'd already done this.
    # This affects both the format and the schema - only certain PDRFs are allowed in COPC, which constrains the schema.
    AS_IF_CONVERTED_TO_COPC = 0x1

    # Drop all the optimization info from the format info - we don't need to verify it or store it.
    # (ie, because we don't care about whether tiles are optimized, or, we're about to change the tile's optimization anyway.)
    DROP_OPTIMIZATION = 0x2

    # Drop all the format info - we don't need to verify it or store it.
    # (ie, because we're about to convert the tile to a different format anyway).
    DROP_FORMAT = 0x4

    # Drop the schema info - we don't need to verify it or store it.
    # (ie, because we're about to convert the tile to have a different schema anyway).
    DROP_SCHEMA = 0x8


def rewrite_and_merge_metadata(
    tile_metadata_list, rewrite_metadata=RewriteMetadata.NO_REWRITE, override_crs=None
):
    """
    Given a list of tile metadata, merges the parts we expect to be homogenous into a single piece of tile metadata in
    the same format that describes the whole list.

    Depending on how we are to import the tiles, some differences in the metadata may be allowed - to allow for this, we
    drop those parts of the metadata in accordance with the rewrite_metadata option. This means a) the merge will happen
    cleanly in spite of possible differences and b) we won't store any metadata that can't describe every tile in the
    dataset (ie, we won't store anything about whether tiles are COPC if we're going to allow a mix of both COPC and not).

    If override_crs is provided, it will be used to override the CRS of all tiles, setting the dataset CRS.
    """
    result = {}
    # Normalize override CRS if provided
    normalized_override_crs = None
    if override_crs:
        crs_obj = make_crs(override_crs)
        normalized_override_crs = normalise_wkt(crs_obj.ExportToWkt())

    for tile_metadata in tile_metadata_list:
        _merge_metadata_field(
            result, "format.json", rewrite_format(tile_metadata, rewrite_metadata)
        )
        _merge_metadata_field(
            result, "schema.json", rewrite_schema(tile_metadata, rewrite_metadata)
        )

        # Handle CRS with potential override
        if override_crs:
            # Override CRS for all tiles if specified
            tile_crs = normalized_override_crs
        else:
            tile_crs = tile_metadata["crs.wkt"]

        _merge_metadata_field(result, "crs.wkt", tile_crs, eq_func=wkt_equal)
        # Don't copy anything from "tile" to the result - these fields are tile specific and needn't be merged.
    return result


def rewrite_format(tile_metadata, rewrite_metadata=RewriteMetadata.NO_REWRITE):
    orig_format = tile_metadata["format.json"]
    if RewriteMetadata.DROP_FORMAT in rewrite_metadata:
        return {}
    elif RewriteMetadata.DROP_OPTIMIZATION in rewrite_metadata:
        return {
            k: v for k, v in orig_format.items() if not k.startswith("optimization")
        }
    elif RewriteMetadata.AS_IF_CONVERTED_TO_COPC in rewrite_metadata:
        orig_pdrf = orig_format["pointDataRecordFormat"]
        new_pdrf = equivalent_copc_pdrf(orig_pdrf)
        orig_length = orig_format["pointDataRecordLength"]
        new_length = (
            orig_length
            - get_record_length_from_pdrf(orig_pdrf)
            + get_record_length_from_pdrf(new_pdrf)
        )
        return _remove_nones(
            {
                "compression": "laz",
                "lasVersion": "1.4",
                "optimization": "copc",
                "optimizationVersion": "1.0",
                "pointDataRecordFormat": new_pdrf,
                "pointDataRecordLength": new_length,
                "extraBytesVlr": orig_format.get("extraBytesVlr"),
            }
        )
    else:
        return orig_format


def rewrite_schema(tile_metadata, rewrite_metadata=RewriteMetadata.NO_REWRITE):
    if RewriteMetadata.DROP_SCHEMA in rewrite_metadata:
        return {}

    orig_schema = tile_metadata["schema.json"]
    if RewriteMetadata.AS_IF_CONVERTED_TO_COPC in rewrite_metadata:
        orig_pdrf = tile_metadata["format.json"]["pointDataRecordFormat"]
        return get_schema_from_pdrf_and_vlr(equivalent_copc_pdrf(orig_pdrf), None)
    else:
        return orig_schema


def _equal(x, y):
    return x == y


def _merge_metadata_field(output, key, value, *, eq_func=_equal):
    if key not in output:
        output[key] = value
        return
    existing_value = output[key]
    if isinstance(existing_value, ListOfConflicts):
        if value not in existing_value:
            existing_value.append(value)
    else:
        values_are_equal = eq_func(existing_value, value)
        if not values_are_equal:
            output[key] = ListOfConflicts([existing_value, value])


def get_copc_version(info):
    if info.get("copc"):
        # PDAL now hides the COPC VLR from us so we can't do better than this without peeking at the file directly.
        # See https://github.com/PDAL/PDAL/blob/3e33800d85d48f726dcd0931cefe062c4af2b573/io/private/las/Vlr.cpp#L53
        return "1.0"
    else:
        return None


def get_native_extent(info):
    return (
        info["minx"],
        info["maxx"],
        info["miny"],
        info["maxy"],
        info["minz"],
        info["maxz"],
    )


def extract_pc_tile_metadata(pc_tile_path, oid_and_size=None, override_crs=None):
    """
    Use pdal to get any and all point-cloud metadata we can make use of in Kart.
    This includes metadata that must be dataset-homogenous and would be stored in the dataset's /meta/ folder,
    along with other metadata that is tile-specific and would be stored in the tile's pointer file.

    Output:
    {
        "format.json": - Information about file format, as stored at meta/format.json (or some subset thereof).
        "schema.json": - PDRF schema, as stored in meta/schema.json
        "crs.wkt":    - CRS as stored at meta/crs.wkt
        "tile":   - Tile-specific (non-homogenous) information, as stored in individual tile pointer files.
    }

    Although any two point cloud tiles can differ in any way imaginable, we specifically constrain tiles in the
    same dataset to be homogenous enough that the meta items format.json, schema.json and crs.wkt
    describe *all* of the tiles in that dataset. The "tile" field is where we keep all information
    that can be different for every tile in the dataset, which is why it must be stored in pointer files.

    pc_tile_path - a pathlib.Path or a string containing the path to a file or an S3 url.
    oid_and_size - a tuple (sha256_oid, filesize) if already known, to avoid repeated work.
    override_crs - if provided, override the CRS of the tile with this CRS.
    """
    pc_tile_path = str(pc_tile_path)

    try:
        output = subprocess.check_output(
            [
                "pdal",
                "info",
                pc_tile_path,
                "--metadata",
                "--schema",
                "--driver=readers.las",
            ],
            encoding="utf-8",
        )
        output = json.loads(output)
    except subprocess.CalledProcessError:
        raise InvalidOperation(
            f"Error reading {pc_tile_path}", exit_code=INVALID_FILE_FORMAT
        )

    metadata = output["metadata"]

    native_extent = get_native_extent(metadata)
    compound_crs = metadata["srs"].get("compoundwkt")
    horizontal_crs = metadata["srs"].get("wkt")
    is_copc = metadata.get("copc") or False
    pdrf = metadata["dataformat_id"]
    format_json = {
        "compression": "laz" if metadata["compressed"] else "las",
        "lasVersion": f"{metadata['major_version']}.{metadata['minor_version']}",
        "optimization": "copc" if is_copc else None,
        "optimizationVersion": get_copc_version(metadata) if is_copc else None,
        "pointDataRecordFormat": pdrf,
        "pointDataRecordLength": metadata["point_length"],
    }
    extra_bytes_vlr = find_extra_bytes_vlr(metadata)
    if extra_bytes_vlr:
        format_json["extraBytesVlr"] = True

    schema_json = get_schema_from_pdrf_and_vlr(pdrf, extra_bytes_vlr)
    if oid_and_size:
        oid, size = oid_and_size
    else:
        oid, size = get_oid_and_size_of_file(pc_tile_path)

    name = Path(pc_tile_path).name
    url = pc_tile_path if pc_tile_path.startswith("s3://") else None
    # Keep tile info keys in alphabetical order, except oid and size should be last.
    tile_info = {
        "name": name,
        # Reprojection seems to work best if we give it only the horizontal CRS here:
        "crs84Extent": _calc_crs84_extent(
            native_extent, horizontal_crs or compound_crs
        ),
        "format": get_format_summary(format_json),
        "nativeExtent": _format_list_as_str(native_extent),
        "pointCount": metadata["count"],
        "url": url,
        "oid": prefix_sha256(oid),
        "size": size,
    }

    # Use override CRS if provided, otherwise use the CRS from the file
    final_crs = compound_crs or horizontal_crs
    if override_crs:
        crs_obj = make_crs(override_crs)
        final_crs = crs_obj.ExportToWkt()

    result = {
        "format.json": format_json,
        "schema.json": schema_json,
        "crs.wkt": normalise_wkt(final_crs),
        "tile": _remove_nones(tile_info),
    }

    return result


def _format_list_as_str(array):
    """
    We treat a pointer file as a place to store JSON, but its really for storing string-string key-value pairs only.
    Some of our values are a lists of numbers - we turn them into strings by comma-separating them, and we don't
    put them inside square brackets as they would be in JSON.
    """
    return json.dumps(array, separators=(",", ":"))[1:-1]


def get_format_summary(format_info):
    """
    Given format info as stored in format.json, return a short string summary such as: laz-1.4/copc-1.0
    """
    if "format.json" in format_info:
        format_info = format_info["format.json"]

    format_summary = f"{format_info['compression']}-{format_info['lasVersion']}"
    if format_info["optimization"]:
        format_summary += (
            f"/{format_info['optimization']}-{format_info['optimizationVersion']}"
        )
    return format_summary


def _calc_crs84_extent(src_extent, src_crs):
    """
    Given a 3D extent with a particular CRS, return a CRS84 extent that surrounds that extent.
    """
    if not src_crs:
        return None
    src_srs = osr.SpatialReference()
    src_srs.ImportFromWkt(src_crs)
    src_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    dest_srs = osr.SpatialReference()
    dest_srs.SetWellKnownGeogCS("CRS84")

    transform = osr.CoordinateTransformation(src_srs, dest_srs)
    min_x, max_x, min_y, max_y, min_z, max_z = src_extent
    result = transform.TransformPoints(
        [
            (min_x, min_y),
            (min_x, max_y),
            (max_x, max_y),
            (max_x, min_y),
        ]
    )
    return "POLYGON(" + ring_as_wkt(*result, dp=7) + ")"


def is_copc(tile_format):
    tile_format = extract_format(tile_format)
    if isinstance(tile_format, dict):
        return tile_format.get("optimization") == "copc"
    elif isinstance(tile_format, str):
        return "copc" in tile_format
    raise ValueError("Bad tile format")


def get_las_version(tile_format):
    tile_format = extract_format(tile_format)
    if isinstance(tile_format, dict):
        return tile_format.get("lasVersion")
    elif isinstance(tile_format, str):
        match = re.match(r"la[sz]-([0-9\.]+)", tile_format, re.IGNORECASE)
        if match:
            return match.group(1)
    raise ValueError("Bad tile format")


def extract_format(tile_format):
    if isinstance(tile_format, dict):
        if "format.json" in tile_format:
            return tile_format["format.json"]
        if "format" in tile_format:
            return tile_format["format"]
    return tile_format


def find_extra_bytes_vlr(metadata):
    return find_vlr(metadata, "LASF_Spec", 4)


def find_vlr(metadata, user_id, record_id):
    for key, value in metadata.items():
        if not key.startswith("vlr"):
            continue
        if value["user_id"] == user_id and value["record_id"] == record_id:
            return base64.b64decode(value["data"])


def _remove_nones(input_dict):
    return {key: value for key, value in input_dict.items() if value is not None}
