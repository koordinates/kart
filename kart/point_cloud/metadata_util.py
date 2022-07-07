from enum import IntEnum
import json
import logging
import re
import sys

import click

from kart.crs_util import normalise_wkt
from kart.list_of_conflicts import ListOfConflicts
from kart.exceptions import (
    InvalidOperation,
    INVALID_FILE_FORMAT,
    WORKING_COPY_OR_IMPORT_CONFLICT,
)
from kart.output_util import format_json_for_output, format_wkt_for_output
from kart.point_cloud.schema_util import (
    get_schema_from_pdrf,
    get_record_length_from_pdrf,
    equivalent_copc_pdrf,
    pdal_schema_to_kart_schema,
)


L = logging.getLogger(__name__)


class RewriteMetadata(IntEnum):
    """Different ways to interpret metadata depending on the type of import."""

    # We're about to convert this file to COPC - update the metadata to be as if we'd already done this.
    # This affects both the format and the schema - only certain PDRFs are allowed in COPC, which constrains the schema.
    AS_IF_CONVERTED_TO_COPC = 0x1

    # Drop all the optimization info from the format info - we don't need to verify it or store it.
    # (ie, because we don't care about whether tiles are optimized, or, we're about to change the tile's optimization anyway.)
    DROP_OPTIMIZATION = 0x2

    # Drop all the format info - we don't need to verify it or store it
    # (ie, because we're about to convert the tile to a different format anyway).
    DROP_FORMAT = 0x4


def rewrite_and_merge_metadata(tile_metadata_list, rewrite_metadata=None):
    """
    Given a list of tile metadata, merges the parts we expect to be homogenous into a single piece of tile metadata in
    the same format that describes the whole list.

    Depending on how we are to import the tiles, some differences in the metadata may be allowed - to allow for this, we
    drop those parts of the metadata in accordance with the rewrite_metadata option. This means a) the merge will happen
    cleanly in spite of possible differences and b) we won't store any metadata that can't describe every tile in the
    dataset (ie, we won't store anything about whether tiles are COPC if we're going to allow a mix of both COPC and not).
    """
    result = {}
    for tile_metadata in tile_metadata_list:
        _merge_metadata_field(
            result, "format", rewrite_format(tile_metadata, rewrite_metadata)
        )
        _merge_metadata_field(
            result, "schema", rewrite_schema(tile_metadata, rewrite_metadata)
        )
        _merge_metadata_field(result, "crs", tile_metadata["crs"])
        # Don't copy anything from "tile" to the result - these fields are tile specific and needn't be merged.
    return result


def rewrite_format(tile_metadata, rewrite_metadata=None):
    rewrite_metadata = rewrite_metadata or 0

    orig_format = tile_metadata["format"]
    if rewrite_metadata & RewriteMetadata.DROP_FORMAT:
        return {}
    elif rewrite_metadata & RewriteMetadata.DROP_OPTIMIZATION:
        return {
            k: v for k, v in orig_format.items() if not k.startswith("optimization")
        }
    elif rewrite_metadata & RewriteMetadata.AS_IF_CONVERTED_TO_COPC:
        orig_pdrf = orig_format["pointDataRecordFormat"]
        new_pdrf = equivalent_copc_pdrf(orig_pdrf)
        return {
            "compression": "laz",
            "lasVersion": "1.4",
            "optimization": "copc",
            "optimizationVersion": "1.0",
            "pointDataRecordFormat": new_pdrf,
            "pointDataRecordLength": get_record_length_from_pdrf(new_pdrf),
        }
    else:
        return orig_format


def rewrite_schema(tile_metadata, rewrite_metadata=None):
    rewrite_metadata = rewrite_metadata or 0

    orig_schema = tile_metadata["schema"]
    if rewrite_metadata & RewriteMetadata.AS_IF_CONVERTED_TO_COPC:
        orig_pdrf = tile_metadata["format"]["pointDataRecordFormat"]
        return get_schema_from_pdrf(equivalent_copc_pdrf(orig_pdrf))
    else:
        return orig_schema


def _merge_metadata_field(output, key, value):
    if key not in output:
        output[key] = value
        return
    existing_value = output[key]
    if isinstance(existing_value, ListOfConflicts):
        if value not in existing_value:
            existing_value.append(value)
    elif existing_value != value:
        output[key] = ListOfConflicts([existing_value, value])


def check_for_non_homogenous_metadata(merged_metadata, will_convert_to_copc=False):
    _check_for_non_homogenous_meta_item(
        merged_metadata, "format", "file format", future_tense=will_convert_to_copc
    )
    _check_for_non_homogenous_meta_item(
        merged_metadata, "schema", "schema", future_tense=will_convert_to_copc
    )
    _check_for_non_homogenous_meta_item(merged_metadata, "crs", "CRS")


def _check_for_non_homogenous_meta_item(
    merged_metadata, key, output_name, future_tense=False
):
    value = merged_metadata[key]

    if isinstance(value, ListOfConflicts):
        format_func = format_wkt_for_output if key == "crs" else format_json_for_output
        disparity = " vs \n".join(
            (format_func(value, sys.stderr) for value in merged_metadata[key])
        )
        click.echo(
            "Kart constrains certain aspects of Point Cloud datasets to be homogenous.",
            err=True,
        )
        if future_tense:
            click.echo(
                f"The imported files would have more than one {output_name}:", err=True
            )
        else:
            click.echo(f"The input files have more than one {output_name}:", err=True)
        click.echo(disparity, err=True)
        raise InvalidOperation(
            "Non-homogenous dataset supplied", exit_code=WORKING_COPY_OR_IMPORT_CONFLICT
        )


def _unwrap_metadata(metadata):
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    if "metadata" in metadata:
        metadata = metadata["metadata"]
    return metadata


def _format_array(array):
    if array is None:
        return None
    return json.dumps(array, separators=(",", ":"))[1:-1]


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


def extract_pc_tile_metadata(
    pc_tile_path,
    *,
    extract_schema=True,
):
    """
    Use pdal to get any and all point-cloud metadata we can make use of in Kart.
    This includes metadata that must be dataset-homogenous and would be stored in the dataset's /meta/ folder,
    along with other metadata that is tile-specific and would be stored in the tile's pointer file.

    Output:
    {
        "format": - Information about file format, as stored at meta/format.json (or some subset thereof).
        "tile":   - Tile-specific (non-homogenous) information, as stored in individual tile pointer files.
        "schema": - PDRF schema, as stored in meta/schema.json
        "crs":    - CRS as stored at meta/crs.wkt
    }

    Although any two point cloud tiles can differ in any way imaginable, we specifically constrain tiles in the
    same dataset to be homogenous enough that the meta items format.json, schema.json and crs.wkt
    describe *all* of the tiles in that dataset. The "tile" field is where we keep all information
    that can be different for every tile in the dataset, which is why it must be stored in pointer files.
    """
    import pdal

    config = [
        {
            "type": "readers.las",
            "filename": str(pc_tile_path),
            "count": 0,  # Don't read any individual points.
        }
    ]
    if extract_schema:
        config.append({"type": "filters.info"})

    pipeline = pdal.Pipeline(json.dumps(config))
    try:
        pipeline.execute()
    except RuntimeError:
        raise InvalidOperation(
            f"Error reading {pc_tile_path}", exit_code=INVALID_FILE_FORMAT
        )

    metadata = _unwrap_metadata(pipeline.metadata)
    info = metadata["readers.las"]

    native_extent = get_native_extent(info)
    compound_crs = info["srs"].get("compoundwkt")
    horizontal_crs = info["srs"].get("wkt")
    is_copc = info.get("copc") or False
    format_info = {
        "compression": "laz" if info["compressed"] else "las",
        "lasVersion": f"{info['major_version']}.{info['minor_version']}",
        "optimization": "copc" if is_copc else None,
        "optimizationVersion": get_copc_version(info) if is_copc else None,
        "pointDataRecordFormat": info["dataformat_id"],
        "pointDataRecordLength": info["point_length"],
    }

    # Keep tile info keys in alphabetical order.
    tile_info = {
        # PDAL seems to work best if we give it only the horizontal CRS here:
        "crs84Extent": _calc_crs84_extent(
            native_extent, horizontal_crs or compound_crs
        ),
        "format": get_format_summary(format_info),
        "nativeExtent": native_extent,
        "pointCount": info["count"],
    }

    result = {
        "format": format_info,
        "tile": tile_info,
        "crs": normalise_wkt(compound_crs or horizontal_crs),
    }
    if extract_schema:
        result["schema"] = pdal_schema_to_kart_schema(
            metadata["filters.info"]["schema"]
        )

    return result


def get_format_summary(format_info):
    """
    Given format info as stored in format.json, return a short string summary such as: laz-1.4/copc-1.0
    """
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

    import pdal
    import numpy as np

    # Treat the src_extent as if it is a point cloud with only two points:
    # (minx, miny, minz) and (maxx, maxy, maxz).
    # This "point cloud" has the same extent as the source extent, but is otherwise not descriptive
    # of the point cloud that src_extent was extracted from (whatever that might be).
    src_points = np.array(
        [src_extent[0::2], src_extent[1::2]],
        dtype=[
            ("X", np.dtype(float)),
            ("Y", np.dtype(float)),
            ("Z", np.dtype(float)),
        ],
    )

    pipeline = (
        # This reprojection just associates src_crs with src_points - doesn't do any reprojection.
        pdal.Filter.reprojection(in_srs=src_crs, out_srs=src_crs).pipeline(src_points)
        # PDAL filter.stats calculates the native bbox of the input points, and also converts the native bbox into a
        # CRS84 bbox that surrounds the native bbox. The CRS84 bbox only depends on the native bbox, not the input
        # points directly, which is good since our input points define a useful native bbox but are otherwise not
        # descriptive of the actual point cloud that the native bbox was extracted from.
        | pdal.Filter.stats()
    )
    try:
        pipeline.execute()
    except RuntimeError:
        L.warning("Couldn't convert tile CRS to EPGS:4326")
        return None
    metadata = _unwrap_metadata(pipeline.metadata)
    b = metadata["filters.stats"]["bbox"]["EPSG:4326"]["bbox"]
    return b["minx"], b["maxx"], b["miny"], b["maxy"], b["minz"], b["maxz"]


def format_tile_info_for_pointer_file(tile_info):
    """
    Given the tile-info metadata, converts it to a format appropriate for the LFS pointer file.
    """
    # Keep tile info keys in alphabetical order.
    result = {
        "crs84Extent": _format_array(tile_info.get("crs84Extent")),
        "format": tile_info["format"],
        "nativeExtent": _format_array(tile_info["nativeExtent"]),
        "pointCount": tile_info["pointCount"],
    }
    if result["crs84Extent"] is None:
        del result["crs84Extent"]
    return result


def remove_las_extension(filename):
    """Given a tile filename, removes the suffix .las or .laz or .copc.las or .copc.laz"""
    match = re.fullmatch(r"(.+?)(\.copc)*\.la[sz]", filename, re.IGNORECASE)
    if match:
        return match.group(1)
    return filename


def set_file_extension(filename, ext=None, tile_format=None):
    """Changes a tile's file extension to the given extension, or to the extension appropriate for its format."""
    if not ext:
        assert tile_format is not None
        if isinstance(tile_format, dict):
            # Format info as stored in format.json
            ext = f".{tile_format['compression']}"
        else:
            # Format summary string.
            ext = f".{tile_format[0:3]}"

        if is_copc(tile_format):
            ext = ".copc" + ext

    elif not ext.startswith("."):
        ext = f".{ext}"

    return remove_las_extension(filename) + ext


def is_copc(tile_format):
    if isinstance(tile_format, dict):
        return tile_format.get("optimization") == "copc"
    elif isinstance(tile_format, str):
        return "copc" in tile_format
    raise ValueError("Bad tile format")


def get_las_version(tile_format):
    if isinstance(tile_format, dict):
        return tile_format.get("lasVersion")
    elif isinstance(tile_format, str):
        match = re.match(r"la[sz]-([0-9\.]+)", tile_format, re.IGNORECASE)
        if match:
            return match.group(1)
    raise ValueError("Bad tile format")
