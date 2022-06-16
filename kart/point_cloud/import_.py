from enum import Enum, auto
import json
import logging
import os
from pathlib import Path
import re
import sys

import click

from kart.crs_util import normalise_wkt
from kart.dataset_util import validate_dataset_paths
from kart.exceptions import (
    InvalidOperation,
    NotFound,
    NO_IMPORT_SOURCE,
    INVALID_FILE_FORMAT,
)
from kart.fast_import import (
    FastImportSettings,
    git_fast_import,
    generate_header,
    write_blob_to_stream,
    write_blobs_to_stream,
)
from kart.key_filters import RepoKeyFilter
from kart.lfs_util import (
    install_lfs_hooks,
    dict_to_pointer_file_bytes,
    copy_file_to_local_lfs_cache,
)
from kart.serialise_util import hexhash, json_pack, ensure_bytes
from kart.output_util import format_json_for_output, format_wkt_for_output
from kart.tabular.version import (
    SUPPORTED_VERSIONS,
    extra_blobs_for_version,
)
from kart.working_copy import PartType
from kart.point_cloud.schema_util import get_schema_from_pdrf, equivalent_copc_pdrf


L = logging.getLogger(__name__)


class RewriteMetadata(Enum):
    """Different ways to interpret metadata depending on the type of import."""

    PRE_CONVERT_TO_COPC = auto()  # We're about to convert these files to COPC.
    PRESERVE_FORMAT = auto()  # We're going to keep these files as they are.


@click.command("point-cloud-import", hidden=True)
@click.pass_context
@click.option(
    "--convert-to-copc/--no-convert-to-copc",
    " /--preserve-format",
    is_flag=True,
    default=True,
    help="Whether to convert all non-COPC LAS or LAZ files to COPC LAZ files, or to import all files in their native format.",
)
@click.option(
    "--dataset-path", "ds_path", help="The dataset's path once imported", required=True
)
@click.option(
    "--checkout/--no-checkout",
    "do_checkout",
    is_flag=True,
    default=True,
    help="Whether to create a working copy once the import is finished, if no working copy exists yet.",
)
@click.argument("sources", metavar="SOURCES", nargs=-1, required=True)
def point_cloud_import(ctx, convert_to_copc, ds_path, do_checkout, sources):
    """
    Experimental command for importing point cloud datasets. Work-in-progress.
    Will eventually be merged with the main `import` command.

    SOURCES should be one or more LAZ or LAS files (or wildcards that match multiple LAZ or LAS files).
    """
    repo = ctx.obj.repo

    # TODO - improve path validation to make sure datasets of any type don't collide with each other
    # or with attachments.
    validate_dataset_paths([ds_path])

    for source in sources:
        if not (Path() / source).is_file():
            raise NotFound(f"No data found at {source}", exit_code=NO_IMPORT_SOURCE)

    source_to_metadata = {}

    for source in sources:
        click.echo(f"Checking {source}...          \r", nl=False)
        source_to_metadata[source] = extract_pc_tile_metadata(source)
    click.echo()

    if not convert_to_copc:
        if any(
            v["format"]["compression"] == "las" for v in source_to_metadata.values()
        ):
            raise InvalidOperation(
                "LAS datasets are not supported - dataset must be converted to LAZ / COPC",
                exit_code=INVALID_FILE_FORMAT,
            )

    rewrite_metadata = (
        RewriteMetadata.PRE_CONVERT_TO_COPC
        if convert_to_copc
        else RewriteMetadata.PRESERVE_FORMAT
    )
    merged_metadata = rewrite_and_merge_metadata(
        source_to_metadata.values(), rewrite_metadata
    )
    check_for_non_homogenous_metadata(merged_metadata, convert_to_copc)

    # Set up LFS hooks. This is also in `kart init`, but not every existing Kart repo will have these hooks.
    install_lfs_hooks(repo)

    # We still need to write .kart.repostructure.version unfortunately, even though it's only relevant to tabular datasets.
    assert repo.table_dataset_version in SUPPORTED_VERSIONS
    extra_blobs = (
        extra_blobs_for_version(repo.table_dataset_version)
        if not repo.head_commit
        else []
    )

    header = generate_header(
        repo,
        None,
        f"Importing {len(sources)} LAZ tiles as {ds_path}",
        repo.head_branch,
        repo.head_commit,
    )

    ds_inner_path = f"{ds_path}/.point-cloud-dataset.v1"

    def convert_tile_to_copc_and_reextract_metadata(source, dest):
        nonlocal source_to_metadata

        _convert_tile_to_copc(source, dest)
        source_to_metadata[source] = extract_pc_tile_metadata(dest, extract_schema=True)

    with git_fast_import(repo, *FastImportSettings().as_args(), "--quiet") as proc:
        proc.stdin.write(header.encode("utf8"))

        for i, blob_path in write_blobs_to_stream(proc.stdin, extra_blobs):
            pass

        for source in sources:
            click.echo(f"Importing {source}...")
            source_metadata = source_to_metadata[source]

            tile_is_copc = source_metadata["format"]["optimization"] == "copc"
            import_ext = ".copc.laz" if tile_is_copc else ".laz"
            conversion_func = None

            if convert_to_copc and not tile_is_copc:
                conversion_func = convert_tile_to_copc_and_reextract_metadata
                import_ext = ".copc.laz"

            pointer_dict = copy_file_to_local_lfs_cache(repo, source, conversion_func)
            pointer_dict.update(
                format_tile_info_for_pointer_file(source_to_metadata[source]["tile"])
            )
            # TODO - is this the right prefix and name?
            tilename = _remove_las_ext(os.path.basename(source)) + import_ext
            tile_prefix = hexhash(tilename)[0:2]
            blob_path = f"{ds_inner_path}/tile/{tile_prefix}/{tilename}"
            write_blob_to_stream(
                proc.stdin, blob_path, dict_to_pointer_file_bytes(pointer_dict)
            )

        rewrite_metadata = None if convert_to_copc else RewriteMetadata.PRESERVE_FORMAT
        merged_metadata = rewrite_and_merge_metadata(
            source_to_metadata.values(), rewrite_metadata
        )
        check_for_non_homogenous_metadata(merged_metadata)

        write_blob_to_stream(
            proc.stdin,
            f"{ds_inner_path}/meta/format.json",
            json_pack(merged_metadata["format"]),
        )
        write_blob_to_stream(
            proc.stdin,
            f"{ds_inner_path}/meta/schema.json",
            json_pack(merged_metadata["schema"]),
        )
        write_blob_to_stream(
            proc.stdin,
            f"{ds_inner_path}/meta/crs.wkt",
            ensure_bytes(normalise_wkt(merged_metadata["crs"])),
        )

    parts_to_create = [PartType.WORKDIR] if do_checkout else []
    # During imports we can keep old changes since they won't conflict with newly imported datasets.
    repo.working_copy.reset_to_head(
        repo_key_filter=RepoKeyFilter.datasets([ds_path]),
        create_parts_if_missing=parts_to_create,
    )


def _remove_las_ext(filename):
    match = re.fullmatch(r"(.+?)(\.copc)*\.la[sz]", filename, re.IGNORECASE)
    if match:
        return match.group(1)
    return filename


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


class ListOfConflicts(list):
    """
    A list of conflicting possibilities.
    Having one of these in a merged_metadata means that the metadata couldn't be fully merged.
    """

    pass


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
            result, "format", _rewrite_format(tile_metadata, rewrite_metadata)
        )
        _merge_metadata_field(
            result, "schema", _rewrite_schema(tile_metadata, rewrite_metadata)
        )
        _merge_metadata_field(result, "crs", tile_metadata["crs"])
        # Don't copy anything from "tile" to the result - these fields are tile specific and needn't be merged.
    return result


def _rewrite_format(tile_metadata, rewrite_metadata=None):
    format_ = tile_metadata["format"]
    if not rewrite_metadata:
        return format_
    elif rewrite_metadata == RewriteMetadata.PRESERVE_FORMAT:
        # For a preserve-format / non-COPC dataset, we don't care which optimization tiles have, if any.
        # So we drop those fields so that we don't constrain them to be homogenous and don't write them to
        # to the dataset's "format.json" file.
        return {k: v for k, v in format_.items() if not k.startswith("optimization")}
    elif rewrite_metadata == RewriteMetadata.PRE_CONVERT_TO_COPC:
        # In this case, we don't care about any of these fields - they should all end up the same,
        # post-conversion. We'll check them properly then.
        return {}


def _rewrite_schema(tile_metadata, rewrite_metadata=None):
    schema = tile_metadata["schema"]
    if not rewrite_metadata or rewrite_metadata == RewriteMetadata.PRESERVE_FORMAT:
        # We care about the schema - we constrain it to be homogenous, and we write it to "schema.json"
        return schema
    elif rewrite_metadata == RewriteMetadata.PRE_CONVERT_TO_COPC:
        # We care that the schema *will* be homogenous once converted to COPC.
        # This is not guaranteed, so we'll constrain it here.
        original_pdrf = tile_metadata["format"]["pointDataRecordFormat"]
        return get_schema_from_pdrf(equivalent_copc_pdrf(original_pdrf))


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
            "Non-homogenous dataset supplied", exit_code=INVALID_FILE_FORMAT
        )


def _convert_tile_to_copc(source, dest):
    """
    Converts a LAS/LAZ file of some sort as source to a COPC.LAZ file at dest.
    """
    import pdal

    config = [
        {
            "type": "readers.las",
            "filename": str(source),
        },
        {
            "type": "writers.copc",
            "filename": str(dest),
            "forward": "all",
        },
    ]
    pipeline = pdal.Pipeline(json.dumps(config))
    try:
        pipeline.execute()
    except RuntimeError as e:
        raise InvalidOperation(
            f"Error converting {source}\n{e}", exit_code=INVALID_FILE_FORMAT
        )
    assert dest.is_file()


def _unwrap_metadata(metadata):
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    if "metadata" in metadata:
        metadata = metadata["metadata"]
    return metadata


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
    format_summary = f"{format_info['compression']}-{format_info['lasVersion']}"
    if format_info["optimization"]:
        format_summary += (
            f"/{format_info['optimization']}-{format_info['optimizationVersion']}"
        )

    # Keep tile info keys in alphabetical order.
    tile_info = {
        # PDAL seems to work best if we give it only the horizontal CRS here:
        "crs84Extent": _calc_crs84_extent(
            native_extent, horizontal_crs or compound_crs
        ),
        "format": format_summary,
        "nativeExtent": native_extent,
        "pointCount": info["count"],
    }

    result = {
        "format": format_info,
        "tile": tile_info,
        "crs": compound_crs or horizontal_crs,
    }
    if extract_schema:
        result["schema"] = _pdal_schema_to_kart_schema(
            metadata["filters.info"]["schema"]
        )

    return result


def _pdal_schema_to_kart_schema(pdal_schema):
    return [
        _pdal_col_schema_to_kart_col_schema(col) for col in pdal_schema["dimensions"]
    ]


def _pdal_col_schema_to_kart_col_schema(pdal_col_schema):
    return {
        "name": pdal_col_schema["name"],
        "dataType": _pdal_type_to_kart_type(pdal_col_schema["type"]),
        # Kart measures data-sizes in bits, PDAL in bytes.
        "size": pdal_col_schema["size"] * 8,
    }


# TODO - investigate what types PDAL can actually return - it's not the same as the LAZ spec.
# TODO - our dataset types don't have any notion of signed vs unsigned.
_PDAL_TYPE_TO_KART_TYPE = {
    "floating": "float",
    "unsigned": "integer",
    "string": "text",
}


def _pdal_type_to_kart_type(pdal_type):
    return _PDAL_TYPE_TO_KART_TYPE.get(pdal_type) or pdal_type


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
