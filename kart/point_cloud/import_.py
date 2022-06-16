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


L = logging.getLogger(__name__)


@click.command("point-cloud-import", hidden=True)
@click.pass_context
@click.option(
    "--convert-to-copc/--no-convert-to-copc",
    is_flag=True,
    default=True,
    help="Convert non-COPC LAS or LAZ files to COPC LAZ files",
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

    metadata = merge_metadata(source_to_metadata.values())
    check_for_non_homogenous_metadata(metadata)

    compression = metadata["fileInfo"]["compression"]
    optimization = metadata["fileInfo"]["optimization"]

    if optimization == "copc":
        # Keep native format.
        convert_to_copc = False
        import_ext = ".copc.laz"
    elif compression == "laz":
        # Optionally Convert to COPC 1.0 if requested
        import_ext = ".copc.laz" if convert_to_copc else ".laz"
    else:  # LAS
        if not convert_to_copc:
            raise InvalidOperation(
                "LAS datasets are not supported - dataset must be converted to LAZ / COPC",
                exit_code=INVALID_FILE_FORMAT,
            )
        import_ext = ".copc.laz"

    if convert_to_copc:
        # Re-extract the metadata after conversion - we want to store the metadata of the dataset,
        # not of what the tiles were before we converted them.
        metadata = None

        def convert_tile_to_copc_and_reextract_metadata(source, dest):
            nonlocal metadata

            _convert_tile_to_copc(source, dest)
            if metadata is None:
                metadata = extract_pc_tile_metadata(dest, extract_schema=True)

        conversion_func = convert_tile_to_copc_and_reextract_metadata
    else:
        conversion_func = None

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

    with git_fast_import(repo, *FastImportSettings().as_args(), "--quiet") as proc:
        proc.stdin.write(header.encode("utf8"))

        for i, blob_path in write_blobs_to_stream(proc.stdin, extra_blobs):
            pass

        for source in sources:
            click.echo(f"Importing {source}...")

            pointer_dict = copy_file_to_local_lfs_cache(repo, source, conversion_func)
            pointer_dict.update(
                format_tile_info_for_pointer_file(
                    source_to_metadata[source]["tileInfo"]
                )
            )
            # TODO - is this the right prefix and name?
            tilename = _remove_las_ext(os.path.basename(source)) + import_ext
            tile_prefix = hexhash(tilename)[0:2]
            blob_path = f"{ds_inner_path}/tile/{tile_prefix}/{tilename}"
            write_blob_to_stream(
                proc.stdin, blob_path, dict_to_pointer_file_bytes(pointer_dict)
            )

        write_blob_to_stream(
            proc.stdin,
            f"{ds_inner_path}/meta/fileInfo.json",
            json_pack(metadata["fileInfo"]),
        )
        write_blob_to_stream(
            proc.stdin,
            f"{ds_inner_path}/meta/schema.json",
            json_pack(metadata["schema"]),
        )
        write_blob_to_stream(
            proc.stdin,
            f"{ds_inner_path}/meta/crs.wkt",
            ensure_bytes(normalise_wkt(metadata["crs"])),
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
    pass


def merge_metadata(tile_metadata_list):
    result = {}
    for tile_metadata in tile_metadata_list:
        _merge_metadata_field(result, "fileInfo", tile_metadata["fileInfo"])
        _merge_metadata_field(result, "schema", tile_metadata["schema"])
        _merge_metadata_field(result, "crs", tile_metadata["crs"])
    return result


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


def check_for_non_homogenous_metadata(merged_metadata):
    _check_for_non_homogenous_meta_item(merged_metadata, "fileInfo", "type of file")
    _check_for_non_homogenous_meta_item(merged_metadata, "schema", "schema")
    _check_for_non_homogenous_meta_item(merged_metadata, "crs", "CRS")


def _check_for_non_homogenous_meta_item(merged_metadata, key, output_name):
    value = merged_metadata[key]

    if isinstance(value, ListOfConflicts):
        format_func = format_wkt_for_output if key == "crs" else format_json_for_output
        disparity = " vs \n".join(
            (
                format_func(file_info, sys.stderr)
                for file_info in merged_metadata["fileInfo"]
            )
        )
        click.echo("Only the import of homogenous datasets is supported.", err=True)
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
    file_info = {
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
        "nativeExtent": native_extent,
        "pointCount": info["count"],
    }

    result = {
        "fileInfo": file_info,
        "tileInfo": tile_info,
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
        "nativeExtent": _format_array(tile_info["nativeExtent"]),
        "pointCount": tile_info["pointCount"],
    }
    if result["crs84Extent"] is None:
        del result["crs84Extent"]
    return result
