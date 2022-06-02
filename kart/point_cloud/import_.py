import json
import logging
import os
from pathlib import Path
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
from kart.output_util import format_wkt_for_output
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

    first_tile_metadata = None
    source_to_metadata = {}

    for source in sources:
        click.echo(f"Checking {source}...          \r", nl=False)

        extract_schema = first_tile_metadata is None
        metadata = extract_pc_tile_metadata(source, extract_schema=extract_schema)
        source_to_metadata[source] = metadata

        if first_tile_metadata is None:
            first_tile_metadata = metadata
        else:
            check_for_non_homogenous_metadata(first_tile_metadata, metadata)

    click.echo()

    version = first_tile_metadata["version"]
    copc_version = first_tile_metadata["copc-version"]
    is_laz = first_tile_metadata["compressed"] is True
    is_copc = is_laz and copc_version != NOT_COPC

    if is_copc:
        # Keep native format.
        conversion_func = None
        import_format = f"pc:v1/copc-{copc_version}.0"
    elif is_laz:
        # Optionally Convert to COPC 1.0 if requested
        conversion_func = _convert_tile_to_copc if convert_to_copc else None
        import_format = "pc:v1/copc-1.0" if convert_to_copc else f"pc:v1/laz-{version}"
    else:  # LAS
        if not convert_to_copc:
            raise InvalidOperation(
                "LAS datasets are not supported - dataset must be converted to LAZ / COPC",
                exit_code=INVALID_FILE_FORMAT,
            )
        conversion_func = _convert_tile_to_copc
        import_format = "pc:v1/copc-1.0"

    import_ext = ".copc.laz" if "copc" in import_format else ".laz"

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
                pc_tile_metadata_to_pointer_metadata(source_to_metadata[source])
            )
            pointer_dict["format"] = import_format
            # TODO - is this the right prefix and name?
            tilename = os.path.splitext(os.path.basename(source))[0] + import_ext
            tile_prefix = hexhash(tilename)[0:2]
            blob_path = f"{ds_inner_path}/tile/{tile_prefix}/{tilename}"
            write_blob_to_stream(
                proc.stdin, blob_path, dict_to_pointer_file_bytes(pointer_dict)
            )

        write_blob_to_stream(
            proc.stdin,
            f"{ds_inner_path}/meta/schema.json",
            json_pack(first_tile_metadata["schema"]),
        )
        write_blob_to_stream(
            proc.stdin,
            f"{ds_inner_path}/meta/crs.wkt",
            ensure_bytes(normalise_wkt(first_tile_metadata["crs"])),
        )

    parts_to_create = [PartType.WORKDIR] if do_checkout else []
    # During imports we can keep old changes since they won't conflict with newly imported datasets.
    repo.working_copy.reset_to_head(
        repo_key_filter=RepoKeyFilter.datasets([ds_path]),
        create_parts_if_missing=parts_to_create,
    )


def _format_array(array):
    return json.dumps(array, separators=(",", ":"))[1:-1]


# The COPC version number we use for any LAZ / LAS file that is not actually COPC.
NOT_COPC = "NOT COPC"


def get_copc_version(info):
    if info.get("copc"):
        # PDAL now hides the COPC VLR from us so we can't do better than this without peeking at the file directly.
        # See https://github.com/PDAL/PDAL/blob/3e33800d85d48f726dcd0931cefe062c4af2b573/io/private/las/Vlr.cpp#L53
        return "1.0"
    else:
        return NOT_COPC


def get_native_extent(info):
    return (
        info["minx"],
        info["maxx"],
        info["miny"],
        info["maxy"],
        info["minz"],
        info["maxz"],
    )


def check_for_non_homogenous_metadata(m1, m2):
    """
    Given two sets of point-cloud metadata - as extracted by extract_pc_tile_metadata - raises an error
    if there is any metadata which is non-homogenous (eg "version" - other fields like "extent" are allowed to vary).
    """

    _check_for_non_homogenous_field(m1, m2, "version")
    _check_for_non_homogenous_field(
        m1, m2, "compressed", "compression", disparity="LAS vz LAZ"
    )
    _check_for_non_homogenous_field(m1, m2, "copc-version", "COPC version")
    _check_for_non_homogenous_field(
        m1, m2, "point-data-record-format", "Point Data Record Format (PDRF)"
    )
    _check_for_non_homogenous_field(
        m1, m2, "point-data-record-length", "Point Data Record Format (PDRF)"
    )
    # Do CRS a bit differently so we can format the output.
    if m1["crs"] != m2["crs"]:
        disparity = "\n vs \n".join(
            (format_wkt_for_output(wkt, sys.stderr) for wkt in [m1["crs"], m2["crs"]])
        )
        _error_for_non_homogenous_field(m1, m2, "crs", "CRS", disparity)


def _check_for_non_homogenous_field(m1, m2, key, name=None, disparity=None):
    v1 = m1.get(key)
    v2 = m2.get(key)
    if v1 != v2:
        _error_for_non_homogenous_field(v1, v2, name or key, disparity)


def _error_for_non_homogenous_field(v1, v2, name, disparity):
    if disparity is None:
        disparity = f"{v1} vs {v2}"
    click.echo()  # Go to next line to get past the progress output.
    click.echo("Only the import of homogenous datasets is supported.", err=True)
    click.echo(f"The input files have more than one {name}:", err=True)
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
    calc_crs84_extent=True,
    extract_schema=False,
):
    """
    Use pdal to get any and all point-cloud metadata we can make use of in Kart.
    This can include metadata must be dataset-homogenous and would be stored in the dataset's /meta/ folder,
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
    native_crs = info["srs"]["wkt"]
    result = {
        "compressed": info["compressed"],
        "version": f"{info['major_version']}.{info['minor_version']}",
        "copc-version": get_copc_version(info),
        "point-data-record-format": info["dataformat_id"],
        "point-data-record-length": info["point_length"],
        "crs": native_crs,
        "native-extent": native_extent,
        "count": info["count"],
    }
    if extract_schema:
        result["schema"] = _pdal_schema_to_kart_schema(
            metadata["filters.info"]["schema"]
        )
    if calc_crs84_extent:
        crs84_extent = _calc_crs84_extent(native_extent, native_crs)
        if crs84_extent is not None:
            result["crs84-extent"] = crs84_extent
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
            ('X', np.dtype(float)),
            ('Y', np.dtype(float)),
            ('Z', np.dtype(float)),
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
    b = metadata['filters.stats']['bbox']['EPSG:4326']['bbox']
    return b["minx"], b["maxx"], b["miny"], b["maxy"], b["minz"], b["maxz"]


def pc_tile_metadata_to_pointer_metadata(metadata):
    """
    Given all the tile-metadata, returns thats which should be written to an LFS pointer file,
    in the appropriate format.
    """
    # Keep these keys in alphabetical order.
    result = {
        "extent.crs84": _format_array(metadata["crs84-extent"])
        if "crs84-extent" in metadata
        else None,
        "extent.native": _format_array(metadata["native-extent"]),
        "format": _pc_tile_metadata_to_kart_format(metadata),
        "points.count": metadata["count"],
    }
    if result["extent.crs84"] is None:
        del result["extent.crs84"]
    return result


def _pc_tile_metadata_to_kart_format(metadata):
    if metadata["copc-version"] == NOT_COPC:
        return f"pc:v1/laz-{metadata['version']}"
    else:
        return f"pc:v1/copc-{metadata['copc-version']}"
