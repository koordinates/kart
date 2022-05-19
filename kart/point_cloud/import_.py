import json
import os
from pathlib import Path
import sys

import click

from .checkout import reset_wc_if_needed
from kart.crs_util import get_identifier_str, normalise_wkt
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
@click.argument("sources", metavar="SOURCES", nargs=-1, required=True)
def point_cloud_import(ctx, convert_to_copc, ds_path, sources):
    """
    Experimental command for importing point cloud datasets. Work-in-progress.
    Will eventually be merged with the main `import` command.

    SOURCES should be one or more LAZ or LAS files (or wildcards that match multiple LAZ or LAS files).
    """
    import pdal

    repo = ctx.obj.repo

    # TODO - improve path validation to make sure datasets of any type don't collide with each other
    # or with attachments.
    validate_dataset_paths([ds_path])

    for source in sources:
        if not (Path() / source).is_file():
            raise NotFound(f"No data found at {source}", exit_code=NO_IMPORT_SOURCE)

    compressed_set = ListBasedSet()
    version_set = ListBasedSet()
    copc_version_set = ListBasedSet()
    pdrf_set = ListBasedSet()
    pdr_length_set = ListBasedSet()
    crs_set = ListBasedSet()
    transform = None
    schema = None
    crs_name = None

    per_source_info = {}

    for source in sources:
        click.echo(f"Checking {source}...          \r", nl=False)
        config = [
            {
                "type": "readers.las",
                "filename": source,
                "count": 0,  # Don't read any individual points.
            }
        ]
        if schema is None:
            config.append({"type": "filters.info"})

        pipeline = pdal.Pipeline(json.dumps(config))
        try:
            pipeline.execute()
        except RuntimeError:
            raise InvalidOperation(
                f"Error reading {source}", exit_code=INVALID_FILE_FORMAT
            )

        metadata = _unwrap_metadata(pipeline.metadata)

        info = metadata["readers.las"]

        compressed_set.add(info["compressed"])
        if len(compressed_set) > 1:
            raise _non_homogenous_error("filetype", "LAS vs LAZ")

        version = f"{info['major_version']}.{info['minor_version']}"
        version_set.add(version)
        if len(version_set) > 1:
            raise _non_homogenous_error("version", version_set)

        copc_version_set.add(get_copc_version(info))
        if len(copc_version_set) > 1:
            raise _non_homogenous_error("COPC version", copc_version_set)

        pdrf_set.add(info["dataformat_id"])
        if len(pdrf_set) > 1:
            raise _non_homogenous_error("Point Data Record Format", pdrf_set)

        pdr_length_set.add(info["point_length"])
        if len(pdr_length_set) > 1:
            raise _non_homogenous_error("Point Data Record Length", pdr_length_set)

        crs_set.add(info["srs"]["wkt"])
        if len(crs_set) > 1:
            raise _non_homogenous_error(
                "CRS",
                "\n vs \n".join(
                    (format_wkt_for_output(wkt, sys.stderr) for wkt in crs_set)
                ),
            )

        if transform is None:
            transform = _make_transform_to_crs84(crs_set.only())

        native_envelope = get_native_envelope(info)
        crs84_envelope = _transform_3d_envelope(transform, native_envelope)
        per_source_info[source] = {
            "count": info["count"],
            "native_envelope": native_envelope,
            "crs84_envelope": crs84_envelope,
        }

        if schema is None:
            crs_name = get_identifier_str(crs_set.only())
            schema = metadata["filters.info"]["schema"]
            schema["CRS"] = crs_name

    click.echo()

    version = version_set.only()
    copc_version = copc_version_set.only()
    is_laz = compressed_set.only() is True
    is_copc = is_laz and copc_version != NOT_COPC

    if is_copc:
        # Keep native format.
        conversion_func = None
        kart_format = f"pc:v1/copc-{copc_version}.0"
    elif is_laz:
        # Optionally Convert to COPC 1.0 if requested
        conversion_func = _convert_tile_to_copc if convert_to_copc else None
        kart_format = "pc:v1/copc-1.0" if convert_to_copc else f"pc:v1/laz-{version}"
    else:  # LAS
        if not convert_to_copc:
            raise InvalidOperation(
                "LAS datasets are not supported - dataset must be converted to LAZ / COPC",
                exit_code=INVALID_FILE_FORMAT,
            )
        conversion_func = _convert_tile_to_copc
        kart_format = "pc:v1/copc-1.0"

    import_ext = ".copc.laz" if "copc" in kart_format else ".laz"

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

            # TODO - is this the right prefix and name?
            tilename = os.path.splitext(os.path.basename(source))[0] + import_ext
            tile_prefix = hexhash(tilename)[0:2]
            blob_path = f"{ds_inner_path}/tile/{tile_prefix}/{tilename}"
            info = per_source_info[source]
            pointer_dict.update(
                {
                    # TODO - available.<URL-IDX> <URL>
                    "kart.extent.crs84": _format_array(info["crs84_envelope"]),
                    "kart.extent.native": _format_array(info["native_envelope"]),
                    "kart.format": kart_format,
                    "kart.pc.count": info["count"],
                }
            )
            write_blob_to_stream(
                proc.stdin, blob_path, dict_to_pointer_file_bytes(pointer_dict)
            )

        write_blob_to_stream(
            proc.stdin, f"{ds_inner_path}/meta/schema.json", json_pack(schema)
        )
        write_blob_to_stream(
            proc.stdin,
            f"{ds_inner_path}/meta/crs/{crs_name}.wkt",
            ensure_bytes(normalise_wkt(crs_set.only())),
        )

    click.echo("Updating working copy...")
    reset_wc_if_needed(repo)

    # TODO - fix up reset code - there should be a single function you can call that updates all working copies.
    tabular_wc = repo.working_copy
    if tabular_wc is not None:
        tabular_wc.reset(repo.head_commit)


def _unwrap_metadata(metadata):
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    if "metadata" in metadata:
        metadata = metadata["metadata"]
    return metadata


def _format_array(array):
    return json.dumps(array, separators=(",", ":"))[1:-1]


# The COPC version number we use for any LAZ / LAS file that is not actually COPC.
NOT_COPC = "NOT COPC"


def get_copc_version(info):
    vlr_0 = info.get("vlr_0")
    if vlr_0:
        user_id = vlr_0.get("user_id")
        if user_id == "copc":
            return vlr_0.get("record_id")
    return NOT_COPC


def get_native_envelope(info):
    def _get_native_envelope_for_coord(coord):
        min_coord = (
            info[f"min{coord}"] * info[f"scale_{coord}"] + info[f"offset_{coord}"]
        )
        max_coord = (
            info[f"max{coord}"] * info[f"scale_{coord}"] + info[f"offset_{coord}"]
        )
        return min_coord, max_coord

    min_x, max_x = _get_native_envelope_for_coord("x")
    min_y, max_y = _get_native_envelope_for_coord("y")
    min_z, max_z = _get_native_envelope_for_coord("z")
    return min_x, max_x, min_y, max_y, min_z, max_z


def _non_homogenous_error(attribute_name, detail):
    if not isinstance(detail, str):
        detail = " vs ".join(str(d) for d in detail)

    click.echo()  # Go to next line to get past the progress output.
    click.echo("Only the import of homogenous datasets is supported.", err=True)
    click.echo(f"The input files have more than one {attribute_name}:", err=True)
    click.echo(detail, err=True)
    raise InvalidOperation(
        "Non-homogenous dataset supplied", exit_code=INVALID_FILE_FORMAT
    )


def _make_transform_to_crs84(src_wkt):
    # TODO - use pdal to transform from src_wkt to EPSG:4326
    return None


def _transform_3d_envelope(transform, envelope):
    # TODO - actually transform this envelope
    return envelope


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


class ListBasedSet:
    """
    A basic set that doesn't use hashing, so it can contain dicts.
    Very inefficient for lots of elements, perfect for one or two elements.
    """

    def __init__(self):
        self.list = []

    def add(self, element):
        if element not in self.list:
            self.list.append(element)

    def only(self):
        """Return the only element in this collection, or raise a LookupError."""
        if len(self.list) != 1:
            raise LookupError(
                f"Can't return only element: set contains {len(self.list)} elements"
            )
        return self.list[0]

    def __contains__(self, element):
        return element in self.list

    def __len__(self):
        return len(self.list)

    def __iter__(self):
        return iter(self.list)
