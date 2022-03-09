import json
import hashlib
import os
from pathlib import Path
import uuid
import subprocess
import sys

import click
from osgeo import osr

from kart.crs_util import make_crs
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
from kart.serialise_util import hexhash
from kart.output_util import format_wkt_for_output
from kart.repo_version import (
    SUPPORTED_REPO_VERSIONS,
    extra_blobs_for_version,
)


@click.command("point-cloud-import", hidden=True)
@click.pass_context
@click.argument("sources", metavar="SOURCES", nargs=-1, required=True)
def point_cloud_import(ctx, sources):
    """
    Experimental command for importing point cloud datasets. Work-in-progress.
    Will eventually be merged with the main `import` command.

    SOURCES should be one or more LAZ or LAS files (or wildcards that match multiple LAZ or LAS files).
    """
    import pdal

    repo = ctx.obj.repo

    for source in sources:
        if not (Path() / source).is_file():
            raise NotFound(f"No data found at {source}", exit_code=NO_IMPORT_SOURCE)

    compressed_set = ListBasedSet()
    version_set = ListBasedSet()
    copc_version_set = ListBasedSet()
    pdrf_set = ListBasedSet()
    crs_set = ListBasedSet()
    transform = None

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
        pipeline = pdal.Pipeline(json.dumps(config))
        try:
            pipeline.execute()
        except RuntimeError:
            raise InvalidOperation(
                f"Error reading {source}", exit_code=INVALID_FILE_FORMAT
            )

        info = json.loads(pipeline.metadata)["metadata"]["readers.las"]

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

        crs_set.add(info["srs"]["wkt"])
        if len(crs_set) > 1:
            raise _non_homogenous_error(
                "CRS",
                "\n vs \n".join(
                    (format_wkt_for_output(wkt, sys.stderr) for wkt in crs_set)
                ),
            )

        if transform is None:
            src_crs = make_crs(crs_set[0])
            target_crs = make_crs("EPSG:4326")
            transform = osr.CoordinateTransformation(src_crs, target_crs)

        native_envelope = get_native_envelope(info)
        crs84_envelope = _transform_3d_envelope(transform, native_envelope)
        per_source_info[source] = {
            "count": info["count"],
            "native_envelope": native_envelope,
            "crs84_envelope": crs84_envelope,
        }

    click.echo()

    # Set up LFS hooks.
    # TODO: This could eventually be moved to `kart init`.
    r = subprocess.check_call(
        ["git", "-C", str(repo.gitdir_path), "lfs", "install", "hooks"]
    )

    # TODO: Find a proper dataset name or let the user supply it:
    if len(sources) == 1:
        ds_path = os.path.basename(os.path.splitext(sources[0])[0])
    else:
        ds_path = os.path.basename(os.path.commonprefix(sources)).rstrip("-_.")

    # We still need to write .kart.repostructure.version unfortunately, even though it's only relevant to tabular datasets.
    assert repo.version in SUPPORTED_REPO_VERSIONS
    extra_blobs = extra_blobs_for_version(repo.version) if not repo.head_commit else []

    header = generate_header(
        repo,
        None,
        f"Importing {len(sources)} point-cloud tiles as {ds_path}",
        repo.head_branch,
        repo.head_commit,
    )

    # TODO: Don't accept all possible versions of everything / maybe convert everything to COPC-1.0
    copc_version = copc_version_set[0]
    if copc_version == 1:
        kart_format = "pc:v1/copc-1.0"
    elif copc_version != NOT_COPC:
        kart_format = f"pc:DEV/copc-{copc_version}"
    else:
        filetype = "laz" if compressed_set[0] is True else "las"
        kart_format = f"pc:DEV/{filetype}-{version_set[0]}"

    lfs_objects_path = repo.gitdir_path / "lfs" / "objects"
    lfs_tmp_import_path = lfs_objects_path / "import"
    lfs_tmp_import_path.mkdir(parents=True, exist_ok=True)

    with git_fast_import(repo, *FastImportSettings().as_args(), "--quiet") as proc:
        proc.stdin.write(header.encode("utf8"))

        for i, blob_path in write_blobs_to_stream(proc.stdin, extra_blobs):
            pass

        for source in sources:
            click.echo(f"Writing {source}...          \r", nl=False)

            tmp_object_path = lfs_tmp_import_path / str(uuid.uuid4())
            oid, size = _copy_and_get_sha256_and_size(source, tmp_object_path)
            actual_object_path = lfs_objects_path / oid[0:2] / oid[2:4] / oid
            actual_object_path.parents[0].mkdir(parents=True, exist_ok=True)
            tmp_object_path.rename(actual_object_path)

            # TODO - is this the right prefix and name?
            tilename = os.path.basename(source)
            tile_prefix = hexhash(tilename)[0:2]
            blob_path = (
                f"{ds_path}/.point-cloud-dataset.v1/tiles/{tile_prefix}/{tilename}"
            )
            info = per_source_info[source]
            pointer_dict = {
                "version": "https://git-lfs.github.com/spec/v1",
                # TODO - available.<URL-IDX> <URL>
                "kart.extent.crs84": _format_array(info["crs84_envelope"]),
                "kart.extent.native": _format_array(info["native_envelope"]),
                "kart.format": kart_format,
                "kart.pc.count": info["count"],
                "oid": f"sha256:{oid}",
                "size": size,
            }
            write_pointer_file_to_stream(proc.stdin, blob_path, pointer_dict)

    click.echo()


def _format_array(array):
    return json.dumps(array, separators=(",", ":"))[1:-1]


def write_pointer_file_to_stream(stream, blob_path, pointer_dict):
    def sort_key(key_value):
        key, value = key_value
        if key == "version":
            return ""
        return key

    blob = bytearray()
    for key, value in sorted(pointer_dict.items(), key=sort_key):
        # TODO - LFS doesn't support our fancy pointer files yet. Hopefully fix this in LFS.
        if key not in ("version", "oid", "size"):
            continue
        blob += key.encode("utf8")
        blob += b" "
        blob += str(value).encode("utf8")
        blob += b"\n"

    write_blob_to_stream(stream, blob_path, blob)


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


def _transform_3d_envelope(transform, envelope):
    x0, y0, z0 = transform.TransformPoint(*envelope[0::2])
    x1, y1, z1 = transform.TransformPoint(*envelope[1::2])
    return min(x0, x1), max(x0, x1), min(y0, y1), max(y0, y1), min(z0, z1), max(z0, z1)


def _copy_and_get_sha256_and_size(src, dest):
    BUF_SIZE = 65536
    sha256 = hashlib.sha256()
    size = Path(src).stat().st_size
    with open(str(src), "rb") as input:
        with open(str(dest), "wb") as output:
            while True:
                data = input.read(BUF_SIZE)
                if not data:
                    break
                sha256.update(data)
                output.write(data)
    return sha256.hexdigest(), size


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

    def __contains__(self, element):
        return element in self.list

    def __len__(self):
        return len(self.list)

    def __iter__(self):
        return iter(self.list)

    def __getitem__(self, key):
        return self.list[key]
