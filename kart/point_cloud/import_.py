import logging

import click

from kart.cli_util import (
    StringFromFile,
    MutexOption,
    KartCommand,
)
from kart.crs_util import CoordinateReferenceString
from kart.completion_shared import file_path_completer
from kart.exceptions import InvalidOperation, INVALID_FILE_FORMAT
from kart.lfs_util import prefix_sha256
from kart.parse_args import parse_import_sources_and_datasets
from kart.point_cloud.metadata_util import (
    RewriteMetadata,
    rewrite_and_merge_metadata,
    is_copc,
)
from kart.point_cloud.pdal_convert import (
    convert_tile_to_copc,
    convert_tile_with_crs_override,
)
from kart.tile.importer import TileImporter
from kart.point_cloud.v1 import PointCloudV1


L = logging.getLogger(__name__)


@click.command("point-cloud-import", hidden=True, cls=KartCommand)
@click.pass_context
@click.option(
    "--convert-to-copc/--no-convert-to-copc",
    "--cloud-optimized/--no-cloud-optimized",
    "--cloud-optimised/--no-cloud-optimised",
    " /--preserve-format",
    is_flag=True,
    default=None,
    help="Whether to convert all non-COPC LAS or LAZ files to COPC LAZ files, or to import all files in their native format.",
)
@click.option(
    "--message",
    "-m",
    type=StringFromFile(encoding="utf-8"),
    help="Commit message. By default this is auto-generated.",
)
@click.option(
    "--checkout/--no-checkout",
    "do_checkout",
    is_flag=True,
    default=True,
    help=(
        "Whether to check out the dataset once the import is finished. If false, the dataset will be configured as "
        "not being checked out and will never be written to the working copy, until this decision is reversed by "
        "running `kart checkout --dataset=DATASET-PATH`."
    ),
)
@click.option(
    "--replace-existing",
    is_flag=True,
    cls=MutexOption,
    exclusive_with=["--delete", "--update-existing"],
    help="Replace existing dataset at the same path.",
)
@click.option(
    "--update-existing",
    is_flag=True,
    cls=MutexOption,
    exclusive_with=["--replace-existing"],
    help=(
        "Update existing dataset at the same path. "
        "Tiles will be replaced by source tiles with the same name. "
        "Tiles in the existing dataset which are not present in SOURCES will remain untouched."
    ),
)
@click.option(
    "--delete",
    type=StringFromFile(encoding="utf-8"),
    cls=MutexOption,
    exclusive_with=["--replace-existing"],
    multiple=True,
    help=("Deletes the given tile. Can be used multiple times."),
)
@click.option(
    "--amend",
    default=False,
    is_flag=True,
    help="Amend the previous commit instead of adding a new commit",
)
@click.option(
    "--allow-empty",
    is_flag=True,
    default=False,
    help=(
        "Usually recording a commit that has the exact same tree as its sole "
        "parent commit is a mistake, and the command prevents you from making "
        "such a commit. This option bypasses the safety"
    ),
)
@click.option(
    "--num-workers",
    "--num-processes",
    type=click.INT,
    help="How many import workers to run in parallel. Defaults to the number of available CPU cores.",
    default=None,
    hidden=True,
)
@click.option("--dataset-path", "--dataset", help="The dataset's path once imported")
@click.option(
    "--link",
    "do_link",
    is_flag=True,
    help=(
        "Link the created dataset to the original source location, so that the original source location is treated as "
        "the authoritative source for the given data and data is fetched from there if needed."
    ),
)
@click.option(
    "--override-crs",
    type=CoordinateReferenceString(keep_as_string=True),
    help=(
        "Override the CRS of all source tiles and set the dataset CRS. "
        "Can be specified as EPSG code (e.g., EPSG:4326) or as a WKT file (e.g., @myfile.wkt)."
    ),
)
@click.argument(
    "args",
    nargs=-1,
    metavar="SOURCE [SOURCES...]",
    shell_complete=file_path_completer,  # type: ignore[call-arg]
)
def point_cloud_import(
    ctx,
    convert_to_copc,
    message,
    do_checkout,
    replace_existing,
    update_existing,
    delete,
    amend,
    allow_empty,
    num_workers,
    dataset_path,
    do_link,
    override_crs,
    args,
):
    """
    Import a dataset of point-cloud tiles.

    SOURCES should be one or more LAZ or LAS files (or wildcards that match multiple LAZ or LAS files).
    """
    repo = ctx.obj.repo

    sources, datasets = parse_import_sources_and_datasets(args)
    if datasets:
        problem = "    \n".join(datasets)
        raise click.UsageError(
            f"For point-cloud import, every argument should be a LAS/LAZ file:\n    {problem}"
        )

    PointCloudImporter(
        repo=repo,
        ctx=ctx,
        dataset_path=dataset_path,
        convert_to_cloud_optimized=convert_to_copc,
        message=message,
        do_checkout=do_checkout,
        replace_existing=replace_existing,
        update_existing=update_existing,
        delete=delete,
        amend=amend,
        allow_empty=allow_empty,
        num_workers=num_workers,
        do_link=do_link,
        sources=sources,
        override_crs=override_crs,
    ).import_tiles()


class PointCloudImporter(TileImporter):
    DATASET_CLASS = PointCloudV1

    CLOUD_OPTIMIZED_VARIANT = "Cloud-Optimized Point Cloud"
    CLOUD_OPTIMIZED_VARIANT_ACRONYM = "COPC"

    def get_default_message(self):
        return f"Importing {len(self.sources)} LAZ tiles as {self.dataset_path}"

    def _is_any_las(self, all_metadata):
        return any(v["format.json"]["compression"] == "las" for v in all_metadata)

    def check_metadata_pre_convert(self):
        if not self.convert_to_cloud_optimized and self._is_any_las(
            s.metadata for s in self.tile_sources
        ):
            raise InvalidOperation(
                "LAS datasets are not supported - dataset must be converted to LAZ / COPC",
                exit_code=INVALID_FILE_FORMAT,
            )

    def check_metadata_post_convert(self):
        if self._is_any_las(s.imported_metadata for s in self.tile_sources):
            raise InvalidOperation(
                "LAS datasets are not supported - dataset must be converted to LAZ / COPC",
                exit_code=INVALID_FILE_FORMAT,
            )

    def get_merged_source_metadata(self, all_metadata):
        if self.convert_to_cloud_optimized:
            rewrite_metadata = RewriteMetadata.DROP_FORMAT | RewriteMetadata.DROP_SCHEMA
        else:
            rewrite_metadata = RewriteMetadata.DROP_OPTIMIZATION

        return rewrite_and_merge_metadata(
            all_metadata, rewrite_metadata, override_crs=self.override_crs
        )

    def get_predicted_merged_metadata(self, all_metadata):
        if self.convert_to_cloud_optimized:
            # As we check the sources for validity, we care about what the schema will be when we convert to COPC.
            # We don't need to check the format since if a set of tiles are all COPC and all have the same schema,
            # then they all will have the same format. Also, we would rather show the user that, post-conversion, the
            # tile's schema's won't match - quite a concrete idea even for those new to the LAZ format - rather than
            # trying to explain that post-conversion, the tile's Point Data Record Format numbers won't match.
            rewrite_metadata = (
                RewriteMetadata.AS_IF_CONVERTED_TO_COPC | RewriteMetadata.DROP_FORMAT
            )
        else:
            # For --preserve-format we allow both COPC and non-COPC tiles, so we don't need to check or store this information.
            rewrite_metadata = RewriteMetadata.DROP_OPTIMIZATION

        return rewrite_and_merge_metadata(
            all_metadata, rewrite_metadata, override_crs=self.override_crs
        )

    def get_actual_merged_metadata(self, all_metadata):
        rewrite_metadata = (
            RewriteMetadata.NO_REWRITE
            if self.convert_to_cloud_optimized
            else RewriteMetadata.DROP_OPTIMIZATION
        )

        return rewrite_and_merge_metadata(
            all_metadata, rewrite_metadata, override_crs=self.override_crs
        )

    def get_conversion_func(self, tile_source):
        if self.override_crs:
            # When override_crs is specified, we always need to convert
            if self.convert_to_cloud_optimized:
                # Convert to COPC (or maintain COPC) with CRS override
                return lambda source, dest: convert_tile_to_copc(
                    source, dest, override_srs=self.override_crs
                )
            else:
                # Convert with CRS override, preserving original format
                return lambda source, dest: convert_tile_with_crs_override(
                    source, dest, override_srs=self.override_crs
                )
        elif self.convert_to_cloud_optimized and not is_copc(tile_source.metadata):
            # Convert to COPC without CRS override
            return convert_tile_to_copc
        return None

    def existing_tile_matches_source(self, source_oid, existing_summary):
        """Check if the existing tile can be reused instead of reimporting."""
        # If override_crs is specified, we need to rewrite the CRS in the files,
        # so we can't reuse existing tiles
        if self.override_crs:
            return False

        source_oid = prefix_sha256(source_oid)

        if existing_summary.get("oid") == source_oid:
            # The import source we were given has already been imported in its native format.
            # Return True if that's what we would do anyway.
            if self.convert_to_cloud_optimized:
                return is_copc(existing_summary)
            else:
                return True

        # NOTE: this logic would be more complicated if we supported more than one type of conversion.
        if existing_summary.get("sourceOid") == source_oid:
            # The import source we were given has already been imported, but converted to COPC.
            # Return True if we were going to convert it to COPC too.
            return self.convert_to_cloud_optimized and is_copc(existing_summary)

        return False
