import logging

import click

from kart.cli_util import (
    StringFromFile,
    MutexOption,
    KartCommand,
)
from kart.crs_util import CoordinateReferenceString
from kart.completion_shared import file_path_completer
from kart.parse_args import parse_import_sources_and_datasets
from kart.raster.gdal_convert import convert_tile_to_cog, convert_tile_with_crs_override
from kart.raster.metadata_util import (
    rewrite_and_merge_metadata,
    is_cog,
    RewriteMetadata,
)
from kart.raster.v1 import RasterV1
from kart.tile.importer import TileImporter
from kart.tile.tilename_util import PAM_SUFFIX

L = logging.getLogger(__name__)


@click.command("raster-import", hidden=True, cls=KartCommand)
@click.option(
    "--convert-to-cog/--no-convert-to-cog",
    "--cloud-optimized/--no-cloud-optimized",
    "--cloud-optimised/--no-cloud-optimised",
    " /--preserve-format",
    is_flag=True,
    default=None,
    help="Whether to convert all GeoTIFFs to COGs (Cloud Optimized GeoTIFFs), or to import all files in their native format.",
)
@click.pass_context
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
def raster_import(
    ctx,
    convert_to_cog,
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
    Import a dataset of raster tiles.

    SOURCES should be one or more GeoTIFF files (or wildcards that match multiple GeoTIFF files).
    """
    repo = ctx.obj.repo

    sources, datasets = parse_import_sources_and_datasets(args)
    if datasets:
        problem = "    \n".join(datasets)
        raise click.UsageError(
            f"For raster import, every argument should be a GeoTIFF file:\n    {problem}"
        )

    RasterImporter(
        repo=repo,
        ctx=ctx,
        dataset_path=dataset_path,
        convert_to_cloud_optimized=convert_to_cog,
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


class RasterImporter(TileImporter):
    DATASET_CLASS = RasterV1

    CLOUD_OPTIMIZED_VARIANT = "Cloud-Optimized GeoTIFF"
    CLOUD_OPTIMIZED_VARIANT_ACRONYM = "COG"

    SIDECAR_FILES = {"pam": PAM_SUFFIX}

    def get_default_message(self):
        return f"Importing {len(self.sources)} GeoTIFF tiles as {self.dataset_path}"

    def check_metadata_pre_convert(self):
        pass

    def check_metadata_post_convert(self):
        pass

    def get_merged_source_metadata(self, all_metadata):
        return rewrite_and_merge_metadata(
            all_metadata, RewriteMetadata.DROP_PROFILE, override_crs=self.override_crs
        )

    def get_predicted_merged_metadata(self, all_metadata):
        rewrite_metadata = (
            RewriteMetadata.AS_IF_CONVERTED_TO_COG
            if self.convert_to_cloud_optimized
            else RewriteMetadata.DROP_PROFILE
        )
        return rewrite_and_merge_metadata(
            all_metadata, rewrite_metadata, override_crs=self.override_crs
        )

    def get_actual_merged_metadata(self, all_metadata):
        rewrite_metadata = (
            RewriteMetadata.NO_REWRITE
            if self.convert_to_cloud_optimized
            else RewriteMetadata.DROP_PROFILE
        )
        return rewrite_and_merge_metadata(
            all_metadata, rewrite_metadata, override_crs=self.override_crs
        )

    def get_conversion_func(self, tile_source):
        if self.override_crs:
            # When override_crs is specified, we always need to convert
            if self.convert_to_cloud_optimized:
                # Convert to COG (or maintain COG) with CRS override
                return lambda source, dest: convert_tile_to_cog(
                    source, dest, override_srs=self.override_crs
                )
            else:
                # Convert with CRS override, preserving original format
                return lambda source, dest: convert_tile_with_crs_override(
                    source, dest, override_srs=self.override_crs
                )
        elif self.convert_to_cloud_optimized and not is_cog(tile_source.metadata):
            # Convert to COG without CRS override
            return convert_tile_to_cog
        return None

    def _is_cloud_optimized(self, tile_summary):
        return is_cog(tile_summary)
