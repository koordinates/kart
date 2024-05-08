import os
import re

import click
from osgeo import gdal, ogr

from kart.cli_util import (
    call_and_exit_flag,
    KartCommand,
)
from kart.completion_shared import repo_path_completer
from kart.crs_util import make_crs
from kart.exceptions import CrsError
from kart.tabular.ogr_adapter import (
    kart_schema_col_to_ogr_field_definition,
    KART_GEOM_TYPE_TO_OGR_GEOM_TYPE,
)


def is_writable_vector_format(driver):
    m = driver.GetMetadata()
    return m.get("DCAP_VECTOR") == "YES" and m.get("DCAP_CREATE") == "YES"


def list_export_formats(ctx):
    """List the supported export formats as reported by gdal."""

    for i in range(ogr.GetDriverCount()):
        d = ogr.GetDriver(i)
        if is_writable_vector_format(d):
            click.echo(f"    {d.GetName()}: {d.GetMetadataItem(gdal.DMD_LONGNAME)}")


def get_driver_by_shortname(shortname):
    shortname = shortname.upper()
    d = ogr.GetDriverByName(shortname)
    if d is None:
        raise click.UsageError(f"Destination format {shortname} not recognized")
    if not is_writable_vector_format(d):
        raise click.UsageError(f"Driver {shortname} does not support writing vectors")
    return d


def get_driver_by_ext(filename):
    if "." not in filename:
        raise click.UsageError(
            f"Destination format for filename {filename} not recognized"
        )

    ext = os.path.splitext(filename)[1][1:].upper()

    for i in range(ogr.GetDriverCount()):
        d = ogr.GetDriver(i)
        exts = d.GetMetadataItem(gdal.DMD_EXTENSIONS)
        if is_writable_vector_format(d) and exts:
            exts = exts.upper().split(" ")
            if ext in exts:
                return d
    raise click.UsageError(f"Destination format for filename {filename} not recognized")


def get_driver(destination_spec):
    """
    Given a destination like target.gpkg, GPKG:target.gpkg, postgresql://x/y/z,
    returns the driver to use and the target that the driver should write to.
    """
    match = re.match(r'([^:]+)://', destination_spec)
    if match:
        return get_driver_by_shortname(match.group(1)), destination_spec
    if ":" in destination_spec:
        shortname, destination = destination_spec.split(":", maxsplit=1)
        return get_driver_by_shortname(shortname), destination
    return get_driver_by_ext(destination_spec), destination_spec


@click.command(
    "table-export",
    cls=KartCommand,
    hidden=True,
    context_settings=dict(ignore_unknown_options=True),
)
@click.pass_context
@call_and_exit_flag(
    "--list-formats",
    callback=list_export_formats,
    help="List available export formats, and then exit",
)
@click.option(
    "--ref", default="HEAD", help="The revision at which to export the dataset."
)
@click.option(
    "--layer",
    help="Name of the layer to create inside the given destination. Defaults to the dataset path.",
)
@click.option(
    "--layer-creation-option",
    "-lco",
    multiple=True,
    help="A layer creation option (-lco) flag forwarded to the GDAL OGR driver. Example: -lco VERSION=1.0",
)
@click.option(
    "--dataset-creation-option",
    "-dsco",
    multiple=True,
    help="A dataset creation option (-dsco) flag forwarded to the GDAL OGR driver. Example: -dsco LAUNDER=YES",
)
@click.option(
    "--primary-key-as-field/--no-primary-key-as-field",
    is_flag=True,
    default=True,
    help=(
        "Include the primary key column as a regular field that is sent to the GDAL driver. Defaults to on. "
        "Turning this off may be preferable for GDAL drivers which adequately store the FID - in that case supplying "
        "it as a field also would cause it to be stored redundantly, twice per feature."
    ),
)
@click.option(
    "--primary-key-as-fid/--no-primary-key-as-fid",
    is_flag=True,
    default=None,
    help=(
        "Report to the GDAL driver that the FID of each feature is the primary key value. By default this is on "
        "for datasets which have an integer primary key, and off for other primary key types (which GDAL will not"
        "accept as a valid FID). Depending on the driver, GDAL may or may not store the FID in the output dataset in "
        "a recoverable way (for some export types the first FID must be one, in others it can be any value)."
    ),
)
@click.option(
    "--override-geometry-type",
    help="Override the geometry type to something more specific than in the Kart dataset.",
)
@click.option(
    "--drop-null-geometry-features",
    is_flag=True,
    help="Skips export of those features where the geometry is null.",
)
@click.argument(
    "args",
    nargs=-1,
    metavar="DATASET [EXPORT_TYPE:]DESTINATION",
    shell_complete=repo_path_completer,
)
def table_export(
    ctx,
    ref,
    layer,
    layer_creation_option,
    dataset_creation_option,
    primary_key_as_field,
    primary_key_as_fid,
    override_geometry_type,
    drop_null_geometry_features,
    args,
    **kwargs,
):
    """
    Experimental export command - exports a tabular kart dataset at a particular commit.

    Uses GDAL's OGR drivers to do so - consult https://gdal.org/drivers/vector/index.html
    to find the options specific to a particular driver, which can then be set using
    -dsco and -lco just as when using `ogr2ogr`. If these are not set, GDAL's defaults
    are used, which can also be found in the documentation.

    GDAL's general configuration options can be controlled by setting environment variables -
    see https://gdal.org/user/configoptions.html
    """
    if len(args) != 2:
        raise click.UsageError(
            "Usage: kart table-export DATASET [EXPORT_TYPE:]DESTINATION"
        )

    repo = ctx.obj.repo

    ds_path = args[0]
    destination_spec = args[1]

    try:
        dataset = repo.datasets(ref)[ds_path]
    except KeyError:
        raise click.UsageError(f"No such dataset: {ds_path}")

    if dataset.DATASET_TYPE != "table":
        raise click.UsageError(f"Dataset {ds_path} is not a vector or tabular dataset")

    driver, destination = get_driver(destination_spec)
    out_ds = driver.CreateDataSource(destination, options=list(dataset_creation_option))
    layer_name = layer or ds_path

    if (
        override_geometry_type
        and override_geometry_type.upper() not in KART_GEOM_TYPE_TO_OGR_GEOM_TYPE
    ):
        click.echo("Geometry type should be one of:", err=True)
        click.echo("\n".join(KART_GEOM_TYPE_TO_OGR_GEOM_TYPE.keys()), err=True)
        raise click.UsageError(f"Unknown geometry type: {override_geometry_type}")

    schema = dataset.schema
    pk_name = schema.first_pk_column.name
    primary_key_as_fid = _validate_primary_key_as_fid(primary_key_as_fid, schema)

    geom_key = None
    ogr_geom_type = None
    ogr_crs = None

    if schema.has_geometry:
        geom_col = schema.geometry_columns[0]
        geom_key = geom_col.name
        kart_geom_type = override_geometry_type or geom_col["geometryType"]
        ogr_geom_type = KART_GEOM_TYPE_TO_OGR_GEOM_TYPE[kart_geom_type.upper()]

        crs_defs = list(dataset.crs_definitions().values())
        if len(crs_defs) > 1:
            raise CrsError(
                f"Sorry, multiple CRS definitions at {ds_path!r} are not yet supported for export"
            )
        if crs_defs:
            ogr_crs = make_crs(crs_defs[0], context=ds_path)

        out_layer = out_ds.CreateLayer(
            layer_name, ogr_crs, ogr_geom_type, options=list(layer_creation_option)
        )
    else:
        out_layer = out_ds.CreateLayer(layer_name, options=list(layer_creation_option))

    regular_keys = []
    for col in dataset.schema:
        if col.name == pk_name and not primary_key_as_field:
            continue
        if col.data_type == "geometry":
            continue  # Handled separately
        regular_keys.append(col.name)
        out_layer.CreateField(kart_schema_col_to_ogr_field_definition(col))

    for feature in dataset.features_with_crs_ids(
        repo.spatial_filter, show_progress=True
    ):
        out_feature = ogr.Feature(out_layer.GetLayerDefn())
        if primary_key_as_fid:
            out_feature.SetFID(feature[pk_name])
        if geom_key:
            geom = feature[geom_key]
            if geom is None and drop_null_geometry_features:
                continue
            out_feature.SetGeometry(geom.to_ogr() if geom else None)
        for i, key in enumerate(regular_keys):
            out_feature.SetField(i, feature[key])
        out_layer.CreateFeature(out_feature)

    out_ds = None


def _validate_primary_key_as_fid(primary_key_as_fid, schema):
    if schema.pk_columns[0].data_type == "integer":
        return primary_key_as_fid if primary_key_as_fid is not None else True
    if primary_key_as_fid is True:
        raise click.UsageError(
            "Sorry, --primary-key-as-fid is not supported for datasets with non-integer primary keys"
        )
    return False
