import os
from pathlib import Path
import re

import click
from osgeo import gdal, ogr, osr

from kart.cli_util import (
    call_and_exit_flag,
    KartCommand,
)
from kart.completion_shared import repo_path_completer
from kart.crs_util import make_crs, CoordinateReferenceString
from kart.exceptions import BaseException, InvalidOperation, CrsError
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
    match = re.match(r"([^:]+)://", destination_spec)
    if match:
        return get_driver_by_shortname(match.group(1)), destination_spec
    if match := re.match(r"([^:]{2,}):(.+)", destination_spec):
        shortname, destination = match.groups()
        return get_driver_by_shortname(shortname), destination
    return get_driver_by_ext(destination_spec), destination_spec


@click.command(
    "table-export",
    cls=KartCommand,
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
    "layer_name",
    help="Name of the layer to create inside the given destination. Defaults to the dataset path.",
)
@click.option(
    "--crs",
    "output_crs",
    type=CoordinateReferenceString(encoding="utf-8"),
    help="Reproject geometries into the given coordinate reference system. Accepts: 'EPSG:<code>'; proj text; OGC WKT; OGC URN; PROJJSON.)",
)
@click.option(
    "--dataset-creation-option",
    "-dsco",
    "dataset_creation_options",
    multiple=True,
    help="A dataset creation option (-dsco) flag forwarded to the GDAL OGR driver. Example: -dsco LAUNDER=YES",
)
@click.option(
    "--layer-creation-option",
    "-lco",
    "layer_creation_options",
    multiple=True,
    help="A layer creation option (-lco) flag forwarded to the GDAL OGR driver. Example: -lco VERSION=1.0",
)
@click.option(
    "--overwrite",
    is_flag=True,
    help=(
        "Whether an existing layer by the same name can be overwritten. "
        "If not set and a layer by the same name already exists, the export will abort without overwriting it. "
        "This flag can also be set by by setting the GDAL flag -lco OVERWRITE=YES."
    ),
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
@click.option(
    "--drop-empty-geometry-features",
    is_flag=True,
    help='Skips export of those features where the geometry is either null or empty, such as "POLYGON EMPTY".',
)
@click.option(
    "--drop-geometry",
    "drop_geometry_column",
    is_flag=True,
    help="Skips export of each feature's geometry when exporting each feature.",
)
@click.argument(
    "args",
    nargs=-1,
    metavar="DATASET [EXPORT_TYPE:]DESTINATION",
    shell_complete=repo_path_completer,  # type: ignore[call-arg]
)
def table_export(
    ctx,
    ref,
    layer_name,
    output_crs,
    dataset_creation_options,
    layer_creation_options,
    overwrite,
    primary_key_as_field,
    primary_key_as_fid,
    override_geometry_type,
    drop_null_geometry_features,
    drop_empty_geometry_features,
    drop_geometry_column,
    args,
):
    """
    Basic export command - exports a tabular kart dataset at a particular commit.

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

    # Check we can find the dataset.
    try:
        dataset = repo.datasets(ref)[ds_path]
    except KeyError:
        raise click.UsageError(f"No such dataset: {ds_path}")

    if dataset.DATASET_TYPE != "table":
        raise click.UsageError(f"Dataset {ds_path} is not a vector or tabular dataset")

    # Check we can find the driver.
    driver, destination = get_driver(destination_spec)

    dataset_creation_options = list(dataset_creation_options)
    layer_creation_options = list(layer_creation_options)
    overwrite = _validate_overwrite(overwrite, layer_creation_options)
    layer_name = layer_name or ds_path

    schema = dataset.schema
    pk_name = schema.first_pk_column.name
    primary_key_as_fid = _validate_primary_key_as_fid(primary_key_as_fid, schema)

    geom_key = None
    ogr_geom_type = None
    kart_crs = None
    geometry_transform = None

    # Plan how to export geometry.
    if (
        schema.has_geometry
        and not drop_geometry_column
        and _should_export_geometry(driver, layer_creation_options)
    ):
        geom_col = schema.geometry_columns[0]
        geom_key = geom_col.name
        kart_geom_type = override_geometry_type or geom_col["geometryType"]
        ogr_geom_type = KART_GEOM_TYPE_TO_OGR_GEOM_TYPE[kart_geom_type.upper()]
        kart_crs = _get_dataset_crs(dataset)
        if output_crs is None:
            output_crs = kart_crs
        else:
            geometry_transform = _make_crs_transform(kart_crs, output_crs, ds_path)

    # Plan how to export "regular" fields (not FID or geometry).
    ogr_field_defns = {}
    for col in dataset.schema:
        if col.name == pk_name and not primary_key_as_field:
            continue
        if col.data_type == "geometry":
            continue  # Handled separately
        ogr_field_defns[col.name] = kart_schema_col_to_ogr_field_definition(col)

    # Finally, open the dataset for export.
    try:
        out_ds = open_dataset_for_export(
            driver,
            destination,
            layer_name=layer_name,
            overwrite=overwrite,
            dataset_creation_options=dataset_creation_options,
            layer_creation_options=layer_creation_options,
        )

        if ogr_geom_type is not None:
            out_layer = out_ds.CreateLayer(
                layer_name, output_crs, ogr_geom_type, options=layer_creation_options
            )
        else:
            out_layer = out_ds.CreateLayer(layer_name, options=layer_creation_options)

        ogr_field_idxs = {}
        for col_name, ogr_field_defn in ogr_field_defns.items():
            out_layer.CreateField(ogr_field_defn)
            ogr_field_idxs[col_name] = out_layer.GetLayerDefn().GetFieldCount() - 1

        for feature in dataset.features_with_crs_ids(
            repo.spatial_filter, show_progress=True
        ):
            out_feature = ogr.Feature(out_layer.GetLayerDefn())
            pk_value = feature[pk_name]
            if primary_key_as_fid:
                out_feature.SetFID(pk_value)
            if geom_key:
                geom = feature[geom_key]
                if geom is None and drop_null_geometry_features:
                    continue
                if (geom is None or geom.is_empty()) and drop_empty_geometry_features:
                    continue
                out_feature.SetGeometry(
                    _output_geometry(geom, geometry_transform, pk_value)
                )
            for col_name, ogr_field_idx in ogr_field_idxs.items():
                out_feature.SetField(ogr_field_idx, feature[col_name])
            out_layer.CreateFeature(out_feature)

        out_ds = None
    except RuntimeError as e:
        raise BaseException(f"Error running GDAL OGR driver:\n{e}") from e


def _validate_primary_key_as_fid(primary_key_as_fid, schema):
    if schema.pk_columns[0].data_type == "integer":
        return primary_key_as_fid if primary_key_as_fid is not None else True
    if primary_key_as_fid is True:
        raise click.UsageError(
            "Sorry, --primary-key-as-fid is not supported for datasets with non-integer primary keys"
        )
    return False


def _validate_overwrite(overwrite, layer_creation_options):
    overwrite_lco_set = "OVERWRITE=YES" in (
        lco.upper() for lco in layer_creation_options
    )
    if overwrite and not overwrite_lco_set:
        layer_creation_options.append("OVERWRITE=YES")
    if overwrite_lco_set:
        overwrite = True
    return overwrite


def _should_export_geometry(driver, layer_creation_options):
    if driver.GetName() == "CSV":
        # The CSV driver doesn't export geometry unless this flag is supplied -
        # so it crashes if we try and create a geometry column when this flag is not supplied.
        return any(
            lco.upper().startswith("GEOMETRY=") for lco in layer_creation_options
        )
    return True


def open_dataset_for_export(
    driver,
    destination,
    *,
    layer_name=None,
    overwrite=False,
    dataset_creation_options=[],
    layer_creation_options=[],
):
    """
    Creates or opens an existing OGR dataset ready for writing.
    If overwrite is False, makes sure that the given layer doesn't already exist at the target dataset.
    If overwrite is True and the output is a file, removes that file since not all drivers support OVERWRITE=YES.
    """
    if _is_single_layer_file_driver(driver) and Path(destination).is_file():
        if not overwrite:
            raise InvalidOperation(
                f"Driver {driver.GetName()} cannot add a new layer to an existing file, and file {destination} already exists.\n"
                "To overwrite, add the --overwrite flag."
            )
        Path(destination).unlink()
        # From OGR's point of view, we're not doing an overwrite (and not all drivers support OVERWRITE anyway).
        if "OVERWRITE=YES" in layer_creation_options:
            layer_creation_options.remove("OVERWRITE=YES")

    try:
        result = driver.Open(destination, update=True)
    except RuntimeError as e:
        if "No such file" in str(e):
            pass
        else:
            raise
    else:
        click.echo(f"Opened existing dataset {destination} for update.", err=True)
        if dataset_creation_options:
            click.echo(
                f"Dataset creation options are ignored since {destination} already exists.",
                err=True,
            )
        if layer_name is not None and not overwrite:
            try:
                layer = result.GetLayerByName(layer_name)
                if layer is not None:
                    raise InvalidOperation(
                        f"Layer {layer_name} already exists in {destination} - to overwrite, add the --overwrite flag."
                    )
            except RuntimeError:
                pass
        return result
    click.echo(f"Creating new dataset {destination}", err=True)
    return driver.CreateDataSource(destination, options=dataset_creation_options)


def _is_single_layer_file_driver(driver):
    """Returns True if a driver works on files (at least sometimes) and can only write a single layer at a time."""
    return (
        driver.GetMetadataItem(gdal.DMD_EXTENSIONS)
        and driver.GetMetadataItem(gdal.DCAP_MULTIPLE_VECTOR_LAYERS) != "YES"
    )


def _get_dataset_crs(dataset):
    crs_defs = list(dataset.crs_definitions().values())
    if not crs_defs:
        return None
    if len(crs_defs) > 1:
        raise CrsError(
            f"Sorry, multiple CRS definitions at {dataset.path!r} are not yet supported for export"
        )
    return make_crs(crs_defs[0], context=dataset.path)


def _make_crs_transform(source_crs, target_crs, ds_path):
    try:
        return osr.CoordinateTransformation(source_crs, target_crs)
    except RuntimeError as e:
        raise CrsError(f"Can't reproject dataset {ds_path!r} into target CRS: {e}")


def _output_geometry(geom, geometry_transform, pk_value):
    if geom is None:
        return None
    ogr_geom = geom.to_ogr()
    if geometry_transform is not None:
        try:
            ogr_geom.Transform(geometry_transform)
        except RuntimeError as e:
            raise CrsError(
                f"Can't reproject geometry with ID '{pk_value}' into target CRS"
            ) from e
    return ogr_geom
