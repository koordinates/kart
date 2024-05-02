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
    for i in range(ogr.GetDriverCount()):
        d = ogr.GetDriver(i)
        if d.GetName().upper() == shortname:
            if not is_writable_vector_format(d):
                raise click.UsageError(
                    f"Driver {shortname} does not support writing vectors"
                )
            return d
    raise click.UsageError(f"Destination format {shortname} not recognized")


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
    help="Include the primary key column as a regular field that is sent to the GDAL driver.",
)
@click.option(
    "--primary-key-as-fid/--no-primary-key-as-fid",
    is_flag=True,
    default=False,
    help="Report to the GDAL driver that the FID of each feature is the primary key value.",
)
@click.option(
    "--override-geometry",
    help="Override the geometry type to something more specific than in the Kart dataset",
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
    override_geometry,
    args,
    **kwargs,
):
    """
    Experimental export command - exports a tabular kart dataset at a particular commit.

    Uses GDAL's OGR drivers to do so - consult https://gdal.org/drivers/vector/index.html
    to find the options specific to a particular driver, which can then be set using
    -dsco and -lco just as when using `ogr2ogr`.
    """
    if len(args) < 2:
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

    if override_geometry and override_geometry.upper() not in OGR_GEOMTYPE_MAP:
        click.echo("Geometry type should be one of:", err=True)
        click.echo("\n".join(OGR_GEOMTYPE_MAP.keys()), err=True)
        raise click.UsageError(f"Unknown geometry type: {override_geometry}")

    schema = dataset.schema
    pk_name = schema.first_pk_column.name

    geom_key = None
    ogr_geom_type = None
    ogr_crs = None

    if schema.has_geometry:
        geom_col = schema.geometry_columns[0]
        geom_key = geom_col.name
        kart_geom_type = override_geometry or geom_col["geometryType"]
        ogr_geom_type = OGR_GEOMTYPE_MAP[kart_geom_type.upper()]

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
        out_layer.CreateField(kart_col_to_ogr_field(col))

    for feature in dataset.features_with_crs_ids(
        repo.spatial_filter, show_progress=True
    ):
        out_feature = ogr.Feature(out_layer.GetLayerDefn())
        if primary_key_as_fid:
            out_feature.SetFID(feature[pk_name])
        if geom_key:
            geom = feature[geom_key]
            out_feature.SetGeometry(geom.to_ogr() if geom else None)
        for i, key in enumerate(regular_keys):
            out_feature.SetField(i, feature[key])
        out_layer.CreateFeature(out_feature)
        out_feature = None

    out_ds = None


def kart_col_to_ogr_field(col):
    ogr_type = None
    size = col.get("size")
    if size is not None:
        ogr_type = OGR_DATATYPE_MAP.get((col.data_type, size))
    if ogr_type is None:
        ogr_type = OGR_DATATYPE_MAP[col.data_type]
    assert ogr_type is not None

    if isinstance(ogr_type, tuple):
        ogr_type, ogr_subtype = ogr_type
    else:
        ogr_subtype = None

    result = ogr.FieldDefn(col.name, ogr_type)
    if ogr_subtype is not None:
        result.SetSubType(ogr_subtype)

    if col.data_type in ("text", "blob"):
        length = col.get("length")
        if length:
            result.SetWidth(length)

    if col.data_type == "numeric":
        precision = col.get("precision")
        scale = col.get("scale")
        # Rather confusingly, OGR's concepts of 'width' and 'precision'
        # correspond to 'precision' and 'scale' in most other systems, respectively:
        if precision:
            col.SetWidth(precision)
        if scale:
            col.SetPrecision(scale)

    return result


OGR_DATATYPE_MAP = {
    "boolean": (ogr.OFTInteger, ogr.OFSTBoolean),
    "blob": ogr.OFTBinary,
    "date": ogr.OFTDate,
    "float": ogr.OFTReal,
    ("float", 32): (ogr.OFTReal, ogr.OFSTFloat32),
    ("float", 64): ogr.OFTReal,
    "integer": ogr.OFTInteger64,
    ("integer", 8): (ogr.OFTInteger, ogr.OFSTInt16),
    ("integer", 16): (ogr.OFTInteger, ogr.OFSTInt16),
    ("integer", 32): ogr.OFTInteger,
    ("integer", 64): ogr.OFTInteger64,
    "interval": ogr.OFTInteger64,
    "numeric": ogr.OFTString,
    "text": ogr.OFTString,
    "time": ogr.OFTTime,
    "timestamp": ogr.OFTDateTime,
}


def _build_ogr_geomtype_map():
    type_names = [
        "Point",
        "LineString",
        "Polygon",
        "MultiPoint",
        "MultiLineString",
        "MultiPolygon",
        "GeometryCollection",
    ]
    result = {}
    for type_name in type_names:
        kart_type = type_name.upper()
        base_val = getattr(ogr, f"wkb{type_name}")
        result[kart_type] = base_val
        result[f"{kart_type} Z"] = getattr(ogr, f"wkb{type_name}Z", base_val + 1000)
        result[f"{kart_type} M"] = getattr(ogr, f"wkb{type_name}M", base_val + 2000)
        result[f"{kart_type} ZM"] = getattr(ogr, f"wkb{type_name}ZM", base_val + 3000)

    result["GEOMETRY"] = ogr.wkbUnknown
    return result


OGR_GEOMTYPE_MAP = _build_ogr_geomtype_map()
