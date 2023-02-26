from kart.crs_util import normalise_wkt
from kart.geometry import ring_as_wkt
from kart.list_of_conflicts import ListOfConflicts
from kart.schema import Schema, ColumnSchema


def rewrite_and_merge_metadata(tile_metadata_list):
    """
    Given a list of tile metadata, merges the parts we expect to be homogenous into a single piece of tile metadata in
    the same format that describes the whole list.
    """
    # TODO - this will get more complicated as we add support for convert-to-COG.
    result = {}
    for tile_metadata in tile_metadata_list:
        _merge_metadata_field(result, "format", tile_metadata["format"])
        _merge_metadata_field(result, "schema", tile_metadata["schema"])
        _merge_metadata_field(result, "crs", tile_metadata["crs"])
        # Don't copy anything from "tile" to the result - these fields are tile specific and needn't be merged.
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


def extract_raster_tile_metadata(
    raster_tile_path,
    *,
    extract_schema=True,
):
    """
    Use gdalinfo to get any and all raster metadata we can make use of in Kart.
    This includes metadata that must be dataset-homogenous and would be stored in the dataset's /meta/ folder,
    along with other metadata that is tile-specific and would be stored in the tile's pointer file.

    Output:
    {
        "format": - Information about file format, as stored at meta/format.json (or some subset thereof).
        "tile":   - Tile-specific (non-homogenous) information, as stored in individual tile pointer files.
        "schema": - PDRF schema, as stored in meta/schema.json
        "crs":    - CRS as stored at meta/crs.wkt
    }

    Although any two raster tiles can differ in any way imaginable, we specifically constrain tiles in the
    same dataset to be homogenous enough that the meta items format.json, schema.json and crs.wkt
    describe *all* of the tiles in that dataset. The "tile" field is where we keep all information
    that can be different for every tile in the dataset, which is why it must be stored in pointer files.
    """
    from osgeo import gdal

    metadata = gdal.Info(raster_tile_path, options="-json")

    # NOTE: this format is still in early stages of design, is subject to change.

    crs = metadata["coordinateSystem"]["wkt"]
    format_info = {"fileType": "image/tiff; application=geotiff"}

    cc = metadata["cornerCoordinates"]
    size_in_pixels = metadata["size"]
    tile_info = {
        "format": "geotiff",
        "crs84Extent": format_polygon(*metadata["wgs84Extent"]["coordinates"][0]),
        "nativeExtent": format_polygon(
            cc["upperLeft"], cc["lowerLeft"], cc["lowerRight"], cc["upperRight"]
        ),
        "dimensions": f"{size_in_pixels[0]}x{size_in_pixels[1]}",
    }

    result = {
        "format": format_info,
        "tile": tile_info,
        "crs": normalise_wkt(crs),
    }
    if extract_schema:
        result["schema"] = gdalinfo_bands_to_kart_schema(metadata["bands"])

    return result


def format_polygon(*points):
    # TODO - should we just store the axis-aligned extent?
    return "POLYGON(" + ring_as_wkt(*points) + ")"


def gdalinfo_bands_to_kart_schema(gdalinfo_bands):
    return Schema([gdalinfo_band_to_kart_columnschema(b) for b in gdalinfo_bands])


GDAL_TYPE_TO_KART_TYPE = {
    "Byte": {"dataType": "integer", "size": 8, "unsigned": True},
    "Int8": {"dataType": "integer", "size": 8},
    "Int16": {"dataType": "integer", "size": 16},
    "Int32": {"dataType": "integer", "size": 32},
    "Int64": {"dataType": "integer", "size": 64},
    "Float32": {"dataType": "integer", "size": 32},
    "Float64": {"dataType": "integer", "size": 64},
}


def gdalinfo_band_to_kart_columnschema(gdalinfo_band):
    # TODO - handle color tables and category tables.
    result = {}

    gdal_type = gdalinfo_band["type"]
    if gdal_type.startswith("UInt"):
        gdal_type = gdal_type[1:]
        result["unsigned"] = True
    elif gdal_type.startswith("CInt") or gdal_type.startswith("CFloat"):
        gdal_type = gdal_type[1:]
        result["complex"] = True

    kart_type_info = GDAL_TYPE_TO_KART_TYPE.get(gdal_type)
    if kart_type_info is None:
        raise RuntimeError(f"Unrecognized GDAL type: {gdal_type}")

    result.update(kart_type_info)

    if "colorInterpretation" in gdalinfo_band:
        result["interpretation"] = gdalinfo_band["colorInterpretation"].lower()

    return ColumnSchema(result)
