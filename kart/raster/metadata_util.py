import logging
from pathlib import Path
from xml.dom import minidom
from xml.dom.minidom import Node

from kart.crs_util import normalise_wkt
from kart.geometry import ring_as_wkt
from kart.list_of_conflicts import ListOfConflicts
from kart.schema import Schema, ColumnSchema


L = logging.getLogger("kart.raster.metadata_util")


def rewrite_and_merge_metadata(tile_metadata_list):
    """
    Given a list of tile metadata, merges the parts we expect to be homogenous into a single piece of tile metadata in
    the same format that describes the whole list.
    """
    # TODO - this will get more complicated as we add support for convert-to-COG.
    result = {}
    all_keys = set().union(*tile_metadata_list)
    # Don't copy anything from "tile" to the result - these fields are tile specific and needn't be merged.
    all_keys.remove("tile")
    # TODO - handle metadata that doesn't actually conflict but may differ slightly
    # (eg, slightly different subsets of category labels for the different tiles).
    for tile_metadata in tile_metadata_list:
        for key in all_keys:
            _merge_metadata_field(result, key, tile_metadata[key])
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


def extract_raster_tile_metadata(raster_tile_path):
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

    metadata = gdal.Info(str(raster_tile_path), options=["-json", "-norat", "-noct"])

    # NOTE: this format is still in early stages of design, is subject to change.

    format_json = {"fileType": "image/tiff; application=geotiff"}
    schema_json = gdalinfo_bands_to_kart_schema(metadata["bands"])
    crs_wkt = metadata["coordinateSystem"]["wkt"]

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
        "format.json": format_json,
        "schema.json": schema_json,
        "crs.wkt": normalise_wkt(crs_wkt),
        "tile": tile_info,
    }

    try:
        raster_tile_path = Path(raster_tile_path)
        aux_xml_path = raster_tile_path.with_name(raster_tile_path.name + ".aux.xml")
        if aux_xml_path.is_file():
            result.update(extract_aux_xml_metadata(aux_xml_path))
    except Exception as e:
        L.warn("Error extracting aux-xml metadata", e)

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

    if gdalinfo_band.get("colorInterpretation"):
        result["interpretation"] = gdalinfo_band["colorInterpretation"].lower()

    if gdalinfo_band.get("description"):
        result["description"] = gdalinfo_band["description"]

    if gdalinfo_band.get("noDataValue") is not None:
        result["noData"] = gdalinfo_band["noDataValue"]
        if result["dataType"] == "integer" and isinstance(result["noData"], float):
            result["noData"] = int(result["noData"])

    return ColumnSchema(result)


def extract_aux_xml_metadata(aux_xml_path):
    """
    Given the path to a tif.aux.xml file, tries to extract the following:

    - the column headings of any raster-attribute-table(s), as "bands/band-{band_id}-rat.xml"
    - the category labels from any raster-attribute-table(s) as "bands/band-{band_id}-categories.json"
    """
    result = {}

    with minidom.parse(str(aux_xml_path)) as parsed:
        bands = parsed.getElementsByTagName("PAMRasterBand")
        for band in bands:
            category_column = None
            band_id = band.getAttribute("band")
            rat = get_element_by_tag_name(band, "GDALRasterAttributeTable")
            if not rat:
                continue

            rat_schema = rat.cloneNode(deep=False)
            for child in rat.childNodes:
                if getattr(child, "tagName", None) != "FieldDefn":
                    continue
                field_defn = child
                rat_schema.appendChild(remove_xml_whitespace(field_defn))
                usage = get_element_by_tag_name(field_defn, "Usage")
                if usage:
                    usage_text = usage.firstChild.nodeValue.strip()
                    if usage_text == "2":
                        category_column = int(field_defn.getAttribute("index"))

            rat_schema_xml = rat_schema.toprettyxml(indent="    ")
            rat_schema_xml = "\n".join(
                l for l in rat_schema_xml.split("\n") if not l.isspace()
            )
            result[f"bands/band-{band_id}-rat.xml"] = rat_schema_xml

            if category_column is not None:
                category_labels = {}
                for row in rat.getElementsByTagName("Row"):
                    row_id = int(row.getAttribute("index"))
                    category = row.getElementsByTagName("F")[category_column]
                    if not category.hasChildNodes():
                        continue
                    category_text = category.firstChild.nodeValue.strip()
                    if category_text:
                        category_labels[row_id] = category_text

                if category_labels:
                    result[f"bands/band-{band_id}-categories.json"] = category_labels

    return result


def remove_xml_whitespace(node):
    """Removes whitespace from xml - toprettyxml() doesn't really work unless you call this first."""
    for child in node.childNodes:
        if child.nodeType == Node.TEXT_NODE:
            if child.nodeValue:
                child.nodeValue = child.nodeValue.strip()
        elif child.nodeType == Node.ELEMENT_NODE:
            remove_xml_whitespace(child)
    return node


def get_element_by_tag_name(element, tag_name):
    """Returns the first result from getElementsByTagName, if any."""
    result = element.getElementsByTagName(tag_name)
    return result[0] if result else None
