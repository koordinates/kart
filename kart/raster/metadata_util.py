import logging
from pathlib import Path
import re
from xml.dom import minidom
from xml.dom.minidom import Node

from kart.crs_util import normalise_wkt
from kart.geometry import ring_as_wkt
from kart.list_of_conflicts import ListOfConflicts
from kart.lfs_util import get_hash_and_size_of_file
from kart.tile.tilename_util import find_similar_files_case_insensitive, PAM_SUFFIX
from kart.schema import Schema, ColumnSchema


L = logging.getLogger("kart.raster.metadata_util")


CATEGORIES_PATTERN = re.compile(r"band/band-(.*)-categories\.json")


def rewrite_and_merge_metadata(tile_metadata_list):
    """
    Given a list of tile metadata, merges the parts we expect to be homogenous into a single piece of tile metadata in
    the same format that describes the whole list.
    """
    # TODO - this will get more complicated as we add support for convert-to-COG.
    result = {}
    all_keys = set()
    for tm in tile_metadata_list:
        all_keys.update(tm)
    # Don't copy anything from "tile" to the result - these fields are tile specific and needn't be merged.
    all_keys.remove("tile")

    for tile_metadata in tile_metadata_list:
        for key in all_keys:
            result[key] = _merge_meta_item(key, result.get(key), tile_metadata.get(key))
    return result


def _merge_meta_item(key, existing_value, new_value):
    """
    Automatically merge a meta-item with another (usually identical) meta-item.
    If is is not identical, it may still be able to be merged if it doesn't directly conflict -
    the only example of this currently is that the same category labels need not be defined in
    every PAM file, so long as no category number is ever defined to be two different labels.
    If the two meta-item is different and cannot be merged, a ListOfConflicts will be returned instead.
    """

    if existing_value is None:
        return new_value
    if new_value is None:
        return existing_value
    if isinstance(existing_value, ListOfConflicts):
        if new_value not in existing_value:
            existing_value.append(new_value)
        return existing_value
    if existing_value != new_value:
        if CATEGORIES_PATTERN.fullmatch(key):
            merged_value = _merge_category_labels(existing_value, new_value)
            if merged_value is not None:
                return merged_value
        return ListOfConflicts([existing_value, new_value])
    return existing_value


def _merge_category_labels(old_categories, new_categories):
    """
    Merge two dicts of category-labels - returns None if they cannot be merged
    (which is only the case if a particular number is defined as two different labels).
    """
    result = {}
    all_keys = set()
    all_keys.update(old_categories)
    all_keys.update(new_categories)
    for key in sorted(all_keys, key=int):
        old = old_categories.get(key)
        new = new_categories.get(key)
        if old and new and old != new:
            return None
        result[key] = old or new
    return result


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
    oid, size = get_hash_and_size_of_file(raster_tile_path)

    # Keep tile info keys in alphabetical order, except oid and size should be last.
    tile_info = {
        "name": Path(raster_tile_path).name,
        "format": "geotiff",
        "crs84Extent": format_polygon(*metadata["wgs84Extent"]["coordinates"][0]),
        "dimensions": f"{size_in_pixels[0]}x{size_in_pixels[1]}",
        "nativeExtent": format_polygon(
            cc["upperLeft"], cc["lowerLeft"], cc["lowerRight"], cc["upperRight"]
        ),
        "oid": f"sha256:{oid}",
        "size": size,
    }

    result = {
        "format.json": format_json,
        "schema.json": schema_json,
        "crs.wkt": normalise_wkt(crs_wkt),
        "tile": tile_info,
    }

    try:
        raster_tile_path = Path(raster_tile_path)
        expected_pam_path = raster_tile_path.with_name(
            raster_tile_path.name + PAM_SUFFIX
        )
        pams = find_similar_files_case_insensitive(expected_pam_path)
        if len(pams) == 1:
            pam_path = pams[0]
            result.update(extract_aux_xml_metadata(pam_path))

            if pam_path.name == expected_pam_path.name:
                tile_info.update({"pamName": pam_path.name})
            else:
                tile_info.update(
                    {"pamSourceName": pam_path.name, "pamName": expected_pam_path.name}
                )

            pam_oid, pam_size = get_hash_and_size_of_file(pam_path)
            tile_info.update(
                {
                    "pamOid": f"sha256:{pam_oid}",
                    "pamSize": pam_size,
                }
            )
    except Exception as e:
        # TODO - how to handle corrupted PAM file.
        L.warn("Error extracting aux-xml metadata", e)

    return result


def format_polygon(*points):
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

    - the column headings of any raster-attribute-table(s), as "band/band-{band_id}-rat.xml"
    - the category labels from any raster-attribute-table(s) as "band/band-{band_id}-categories.json"
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
            result[f"band/band-{band_id}-rat.xml"] = rat_schema_xml

            if category_column is not None:
                category_labels = {}
                for row in rat.getElementsByTagName("Row"):
                    row_id = int(row.getAttribute("index"))
                    category = row.getElementsByTagName("F")[category_column]
                    if not category.firstChild or not category.firstChild.nodeValue:
                        continue
                    category_text = category.firstChild.nodeValue.strip()
                    if category_text:
                        category_labels[str(row_id)] = category_text

                if category_labels:
                    result[f"band/band-{band_id}-categories.json"] = category_labels

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
