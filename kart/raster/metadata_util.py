from enum import IntFlag
import logging
from pathlib import Path
import re
from xml.dom import minidom
from xml.dom.minidom import Node

from botocore.exceptions import ClientError

from kart.crs_util import make_crs, normalise_wkt
from kart.geometry import ring_as_wkt
from kart.list_of_conflicts import ListOfConflicts
from kart.lfs_util import get_oid_and_size_of_file, prefix_sha256
from kart.raster.validate_cloud_optimized_geotiff import validate as validate_cogtiff
from kart.s3_util import fetch_from_s3, get_error_code
from kart.schema import Schema, ColumnSchema
from kart.tile.tilename_util import find_similar_files_case_insensitive, PAM_SUFFIX

L = logging.getLogger("kart.raster.metadata_util")


CATEGORIES_PATTERN = re.compile(r"band/([0-9]+)/categories\.json")


class RewriteMetadata(IntFlag):
    """Different ways to interpret metadata depending on the type of import."""

    NO_REWRITE = 0x0

    # We're about to convert this file to COG - update the metadata to be as if we'd already done this.
    AS_IF_CONVERTED_TO_COG = 0x1

    # Drop all the profile info from the format info - we don't need to verify it or store it.
    # (ie, because we don't care about the tiles profile, or, we're about to change the tile's profile anyway.)
    DROP_PROFILE = 0x2


def rewrite_and_merge_metadata(
    tile_metadata_list, rewrite_metadata=RewriteMetadata.NO_REWRITE, override_crs=None
):
    """
    Given a list of tile metadata, merges the parts we expect to be homogenous into a single piece of tile metadata in
    the same format that describes the whole list.

    If override_crs is provided, it will be used to override the CRS of all tiles, setting the dataset CRS.
    """
    result = {}
    all_keys = set()
    for tm in tile_metadata_list:
        all_keys.update(tm)
    # Don't copy anything from "tile" to the result - these fields are tile specific and needn't be merged.
    all_keys.discard("tile")

    # Normalize override CRS if provided
    normalized_override_crs = None
    if override_crs:
        crs_obj = make_crs(override_crs)
        normalized_override_crs = normalise_wkt(crs_obj.ExportToWkt())

    for tile_metadata in tile_metadata_list:
        for key in all_keys:
            # Handle CRS with potential override
            if key == "crs.wkt" and override_crs:
                # Override CRS for all tiles if specified
                tile_value = normalized_override_crs
            else:
                tile_value = tile_metadata.get(key)

            result[key] = _merge_meta_item(
                key, result.get(key), tile_value, rewrite_metadata
            )
    return result


def _merge_meta_item(
    key, existing_value, new_value, rewrite_metadata=RewriteMetadata.NO_REWRITE
):
    """
    Automatically merge a meta-item with another (usually identical) meta-item.
    If is is not identical, it may still be able to be merged if it doesn't directly conflict -
    the only example of this currently is that the same category labels need not be defined in
    every PAM file, so long as no category number is ever defined to be two different labels.
    If the two meta-item is different and cannot be merged, a ListOfConflicts will be returned instead.
    """
    if key == "format.json":
        new_value = _rewrite_format(new_value, rewrite_metadata)

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


def _rewrite_format(format_json, rewrite_metadata):
    if RewriteMetadata.DROP_PROFILE in rewrite_metadata:
        return {k: v for k, v in format_json.items() if k != "profile"}
    elif RewriteMetadata.AS_IF_CONVERTED_TO_COG in rewrite_metadata:
        return {**format_json, "profile": "cloud-optimized"}
    return format_json


def extract_raster_tile_metadata(
    raster_tile_path,
    oid_and_size=None,
    pam_path=None,
    search_for_pam=True,
    override_crs=None,
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
        "crs":    - CRS as stored at meta/crs.wkt (or overridden by override_crs)
    }

    Although any two raster tiles can differ in any way imaginable, we specifically constrain tiles in the
    same dataset to be homogenous enough that the meta items format.json, schema.json and crs.wkt
    describe *all* of the tiles in that dataset. The "tile" field is where we keep all information
    that can be different for every tile in the dataset, which is why it must be stored in pointer files.

    pc_tile_path - a pathlib.Path or a string containing the path to a file or an S3 url.
    oid_and_size - a tuple (sha256_oid, filesize) if already known, to avoid repeated work.
    pam_path - a pathlib.Path to a local copy of the PAM file, if one is available, to avoid re-fetching.
    search_for_pam - whether to search for the PAM in the expected location relative to the raster_tile_path.
        If pam_path is set to a truthy value, then the pam_path takes precedence and no search will be performed.
    """
    from osgeo import gdal

    raster_tile_path = str(raster_tile_path)

    gdal_path_spec = raster_tile_path
    if gdal_path_spec.startswith("s3://"):
        gdal_path_spec = gdal_path_spec.replace("s3://", "/vsis3/")
    metadata = gdal.Info(gdal_path_spec, options=["-json", "-norat", "-noct"])

    full_check = not gdal_path_spec.startswith("/vsi")
    warnings, errors, details = validate_cogtiff(gdal_path_spec, full_check=full_check)
    is_cog = not errors

    format_json = {
        "fileType": "geotiff",
    }
    if is_cog:
        format_json["profile"] = "cloud-optimized"

    schema_json = gdalinfo_bands_to_kart_schema(metadata["bands"])
    crs_wkt = metadata["coordinateSystem"]["wkt"]

    cc = metadata["cornerCoordinates"]
    size_in_pixels = metadata["size"]
    if oid_and_size:
        oid, size = oid_and_size
    else:
        oid, size = get_oid_and_size_of_file(raster_tile_path)

    name = Path(raster_tile_path).name
    # Keep tile info keys in alphabetical order, except oid and size should be last.
    tile_info = {
        "name": name,
        "format": "geotiff/cog" if is_cog else "geotiff",
        "crs84Extent": format_polygon(*metadata["wgs84Extent"]["coordinates"][0]),
        "dimensions": f"{size_in_pixels[0]}x{size_in_pixels[1]}",
        "nativeExtent": format_polygon(
            cc["upperLeft"], cc["lowerLeft"], cc["lowerRight"], cc["upperRight"]
        ),
        "oid": prefix_sha256(oid),
        "size": size,
    }

    result = {
        "format.json": format_json,
        "schema.json": schema_json,
        "crs.wkt": normalise_wkt(override_crs or crs_wkt),
        "tile": tile_info,
    }

    _find_and_add_pam_info(
        result,
        raster_tile_path=raster_tile_path,
        pam_path=pam_path,
        search_for_pam=search_for_pam,
    )
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


def _find_and_add_pam_info(
    raster_tile_metadata, *, raster_tile_path=None, pam_path=None, search_for_pam=True
):
    if not pam_path and not search_for_pam:
        return

    raster_tile_path = str(raster_tile_path)
    tile_info = raster_tile_metadata["tile"]

    if not pam_path and raster_tile_path.startswith("s3://"):
        try:
            pam_url = raster_tile_path + PAM_SUFFIX
            pam_path = fetch_from_s3(pam_url)
            raster_tile_metadata.update(extract_aux_xml_metadata(pam_path))
            pam_oid, pam_size = get_oid_and_size_of_file(pam_path)
            tile_info.update(
                {
                    "pamOid": prefix_sha256(pam_oid),
                    "pamSize": pam_size,
                }
            )
            pam_path.unlink()
            return
        except ClientError as e:
            if get_error_code(e) != 404:
                L.warning("Error extracting aux-xml metadata: %s", e)
            return

    raster_tile_path = Path(raster_tile_path)
    expected_pam_path = raster_tile_path.with_name(raster_tile_path.name + PAM_SUFFIX)
    if not pam_path:
        pams = find_similar_files_case_insensitive(expected_pam_path)
        if len(pams) == 1:
            pam_path = pams[0]

    if not pam_path:
        return

    try:
        raster_tile_metadata.update(extract_aux_xml_metadata(pam_path))

        if pam_path.name == expected_pam_path.name:
            tile_info.update({"pamName": pam_path.name})
        else:
            tile_info.update(
                {"pamSourceName": pam_path.name, "pamName": expected_pam_path.name}
            )

        pam_oid, pam_size = get_oid_and_size_of_file(pam_path)
        tile_info.update(
            {
                "pamOid": prefix_sha256(pam_oid),
                "pamSize": pam_size,
            }
        )
    except Exception as e:
        # TODO - how to handle corrupted PAM file.
        L.warning("Error extracting aux-xml metadata: %s", e)


def extract_aux_xml_metadata(aux_xml_path):
    """
    Given the path to a tif.aux.xml file, tries to extract the following:

    - the column headings of any raster-attribute-table(s), as "band/{band_id}/rat.xml"
    - the category labels from any raster-attribute-table(s) as "band/{band_id}/categories.json"
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
            result[f"band/{band_id}/rat.xml"] = rat_schema_xml

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
                    result[f"band/{band_id}/categories.json"] = category_labels

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


def is_cog(tile_format):
    tile_format = extract_format(tile_format)
    if isinstance(tile_format, dict):
        return tile_format.get("profile") == "cloud-optimized"
    elif isinstance(tile_format, str):
        return "cog" in tile_format
    raise ValueError("Bad tile format")


def extract_format(tile_format):
    if isinstance(tile_format, dict):
        if "format.json" in tile_format:
            return tile_format["format.json"]
        if "format" in tile_format:
            return tile_format["format"]
    return tile_format


def get_format_summary(format_info):
    """
    Given format info as stored in format.json, return a short string summary such as: geotiff/cog
    """
    if "format.json" in format_info:
        format_info = format_info["format.json"]

    format_summary = format_info["fileType"]
    if format_info.get("profile") == "cloud-optimized":
        format_summary += "/cog"
    return format_summary
