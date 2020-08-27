# Meta item names - part of the Datasets V2 standard.
# See DATASETS_v2.md

META_ITEM_NAMES = (
    "title",
    "description",
    "schema.json",
    "metadata/dataset.json",
)

# In addition to these meta items, CRS definitions are also meta items.
# These have names in the following format:
# >>> crs/<crs_identifier>.wkt
# For example:
# >>> crs/EPSG:2193.wkt

# But, note the CRS identifier could be anything, if the CRS is not in the EPSG registry.
