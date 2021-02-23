# Meta item names - part of the Datasets V2 standard.
# See DATASETS_v2.md

META_ITEM_NAMES = (
    "title",
    "description",
    "schema.json",
    "metadata.xml",
    "metadata/dataset.json",  # deprecated - imported as metadata.xml going forward.
)

# These meta items aren't stored at <dataset>/.sno-dataset/meta/<meta-item-name>
# Instead they are stored at <dataset>/<meta-item-name>.
# This will eventually become a user-visible area, where users can check in changes directly.
ATTACHMENT_META_ITEMS = ("metadata.xml",)

# In addition to these meta items, CRS definitions are also meta items.
# These have names in the following format:
# >>> crs/<crs_identifier>.wkt
# For example:
# >>> crs/EPSG:2193.wkt

# But, note the CRS identifier could be anything, if the CRS is not in the EPSG registry.
