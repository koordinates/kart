# Meta item names - part of the Datasets V3 standard.
# See DATASETS_v3.md

META_ITEM_NAMES = (
    "title",  # Text - the dataset's name / title.
    "description",  # Text - a longer description about the dataset's contents.
    "schema.json",  # JSON representation of the dataset's schema. See kart/schema.py, DATASETS_v3.md
    "metadata.xml",  # Any XML metadata about the dataset.
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
