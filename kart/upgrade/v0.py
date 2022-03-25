import functools
import re

from kart.base_dataset import MetaItemType
from kart.geometry import normalise_gpkg_geom
from kart.serialise_util import json_unpack
from kart.sqlalchemy.adapter.gpkg import KartAdapter_GPKG
from kart.tabular.table_dataset import TableDataset
from kart.utils import ungenerator


class TableV0(TableDataset):
    """A V0 table-dataset / import source, only used for upgrading to V2 and beyond."""

    VERSION = 0

    FEATURE_PATH = "features/"

    @classmethod
    def is_dataset_tree(cls, tree):
        if "meta" in tree and (tree / "meta").type_str == "tree":
            meta_tree = tree / "meta"
            return "version" in meta_tree and (meta_tree / "version").type_str == "blob"
        return False

    def _iter_feature_dirs(self):
        """
        Iterates over all the features in self.tree that match the expected
        pattern for a feature, and yields the following for each:
        >>> feature_builder(path_name, path_data)
        """
        if self.FEATURE_PATH not in self.tree:
            return

        RE_DIR1 = re.compile(r"([0-9a-f]{4})?$")
        RE_DIR2 = re.compile(r"([0-9a-f-]{36})?$")

        for dir1 in self.feature_tree:
            if hasattr(dir1, "data") or not RE_DIR1.match(dir1.name):
                continue

            for dir2 in dir1:
                if hasattr(dir2, "data") or not RE_DIR2.match(dir2.name):
                    continue

                yield dir2

    def get_meta_item(self, name, missing_ok=True):
        meta_items = self.meta_items()
        return meta_items.get(name) if missing_ok else meta_items[name]

    @functools.lru_cache(maxsize=1)
    @ungenerator(dict)
    def crs_definitions(self):
        for key, value in self.meta_items().items():
            if key.startswith("crs/") and key.endswith(".wkt"):
                yield key[4:-4], value

    @functools.lru_cache(maxsize=1)
    def meta_items(self):
        return KartAdapter_GPKG.all_v2_meta_items_from_gpkg_meta_items(
            self.gpkg_meta_items()
        )

    def get_meta_item_type(self, meta_item_path):
        # For V0 / V1, all data is serialised using json.dumps
        if meta_item_path in KartAdapter_GPKG.GPKG_META_ITEM_NAMES:
            return MetaItemType.JSON
        return super().get_meta_item_type(meta_item_path)

    @functools.lru_cache(maxsize=1)
    def gpkg_meta_items(self):
        super_ = super()
        return {
            name: super_.get_meta_item(name, missing_ok=True)
            for name in KartAdapter_GPKG.GPKG_META_ITEM_NAMES
        }

    def features(self):
        geom_column = self.geom_column_name

        for feature_dir in self._iter_feature_dirs():
            source_feature_dict = {}
            for attr_blob in feature_dir:
                if not hasattr(attr_blob, "data"):
                    continue
                attr = attr_blob.name
                if attr == geom_column:
                    source_feature_dict[attr] = normalise_gpkg_geom(attr_blob.data)
                else:
                    source_feature_dict[attr] = json_unpack(attr_blob.data)
            yield source_feature_dict

    @property
    def feature_count(self):
        count = 0
        for feature_dirs in self._iter_feature_dirs():
            count += 1
        return count
