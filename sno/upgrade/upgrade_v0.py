import functools
import json
import re
from collections import deque

import pygit2

from sno import gpkg_adapter
from sno.geometry import normalise_gpkg_geom
from sno.base_dataset import BaseDataset


def get_upgrade_sources(source_repo, source_commit):
    """Return upgrade sources for all V0 datasets at the given commit."""
    source_tree = source_commit.peel(pygit2.Tree)
    return list(_iter_datasets(source_tree))


def _iter_datasets(tree):
    """ Iterate over available datasets in this repository using a specified commit"""
    to_examine = deque([("", tree)])

    while to_examine:
        path, tree = to_examine.popleft()

        for o in tree:
            # ignore everything other than directories
            if isinstance(o, pygit2.Tree):

                if path:
                    te_path = "/".join([path, o.name])
                else:
                    te_path = o.name

                if "meta" in o and "version" in o / "meta":
                    yield Dataset0(o, te_path)
                else:
                    # examine inside this directory
                    to_examine.append((te_path, o))


class Dataset0(BaseDataset):
    """
    A V0 dataset / import source.
    """

    # TODO - merge the dataset interface with the import source interface.

    META_PATH = "meta/"
    FEATURE_PATH = "feature/"

    def __init__(self, tree, path):
        super().__init__(tree, path)
        # TODO - remove self.table from import-source interface
        self.table = self.path

    def _iter_feature_dirs(self):
        """
        Iterates over all the features in self.tree that match the expected
        pattern for a feature, and yields the following for each:
        >>> feature_builder(path_name, path_data)
        """
        if "features" not in self.tree:
            return

        feature_tree = self.tree / "features"

        RE_DIR1 = re.compile(r"([0-9a-f]{4})?$")
        RE_DIR2 = re.compile(r"([0-9a-f-]{36})?$")

        for dir1 in feature_tree:
            if hasattr(dir1, "data") or not RE_DIR1.match(dir1.name):
                continue

            for dir2 in dir1:
                if hasattr(dir2, "data") or not RE_DIR2.match(dir2.name):
                    continue

                yield dir2

    @functools.lru_cache()
    def get_meta_item(self, name):
        return gpkg_adapter.generate_v2_meta_item(self, name)

    @functools.lru_cache()
    def get_gpkg_meta_item(self, name):
        rel_path = self.META_PATH + name
        data = self.get_data_at(
            rel_path, missing_ok=(name in gpkg_adapter.GPKG_META_ITEMS)
        )
        # For V0 / V1, all data is serialised using json.dumps
        return json.loads(data) if data is not None else None

    def crs_definitions(self):
        gsrs = self.get_gpkg_meta_item("gpkg_spatial_ref_sys")
        if gsrs and gsrs[0]["definition"]:
            definition = gsrs[0]["definition"]
            yield gpkg_adapter.wkt_to_crs_str(definition), definition

    def features(self):
        ggc = self.get_gpkg_meta_item("gpkg_geometry_columns")
        geom_field = ggc["column_name"] if ggc else None

        for feature_dir in self._iter_feature_dirs():
            source_feature_dict = {}
            for attr_blob in feature_dir:
                if not hasattr(attr_blob, "data"):
                    continue
                attr = attr_blob.name
                if attr == geom_field:
                    source_feature_dict[attr] = normalise_gpkg_geom(attr_blob.data)
                else:
                    source_feature_dict[attr] = json.loads(
                        attr_blob.data.decode("utf8")
                    )
            yield source_feature_dict

    @property
    def row_count(self):
        count = 0
        for feature_dirs in self._iter_feature_dirs():
            count += 1
        return count
