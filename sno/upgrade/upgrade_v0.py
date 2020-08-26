import functools
import json
import re
from collections import deque

import pygit2

from sno import gpkg_adapter
from sno.geometry import normalise_gpkg_geom
from sno.schema import Schema
from sno.structure import DatasetStructure


def get_upgrade_sources(source_repo, source_commit):
    """Return upgrade sources for all V0 datasets at the given commit."""
    source_tree = source_commit.peel(pygit2.Tree)
    return {dataset.path: dataset for dataset in _iter_datasets(source_tree)}


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
                    yield ImportV0Dataset(o, te_path)
                else:
                    # examine inside this directory
                    to_examine.append((te_path, o))


class ImportV0Dataset(DatasetStructure):
    """
    A V0 dataset / import source.
    """

    # TODO - merge the dataset interface with the import source interface.

    def __init__(self, tree, path):
        super().__init__(tree, path)
        self.table = self.path
        self.source = "v0-sno-repo"

    META_PATH = "meta/"

    @property
    @functools.lru_cache(maxsize=1)
    def schema(self):
        return Schema.from_column_dicts(self.get_meta_item("schema.json"))

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
        # Dataset V1 items are always JSON.
        try:
            return json.loads(super().get_meta_item(name))
        except KeyError:
            if gpkg_adapter.is_gpkg_meta_item(name):
                return None  # We happen not to have this meta-item, but it is real.
            elif gpkg_adapter.is_v2_meta_item(name):
                return gpkg_adapter.generate_v2_meta_item(self, name)
            raise  # This meta-item doesn't exist at all.

    def crs_definitions(self):
        gsrs = self.get_meta_item("gpkg_spatial_ref_sys")
        if gsrs and gsrs[0]["definition"]:
            definition = gsrs[0]["definition"]
            yield gpkg_adapter.wkt_to_crs_str(definition), definition

    def iter_features(self):
        ggc = self.get_meta_item("gpkg_geometry_columns")
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

    def __str__(self):
        return self.path

    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass
