from kart.geometry import normalise_gpkg_geom

import base64
import functools
import os
import re

import msgpack
import pygit2

from kart.geometry import Geometry
from kart.base_dataset import BaseDataset
from kart.serialise_util import json_unpack
from kart.sqlalchemy.adapter.gpkg import KartAdapter_GPKG
from kart.utils import ungenerator


class Dataset1(BaseDataset):
    """
    - messagePack
    - primary key values
    - blob per feature
    - add at any location: `kart import GPKG:my.gpkg:mytable path/to/mylayer`

    any/structure/mylayer/
      .sno-table/
        meta/
          version      = {"version": "1.0"}
          primary_key
          fields/
            [field]  # map to attribute-id
            ...
        [hex(pk-hash):2]/
          [hex(pk-hash):2]/
            [base64(pk-value)]=[msgpack({attribute-id: attribute-value, ...})]
    """

    VERSION = 1

    DATASET_DIRNAME = ".sno-table"

    META_PATH = "meta/"
    VERSION_PATH = "meta/version"
    VERSION_CONTENTS = {"version": "1.0"}

    MSGPACK_EXT_GEOM = 71  # 'G'

    def _msgpack_unpack_ext(self, code, data):
        if code == self.MSGPACK_EXT_GEOM:
            return Geometry.of(data)  # bytes
        else:
            self.L.warn("Unexpected msgpack extension: %d", code)
            return msgpack.ExtType(code, data)

    @functools.lru_cache()
    def get_meta_item(self, name):
        return self.meta_items().get(name)

    @functools.lru_cache()
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

    @functools.lru_cache(maxsize=1)
    def gpkg_meta_items(self):
        # For V0 / V1, all data is serialised using json.dumps
        return {
            name: self.get_json_data_at(self.META_PATH + name, missing_ok=True)
            for name in KartAdapter_GPKG.GPKG_META_ITEM_NAMES
        }

    @property
    @functools.lru_cache(maxsize=1)
    def feature_tree(self):
        return self.tree

    @property
    @functools.lru_cache(maxsize=1)
    def cid_field_map(self):
        cid_map = {}
        for te in self.meta_tree / "fields":
            if not isinstance(te, pygit2.Blob):
                self.L.warn(
                    "cid_field_map: Unexpected TreeEntry type=%s @ meta/fields/%s",
                    te.type_str,
                    te.name,
                )
                continue

            cid = json_unpack(te.data)
            field_name = te.name
            cid_map[cid] = field_name
        return cid_map

    @property
    @functools.lru_cache(maxsize=1)
    def field_cid_map(self):
        return {v: k for k, v in self.cid_field_map.items()}

    def get_field_cid_map(self, source):
        return {column.name: i for i, column in enumerate(source.schema)}

    @classmethod
    def decode_path_to_1pk(cls, path):
        encoded = os.path.basename(path)
        return msgpack.unpackb(base64.urlsafe_b64decode(encoded), raw=False)

    def get_feature(self, path, data):
        feature = {
            self.primary_key: self.decode_path_to_1pk(path),
        }
        bin_feature = msgpack.unpackb(
            data,
            ext_hook=self._msgpack_unpack_ext,
            raw=False,
        )
        for colid, value in sorted(bin_feature.items()):
            field_name = self.cid_field_map[colid]
            feature[field_name] = value

        return feature

    def feature_blobs(self):
        """
        Yields all the blobs in self.tree that match the expected pattern for a feature.
        """

        feature_tree = self.inner_tree

        # .sno-table/
        #   [hex(pk-hash):2]/
        #     [hex(pk-hash):2]/
        #       [base64(pk-value)]=[msgpack({attribute-id: attribute-value, ...})]
        URLSAFE_B64 = r"A-Za-z0-9_\-"

        RE_DIR = re.compile(r"([0-9a-f]{2})?$")
        RE_LEAF = re.compile(
            fr"(?:[{URLSAFE_B64}]{{4}})*(?:[{URLSAFE_B64}]{{2}}==|[{URLSAFE_B64}]{{3}}=)?$"
        )

        for dir1 in feature_tree:
            if hasattr(dir1, "data") or not RE_DIR.match(dir1.name):
                continue

            for dir2 in dir1:
                if hasattr(dir2, "data") or not RE_DIR.match(dir2.name):
                    continue

                for leaf in dir2:
                    if not RE_LEAF.match(leaf.name):
                        continue
                    elif not hasattr(leaf, "data"):
                        path = f".sno-table/{dir1.name}/{dir2.name}/{leaf.name}"
                        self.L.warn(
                            f"features: No data found at path {path}, type={type(leaf)}"
                        )
                        continue

                    yield leaf

    def features(self):
        # Geometries weren't normalised in V1, but they are in V2.
        # Normalise them here.
        geom_column = self.geom_column_name
        for feature in super().features():
            if geom_column:
                # add bboxes to geometries.
                feature[geom_column] = normalise_gpkg_geom(feature[geom_column])
            yield feature
