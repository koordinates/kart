import base64
import functools
import hashlib
import os
import re

import json
import msgpack
import pygit2

from . import gpkg_adapter
from .geometry import Geometry
from .structure import DatasetStructure, IntegrityError


class Dataset1(DatasetStructure):
    """
    - messagePack
    - primary key values
    - blob per feature
    - add at any location: `sno import GPKG:my.gpkg:mytable path/to/mylayer`

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

    DATASET_DIRNAME = ".sno-table"
    DATASET_PATH = ".sno-table/"
    VERSION_PATH = ".sno-table/meta/version"
    VERSION_CONTENTS = {"version": "1.0"}

    MSGPACK_EXT_GEOM = 71  # 'G'
    META_PATH = ".sno-table/meta/"

    def _msgpack_unpack_ext(self, code, data):
        if code == self.MSGPACK_EXT_GEOM:
            return Geometry.of(data)  # bytes
        else:
            self.L.warn("Unexpected msgpack extension: %d", code)
            return msgpack.ExtType(code, data)

    @property
    def version(self):
        return 1

    def gpkg_meta_items(self):
        yield from self._meta_items()

    @functools.lru_cache()
    def get_gpkg_meta_item(self, name):
        # Dataset V1 items are always JSON.
        try:
            return json.loads(super().get_meta_item(name))
        except KeyError:
            if gpkg_adapter.is_gpkg_meta_item(name):
                return None  # We happen not to have this meta-item, but it is real.
            raise  # This meta-item doesn't exist at all.

    def meta_items(self):
        return gpkg_adapter.all_v2_meta_items(self)

    @functools.lru_cache()
    def get_meta_item(self, name):
        return gpkg_adapter.generate_v2_meta_item(self, name)

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

            cid = json.loads(te.data)
            field_name = te.name
            cid_map[cid] = field_name
        return cid_map

    @property
    @functools.lru_cache(maxsize=1)
    def field_cid_map(self):
        return {v: k for k, v in self.cid_field_map.items()}

    @property
    def primary_key(self):
        return self.get_gpkg_meta_item("primary_key")

    @property
    @functools.lru_cache(maxsize=1)
    def crs_identifier(self):
        for col_dict in self.get_meta_item("schema.json"):
            if col_dict["dataType"] == "geometry":
                return col_dict["geometryCRS"]
        return None

    @property
    @functools.lru_cache(maxsize=1)
    def primary_key_type(self):
        sqlite_table_info = self.get_gpkg_meta_item("sqlite_table_info")
        field = next(f for f in sqlite_table_info if f["name"] == self.primary_key)
        return field["type"]

    def cast_primary_key(self, pk_value):
        pk_type = self.primary_key_type

        if pk_value is not None:
            # https://www.sqlite.org/datatype3.html
            # 3.1. Determination Of Column Affinity
            if "INT" in pk_type:
                pk_value = int(pk_value)
            elif re.search("TEXT|CHAR|CLOB", pk_type):
                pk_value = str(pk_value)

        return pk_value

    def encode_1pk_to_path(self, pk, cast_primary_key=True, relative=False):
        if cast_primary_key:
            pk = self.cast_primary_key(pk)

        pk_enc_bin = msgpack.packb(pk, use_bin_type=True)  # encode pk value via msgpack
        pk_enc = base64.urlsafe_b64encode(pk_enc_bin).decode("utf8")  # filename safe
        pk_hash = hashlib.sha1(
            pk_enc.encode("utf8")
        ).hexdigest()  # hash to randomly spread filenames
        rel_path = "/".join([".sno-table", pk_hash[:2], pk_hash[2:4], pk_enc])
        return rel_path if relative else self.full_path(rel_path)

    @classmethod
    def decode_path_to_1pk(cls, path):
        encoded = os.path.basename(path)
        return msgpack.unpackb(base64.urlsafe_b64decode(encoded), raw=False)

    def remove_feature(self, pk, index):
        feature_path = self.encode_1pk_to_path(pk)
        index.remove(feature_path)

    def repo_feature_to_dict(self, blob_path, blob_memoryview):
        feature = {
            self.primary_key: self.decode_path_to_1pk(blob_path),
        }
        bin_feature = msgpack.unpackb(
            blob_memoryview, ext_hook=self._msgpack_unpack_ext, raw=False,
        )
        for colid, value in sorted(bin_feature.items()):
            field_name = self.cid_field_map[colid]
            feature[field_name] = value

        return feature

    def _get_feature(self, pk_value, *, path=None):
        # The caller must supply at least one of (pk_values, path) so we know which
        # feature is meant. We can infer whichever one is missing from the one supplied.
        if pk_value is None and path is None:
            raise ValueError("Either <pk_values> or <path> must be supplied")

        if path is not None:
            rel_path = self.ensure_rel_path(path)
        else:
            pk_value = self.cast_primary_key(pk_value)
            rel_path = self.encode_1pk_to_path(pk_value, relative=True)

        leaf = self.tree / rel_path
        if not isinstance(leaf, pygit2.Blob):
            raise IntegrityError(
                f"Unexpected TreeEntry type={leaf.type_str} at {rel_path}"
            )
        return leaf

    def get_feature(self, pk_value, *, path=None):
        blob = self._get_feature(pk_value=pk_value, path=path)
        return self.repo_feature_to_dict(blob.name, memoryview(blob))

    def get_feature_tuples(self, pk_values, col_names, *, ignore_missing=False):
        tupleizer = self.build_feature_tupleizer(col_names)
        for pk in pk_values:
            try:
                blob = self._get_feature(pk)
            except KeyError:
                if ignore_missing:
                    continue
                else:
                    raise

            yield tupleizer(blob)

    def build_feature_tupleizer(self, tuple_cols):
        field_cid_map = self.field_cid_map

        ftuple_order = []
        for field_name in tuple_cols:
            if field_name == self.primary_key:
                ftuple_order.append(-1)
            else:
                ftuple_order.append(field_cid_map[field_name])
        ftuple_order = tuple(ftuple_order)

        def tupleizer(blob):
            bin_feature = msgpack.unpackb(
                blob.data, ext_hook=self._msgpack_unpack_ext, raw=False, use_list=False,
            )
            return tuple(
                [
                    self.decode_path_to_1pk(blob.name) if c == -1 else bin_feature[c]
                    for c in ftuple_order
                ]
            )

        return tupleizer

    def _iter_feature_blobs(self, fast=False):
        """
        Iterates over all the features in self.tree that match the expected
        pattern for a feature, and yields the following for each:
        >>> feature_builder(path_name, path_data)
        """
        sno_table_tree = self.tree / self.DATASET_DIRNAME

        # .sno-table/
        #   [hex(pk-hash):2]/
        #     [hex(pk-hash):2]/
        #       [base64(pk-value)]=[msgpack({attribute-id: attribute-value, ...})]
        URLSAFE_B64 = r"A-Za-z0-9_\-"

        RE_DIR = re.compile(r"([0-9a-f]{2})?$")
        RE_LEAF = re.compile(
            fr"(?:[{URLSAFE_B64}]{{4}})*(?:[{URLSAFE_B64}]{{2}}==|[{URLSAFE_B64}]{{3}}=)?$"
        )

        for dir1 in sno_table_tree:
            if hasattr(dir1, "data") or not RE_DIR.match(dir1.name):
                continue

            for dir2 in dir1:
                if hasattr(dir2, "data") or not RE_DIR.match(dir2.name):
                    continue

                for leaf in dir2:
                    if not fast:
                        if not RE_LEAF.match(leaf.name):
                            continue
                        elif not hasattr(leaf, "data"):
                            path = f".sno-table/{dir1.name}/{dir2.name}/{leaf.name}"
                            self.L.warn(
                                f"features: No data found at path {path}, type={type(leaf)}"
                            )
                            continue

                    yield leaf

    def features(self, **kwargs):
        """ Feature iterator yielding (encoded_pk, feature-dict) pairs """
        return (
            (blob.name, self.repo_feature_to_dict(blob.name, memoryview(blob)),)
            for blob in self._iter_feature_blobs(fast=False)
        )

    def feature_tuples(self, col_names, **kwargs):
        """ Optimised feature iterator yielding tuples, ordered by the columns from col_names """
        tupleizer = self.build_feature_tupleizer(col_names)
        return (tupleizer(blob) for blob in self._iter_feature_blobs(fast=True))

    def feature_count(self, fast=True):
        return sum(1 for blob in self._iter_feature_blobs(fast=fast))

    def encode_feature(
        self,
        feature,
        field_cid_map=None,
        geom_cols=None,
        primary_key=None,
        cast_primary_key=True,
    ):
        """
        Given a feature, returns the path and the data that *should be written*
        to write this feature.
        """
        if primary_key is None:
            primary_key = self.primary_key
        return (
            self.encode_1pk_to_path(feature[primary_key], cast_primary_key),
            self.encode_feature_blob(feature, field_cid_map, geom_cols, primary_key),
        )

    def encode_feature_blob(
        self, feature, field_cid_map=None, geom_cols=None, primary_key=None
    ):
        """
        Given a feature, returns the data that *should be written* to write this feature
        (but not the path it should be written to).
        """
        if field_cid_map is None:
            field_cid_map = self.field_cid_map
        if geom_cols is None:
            geom_cols = [self.geom_column_name]
        if primary_key is None:
            primary_key = self.primary_key

        bin_feature = {}
        for field in sorted(feature.keys(), key=lambda f: field_cid_map[f]):
            if field == primary_key:
                continue

            field_id = field_cid_map[field]
            value = feature[field]
            if field in geom_cols:
                if value is not None:
                    value = msgpack.ExtType(self.MSGPACK_EXT_GEOM, value)

            bin_feature[field_id] = value

        return msgpack.packb(bin_feature, use_bin_type=True)

    def _import_meta_items(self, source):
        """
        Iterates through V1 specific meta items to import:
            path/to/layer/.sno-table/
              meta/
                version
                primary_key
                fields/
                  myfield
        and includes source.gpkg_meta_items()
        """
        yield ("version", self.VERSION_CONTENTS)

        for i, column in enumerate(source.schema):
            yield (f"fields/{column.name}", i)

        pk_field = source.primary_key
        yield ("primary_key", pk_field)

        yield from gpkg_adapter.all_gpkg_meta_items(source, self.table_name)

    def import_iter_meta_blobs(self, repo, source):
        """For the given import source, yield the meta blobs that should to be written."""
        for name, value in self._import_meta_items(source):
            yield (
                f"{self.path}/{self.META_PATH}{name}",
                json.dumps(value).encode("utf8"),
            )

    def import_iter_feature_blobs(self, resultset, source, replacing_dataset=None):
        """For the given import source, yields the feature blobs that should be written."""
        pk_field = source.primary_key

        field_cid_map = self.get_field_cid_map(source)

        for row in resultset:
            feature_path = self.encode_1pk_to_path(
                row[pk_field], cast_primary_key=False
            )

            bin_feature = {}
            for field in row.keys():
                if field == pk_field:
                    continue

                field_id = field_cid_map[field]
                value = row[field]
                if field in source.geom_cols:
                    if value is not None:
                        value = msgpack.ExtType(self.MSGPACK_EXT_GEOM, value)

                bin_feature[field_id] = value

            yield (feature_path, msgpack.packb(bin_feature, use_bin_type=True))

    def get_field_cid_map(self, source):
        return {column.name: i for i, column in enumerate(source.schema)}
