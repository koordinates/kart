import base64
import collections
import functools
import hashlib
import itertools
import os
import re

import json
import msgpack
import pygit2

from . import diff, gpkg, gpkg_adapter
from .filter_util import UNFILTERED
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

    VERSION_PATH = ".sno-table/meta/version"
    VERSION_CONTENTS = {"version": "1.0"}

    MSGPACK_EXT_GEOM = 71  # 'G'
    META_PATH = ".sno-table/meta"

    ALL_META_ITEMS = gpkg_adapter.GPKG_META_ITEMS

    def _msgpack_unpack_ext(self, code, data):
        if code == self.MSGPACK_EXT_GEOM:
            return data  # bytes
        else:
            self.L.warn("Unexpected msgpack extension: %d", code)
            return msgpack.ExtType(code, data)

    def _msgpack_unpack_ext_ogr(self, code, data):
        if code == self.MSGPACK_EXT_GEOM:
            return gpkg.gpkg_geom_to_ogr(data)
        else:
            self.L.warn("Unexpected msgpack extension: %d", code)
            return msgpack.ExtType(code, data)

    @property
    def version(self):
        return 1

    def iter_meta_items(self, include_hidden=False):
        exclude = () if include_hidden else ("fields", "version")
        yield from self._iter_meta_items(exclude=exclude)

    def iter_gpkg_meta_items(self):
        exclude = () if include_hidden else ("fields", "version")
        yield from self._iter_meta_items(exclude=exclude)

    @functools.lru_cache()
    def get_meta_item(self, name):
        # Dataset V1 items are always JSON.
        try:
            return json.loads(super().get_meta_item(name))
        except KeyError:
            if name in gpkg_adapter.GPKG_META_ITEMS:
                return None  # We happen not to have this meta-item, but it is real.
            raise  # This meta-item doesn't exist at all in dataset V1.

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
        return self.get_meta_item("primary_key")

    @property
    @functools.lru_cache(maxsize=1)
    def primary_key_type(self):
        schema = self.get_meta_item("sqlite_table_info")
        field = next(f for f in schema if f["name"] == self.primary_key)
        return field["type"]

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

    def decode_path(self, path):
        """
        Given a path in this layer of the sno repository - eg ".sno-table/49/3e/Bg==" -
        returns a tuple in either of the following forms:
        1. ("feature", primary_key)
        2. ("meta", metadata_file_path)
        """
        if path.startswith(".sno-table/"):
            path = path[len(".sno-table/") :]
        if path.startswith("meta/"):
            return ("meta", path[len("meta/") :])
        pk = self.decode_path_to_1pk(os.path.basename(path))
        return ("feature", pk)

    def remove_feature(self, pk, index):
        feature_path = self.encode_1pk_to_path(pk)
        index.remove(feature_path)

    def repo_feature_to_dict(self, blob_path, blob_data, ogr_geoms=False):
        feature = {
            self.primary_key: self.decode_path_to_1pk(blob_path),
        }
        bin_feature = msgpack.unpackb(
            blob_data,
            ext_hook=self._msgpack_unpack_ext_ogr
            if ogr_geoms
            else self._msgpack_unpack_ext,
            raw=False,
        )
        for colid, value in bin_feature.items():
            field_name = self.cid_field_map[colid]
            feature[field_name] = value

        return feature

    def _get_feature(self, pk_value):
        pk_value = self.cast_primary_key(pk_value)
        rel_path = self.encode_1pk_to_path(pk_value, relative=True)

        leaf = self.tree / rel_path
        if not isinstance(leaf, pygit2.Blob):
            raise IntegrityError(
                f"Unexpected TreeEntry type={leaf.type_str} at {rel_path}"
            )
        return leaf

    def get_feature(self, pk_value, *, ogr_geoms=True):
        blob = self._get_feature(pk_value)
        return self.repo_feature_to_dict(blob.name, blob.data, ogr_geoms=ogr_geoms)

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

    def build_feature_tupleizer(self, tuple_cols, ogr_geoms=False):
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
                blob.data,
                ext_hook=self._msgpack_unpack_ext_ogr
                if ogr_geoms
                else self._msgpack_unpack_ext,
                raw=False,
                use_list=False,
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
        sno_table_tree = self.tree / ".sno-table"

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

    def features(self, *, ogr_geoms=False, **kwargs):
        """ Feature iterator yielding (encoded_pk, feature-dict) pairs """
        return (
            (
                blob.name,
                self.repo_feature_to_dict(blob.name, blob.data, ogr_geoms=ogr_geoms),
            )
            for blob in self._iter_feature_blobs(fast=False)
        )

    def feature_tuples(self, col_names, **kwargs):
        """ Optimised feature iterator yielding tuples, ordered by the columns from col_names """
        tupleizer = self.build_feature_tupleizer(col_names)
        return (tupleizer(blob) for blob in self._iter_feature_blobs(fast=True))

    def feature_count(self, fast=True):
        return sum(1 for blob in self._iter_feature_blobs(fast=True))

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
        and includes source.iter_gpkg_meta_items()
        """
        yield ("version", self.VERSION_CONTENTS)

        for colname, colid in source.field_cid_map.items():
            yield (f"fields/{colname}", colid)

        pk_field = source.primary_key
        yield ("primary_key", pk_field)

        for name, value in source.iter_gpkg_meta_items():
            viter = value if isinstance(value, (list, tuple)) else [value]

            for v in viter:
                if v and "table_name" in v:
                    v["table_name"] = self.name

            yield (name, value)

    def import_iter_meta_blobs(self, repo, source):
        """For the given import source, yield the meta blobs that should to be written."""
        for name, value in self._import_meta_items(source):
            yield (
                f"{self.path}/{self.META_PATH}/{name}",
                json.dumps(value).encode("utf8"),
            )

    def import_iter_feature_blobs(self, resultset, source):
        """For the given import source, yields the feature blobs that should be written."""
        pk_field = source.primary_key

        for row in resultset:
            feature_path = self.encode_1pk_to_path(
                row[pk_field], cast_primary_key=False
            )

            bin_feature = {}
            for field in row.keys():
                if field == pk_field:
                    continue

                field_id = source.field_cid_map[field]
                value = row[field]
                if field in source.geom_cols:
                    if value is not None:
                        value = msgpack.ExtType(self.MSGPACK_EXT_GEOM, value)

                bin_feature[field_id] = value

            yield (feature_path, msgpack.packb(bin_feature, use_bin_type=True))
