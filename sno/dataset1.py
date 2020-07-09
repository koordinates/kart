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

from . import gpkg, diff
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
          primary_key
          fields/
            [field]  # map to attribute-id
            ...
        [hex(pk-hash):2]/
          [hex(pk-hash):2]/
            [base64(pk-value)]=[msgpack({attribute-id: attribute-value, ...})]
    """

    VERSION_PATH = ".sno-table/meta/version"
    VERSION_SPECIFIER = "1."
    VERSION_IMPORT = "1.0"

    MSGPACK_EXT_GEOM = 71  # 'G'
    META_PATH = ".sno-table/meta"

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

    def import_meta_items(self, source):
        """
            path/to/layer/.sno-table/
              meta/
                version
                schema
                geometry
                primary_key
                fields/
                  myfield
        """
        for name, item in super().import_meta_items(source):
            yield (name, item)

        for colname, colid in source.field_cid_map.items():
            yield (f"fields/{colname}", colid)

        pk_field = source.primary_key
        yield ("primary_key", pk_field)

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

    def import_iter_feature_blobs(self, resultset, source):
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

    def diff(self, other, pk_filter=UNFILTERED, reverse=False):
        candidates_ins = collections.defaultdict(list)
        candidates_upd = {}
        candidates_del = collections.defaultdict(list)

        params = {}
        if reverse:
            params = {"swap": True}

        if other is None:
            diff_index = self.tree.diff_to_tree(**params)
            self.L.debug(
                "diff (%s -> None / %s): %s changes",
                self.tree.id,
                "R" if reverse else "F",
                len(diff_index),
            )
        else:
            diff_index = self.tree.diff_to_tree(other.tree, **params)
            self.L.debug(
                "diff (%s -> %s / %s): %s changes",
                self.tree.id,
                other.tree.id,
                "R" if reverse else "F",
                len(diff_index),
            )

        if reverse:
            this, other = other, self
        else:
            this, other = self, other

        for d in diff_index.deltas:
            self.L.debug(
                "diff(): %s %s %s", d.status_char(), d.old_file.path, d.new_file.path
            )

            if d.old_file and d.old_file.path.startswith(".sno-table/meta/"):
                continue
            elif d.new_file and d.new_file.path.startswith(".sno-table/meta/"):
                continue

            if d.status == pygit2.GIT_DELTA_DELETED:
                my_pk = this.decode_path_to_1pk(os.path.basename(d.old_file.path))
                if not str(my_pk) in pk_filter:
                    continue

                self.L.debug("diff(): D %s (%s)", d.old_file.path, my_pk)

                my_obj = this.get_feature(my_pk, ogr_geoms=False)

                candidates_del[str(my_pk)].append((str(my_pk), my_obj))
            elif d.status == pygit2.GIT_DELTA_MODIFIED:
                my_pk = this.decode_path_to_1pk(os.path.basename(d.old_file.path))
                other_pk = other.decode_path_to_1pk(os.path.basename(d.new_file.path))
                if not str(my_pk) in pk_filter and not str(other_pk) in pk_filter:
                    continue

                self.L.debug(
                    "diff(): M %s (%s) -> %s (%s)",
                    d.old_file.path,
                    my_pk,
                    d.new_file.path,
                    other_pk,
                )

                my_obj = this.get_feature(my_pk, ogr_geoms=False)
                other_obj = other.get_feature(other_pk, ogr_geoms=False)

                candidates_upd[str(my_pk)] = (my_obj, other_obj)
            elif d.status == pygit2.GIT_DELTA_ADDED:
                other_pk = other.decode_path_to_1pk(os.path.basename(d.new_file.path))
                if not str(other_pk) in pk_filter:
                    continue

                self.L.debug("diff(): A %s (%s)", d.new_file.path, other_pk)

                other_obj = other.get_feature(other_pk, ogr_geoms=False)

                candidates_ins[str(other_pk)].append(other_obj)
            else:
                # GIT_DELTA_RENAMED
                # GIT_DELTA_COPIED
                # GIT_DELTA_IGNORED
                # GIT_DELTA_TYPECHANGE
                # GIT_DELTA_UNMODIFIED
                # GIT_DELTA_UNREADABLE
                # GIT_DELTA_UNTRACKED
                raise NotImplementedError(f"Delta status: {d.status_char()}")

        # detect renames
        for h in list(candidates_del.keys()):
            if h in candidates_ins:
                track_pk, my_obj = candidates_del[h].pop(0)
                other_obj = candidates_ins[h].pop(0)

                candidates_upd[track_pk] = (my_obj, other_obj)

                if not candidates_del[h]:
                    del candidates_del[h]
                if not candidates_ins[h]:
                    del candidates_ins[h]

        return diff.Diff(
            self,
            meta={},
            inserts=list(itertools.chain(*candidates_ins.values())),
            deletes=dict(itertools.chain(*candidates_del.values())),
            updates=candidates_upd,
        )
