import base64
import collections
import functools
import hashlib
import itertools
import json
import logging
import os
import re
import subprocess
import time
from collections import deque
from pathlib import Path

import click
import msgpack
import pygit2

from . import core, gpkg, diff
from .exceptions import NotFound, NO_COMMIT

L = logging.getLogger("sno.structure")


class RepositoryStructure:
    def lookup(repo, key):
        L.debug(f"key={key}")
        try:
            obj = repo.revparse_single(key)
        except KeyError:
            raise NotFound(f"{key} is not a commit or tree", exit_code=NO_COMMIT)

        try:
            return RepositoryStructure(repo, commit=obj.peel(pygit2.Commit))
        except pygit2.InvalidSpecError:
            pass

        try:
            return RepositoryStructure(repo, tree=obj.peel(pygit2.Tree))
        except pygit2.InvalidSpecError:
            pass

        raise NotFound(
            f"{key} is a {obj.type_str}, not a commit or tree", exit_code=NO_COMMIT
        )

    def __init__(self, repo, commit=None, tree=None):
        self.L = logging.getLogger(__class__.__qualname__)
        self.repo = repo

        # If _commit is not None, self.tree -> self._commit.tree, so _tree is not set.
        if commit is not None:
            self._commit = commit
        elif tree is not None:
            self._commit = None
            self._tree = tree
        elif self.repo.is_empty:
            self._commit = None
            self._tree = None
        else:
            self._commit = self.repo.head.peel(pygit2.Commit)

    def __getitem__(self, path):
        """ Get a specific dataset by path """
        return self.get_at(path, self.tree)

    def __eq__(self, other):
        return other and (self.repo.path == other.repo.path) and (self.id == other.id)

    def __repr__(self):
        if self._commit is not None:
            return f"RepoStructure<{self.repo.path}@{self._commit.id}>"
        else:
            return f"RepoStructure<{self.repo.path} <empty>>"

    def get(self, path):
        try:
            return self.get_at(path, self.tree)
        except KeyError:
            return None

    def get_at(self, path, tree):
        """ Get a specific dataset by path using a specified Tree """
        try:
            o = tree[path]
        except KeyError:
            raise

        if isinstance(o, pygit2.Tree):
            ds = DatasetStructure.instantiate(o, path)
            return ds

        raise KeyError(f"No valid dataset found at '{path}'")

    def __iter__(self):
        """ Iterate over available datasets in this repository """
        return self.iter_at(self.tree)

    def iter_at(self, tree):
        """ Iterate over available datasets in this repository using a specified Tree """
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

                    try:
                        ds = DatasetStructure.instantiate(o, te_path)
                        yield ds
                    except IntegrityError:
                        self.L.warn(
                            "Error loading dataset from %s, ignoring tree",
                            te_path,
                            exc_info=True,
                        )
                    except ValueError:
                        # examine inside this directory
                        to_examine.append((te_path, o))

    def get_for_index_entry(self, index_entry):
        dataset_path = index_entry.path.split(r"/.sno-table/", maxsplit=1)[0]
        return self.get(dataset_path)

    @property
    def id(self):
        obj = self._commit or self._tree
        return obj.id if obj else None

    @property
    def short_id(self):
        obj = self._commit or self._tree
        return obj.short_id if obj else None

    @property
    def head_commit(self):
        return self._commit

    @property
    def tree(self):
        if self._commit is not None:
            return self._commit.peel(pygit2.Tree)
        return self._tree

    @property
    def working_copy(self):
        from .working_copy import WorkingCopy

        if getattr(self, "_working_copy", None) is None:
            self._working_copy = WorkingCopy.open(self.repo)

        return self._working_copy

    @working_copy.deleter
    def working_copy(self):
        wc = self.working_copy
        if wc:
            wc.delete()
        del self._working_copy

    def commit(self, wcdiff, message, *, allow_empty=False):
        tree = self.tree
        wc = self.working_copy

        git_index = pygit2.Index()
        git_index.read_tree(tree)

        with wc.session():
            for ds in self:
                ds.write_index(
                    wcdiff[ds], git_index, self.repo, callback=wc.commit_callback
                )

            L.info("Writing tree...")
            new_tree = git_index.write_tree(self.repo)
            L.info(f"Tree sha: {new_tree}")

            wc.commit_callback(None, "TREE", tree=new_tree)

            L.info("Committing...")
            user = self.repo.default_signature
            # this will also update the ref (branch) to point to the current commit
            new_commit = self.repo.create_commit(
                "HEAD",  # reference_name
                user,  # author
                user,  # committer
                message,  # message
                new_tree,  # tree
                [self.repo.head.target],  # parents
            )
            L.info(f"Commit: {new_commit}")

        # TODO: update reflog
        return new_commit


class IntegrityError(ValueError):
    pass


class DatasetStructure:
    DEFAULT_IMPORT_VERSION = "1.0"
    META_PATH = "meta"

    def __init__(self, tree, path):
        if self.__class__ is DatasetStructure:
            raise TypeError("Use DatasetStructure.instantiate()")

        self.tree = tree
        self.path = path.strip("/")
        self.name = self.path.replace("/", "__")
        self.L = logging.getLogger(self.__class__.__qualname__)

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.path}>"

    @classmethod
    @functools.lru_cache(maxsize=1)
    def all_versions(cls):
        """ Get all supported Dataset Structure versions """

        def _get_subclasses(klass):
            for c in klass.__subclasses__():
                yield c
                yield from _get_subclasses(c)

        return tuple(_get_subclasses(cls))

    @classmethod
    @functools.lru_cache(maxsize=1)
    def version_numbers(cls):
        versions = [klass.VERSION_IMPORT for klass in cls.all_versions()]
        # default first, then newest to oldest
        versions.sort(
            key=lambda v: (
                v == DatasetStructure.DEFAULT_IMPORT_VERSION,
                [int(i) for i in v.split(".")],
            ),
            reverse=True,
        )
        return tuple(versions)

    @classmethod
    def for_version(cls, version=None):
        if version is None:
            version = cls.DEFAULT_IMPORT_VERSION

        for klass in cls.all_versions():
            if klass.VERSION_IMPORT == version:
                return klass

        raise ValueError(f"No DatasetStructure for v{version}")

    @classmethod
    def importer(cls, path, version=None):
        return cls.for_version(version)(tree=None, path=path)

    @classmethod
    def instantiate(cls, tree, path):
        """ Load a DatasetStructure from a Tree """
        L = logging.getLogger(cls.__qualname__)

        if not isinstance(tree, pygit2.Tree):
            raise TypeError(f"Expected Tree object, got {type(tree)}")

        for version_klass in cls.all_versions():
            try:
                blob = tree[version_klass.VERSION_PATH]
            except KeyError:
                continue
            else:
                L.debug(
                    "Found candidate %sx tree at: %s",
                    version_klass.VERSION_SPECIFIER,
                    path,
                )

            if not isinstance(blob, pygit2.Blob):
                raise IntegrityError(
                    f"{version_klass.__name__}: {path}/{version_klass.VERSION_PATH} isn't a blob ({blob.type_str})"
                )

            try:
                d = json.loads(blob.data)
            except Exception as e:
                raise IntegrityError(
                    f"{version_klass.__name__}: Couldn't load version file from: {path}/{version_klass.VERSION_PATH}"
                ) from e

            version = d.get("version", None)
            if version and version.startswith(version_klass.VERSION_SPECIFIER):
                L.debug("Found %s dataset at: %s", version, path)
                return version_klass(tree, path)
            else:
                continue

        raise ValueError(
            f"{path}: Couldn't find any Dataset Structure version that matched"
        )

    # useful methods

    @property
    @functools.lru_cache(maxsize=1)
    def version(self):
        return self.get_meta_item("version")["version"]

    @property
    @functools.lru_cache(maxsize=1)
    def meta_tree(self):
        return self.tree / self.META_PATH

    @functools.lru_cache()
    def get_meta_item(self, name):
        meta_tree = self.meta_tree
        try:
            o = meta_tree / name
        except KeyError:
            return None

        if not isinstance(o, pygit2.Blob):
            raise ValueError(f"meta/{name} is a {o.type_str}, expected blob")

        return json.loads(o.data)

    def iter_meta_items(self, exclude=None):
        exclude = set(exclude or [])
        for top_tree, top_path, subtree_names, blob_names in core.walk_tree(
            self.meta_tree
        ):
            for name in blob_names:
                if name in exclude:
                    continue

                meta_path = "/".join([top_path, name]) if top_path else name
                yield (meta_path, self.get_meta_item(meta_path))

            subtree_names[:] = [n for n in subtree_names if n not in exclude]

    @property
    @functools.lru_cache(maxsize=1)
    def has_geometry(self):
        return self.geom_column_name is not None

    @property
    @functools.lru_cache(maxsize=1)
    def geom_column_name(self):
        meta_geom = self.get_meta_item("gpkg_geometry_columns")
        return meta_geom["column_name"] if meta_geom else None

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

    def get_feature(self, pk_value):
        raise NotImplementedError()

    def feature_tuples(self, col_names, **kwargs):
        """ Feature iterator yielding tuples, ordered by the columns from col_names """

        # not optimised in V0
        for k, f in self.features():
            yield tuple(f[c] for c in col_names)

    def import_meta(self, repo, index, source):
        """
            layer-name/
              meta/
                version
                sqlite_table_info
                gpkg_contents
                gpkg_geometry_columns
                gpkg_spatial_ref_sys
                [gpkg_metadata]
                [gpkg_metadata_reference]
        """
        for blob_path, blob_data in self.import_iter_meta_blobs(repo, source):
            blob_id = repo.create_blob(blob_data)
            entry = pygit2.IndexEntry(blob_path, blob_id, pygit2.GIT_FILEMODE_BLOB)
            index.add(entry)

    def import_meta_items(self, source):
        for name, value in source.build_meta_info(repo_version=self.VERSION_IMPORT):
            viter = value if isinstance(value, (list, tuple)) else [value]

            for v in viter:
                if v and "table_name" in v:
                    v["table_name"] = self.name

            yield (name, value)

    def import_iter_meta_blobs(self, repo, source):
        for name, value in self.import_meta_items(source):
            yield (
                f"{self.path}/{self.META_PATH}/{name}",
                json.dumps(value).encode("utf8"),
            )

    def import_table(self, repo, source):
        table = source.table
        if not table:
            raise ValueError("No table specified")

        path = self.path

        if repo.is_empty:
            head_tree = None
        else:
            head_tree = repo.head.peel(pygit2.Tree)
            if path in head_tree:
                raise ValueError(f"{path}/ already exists")

        click.echo(f"Importing {source} to {path} ...")

        with source:
            index = pygit2.Index()
            if head_tree:
                index.read_tree(head_tree)

            click.echo("Writing meta bits...")
            self.import_meta(repo, index, source)

            row_count = source.row_count
            click.echo(f"Found {row_count:,d} features in {table}")

            import_kwargs = {
                "field_cid_map": source.field_cid_map,
                "primary_key": source.primary_key,
                "geom_cols": source.geom_cols,
                "path": path,
            }

            # iterate features
            t0 = time.monotonic()
            t1 = None
            count = 0
            for source_feature in source.iter_features():
                if not t1:
                    t1 = time.monotonic()
                    click.echo(f"Query ran in {t1-t0:.1f}s")

                self.write_feature(source_feature, repo, index, **import_kwargs)
                count += 1

                if count and count % 500 == 0:
                    click.echo(f"  {count:,d} features... @{time.monotonic()-t1:.1f}s")

            t2 = time.monotonic()

            click.echo(f"Added {count:,d} Features to index in {t2 - (t1 or t0):.1f}s")
            click.echo(f"Overall rate: {(count/(t2-t0 or 1E-3)):.0f} features/s)")

            click.echo("Writing tree...")
            tree_id = index.write_tree(repo)
            del index
            t3 = time.monotonic()
            click.echo(f"Tree sha: {tree_id} (in {(t3-t2):.0f}s)")

            click.echo("Committing...")
            user = repo.default_signature
            commit = repo.create_commit(
                "refs/heads/master",
                user,
                user,
                f"Import from {Path(source.source).name} to /{path}/",
                tree_id,
                [] if repo.is_empty else [repo.head.target],
            )
            t4 = time.monotonic()
            click.echo(f"Commit: {commit} (in {(t4-t3):.0f}s)")

            click.echo(f"Garbage-collecting...")
            subprocess.check_call(["git", "-C", repo.path, "gc"])
            t5 = time.monotonic()
            click.echo(f"GC completed in {(t5-t4):.1f}s")

    def fast_import_table(
        self, repo, source, iter_func=1, max_pack_size="2G", limit=None
    ):

        table = source.table
        if not table:
            raise ValueError("No table specified")

        path = self.path

        if not repo.is_empty:
            if path in repo.head.peel(pygit2.Tree):
                raise ValueError(f"{path}/ already exists")

        with source:
            if limit:
                num_rows = min(limit, source.row_count)
                click.echo(
                    f"Importing {num_rows:,d} of {source.row_count:,d} features from {source} to {path}/ ..."
                )
            else:
                num_rows = source.row_count
                click.echo(
                    f"Importing {num_rows:,d} features from {source} to {path}/ ..."
                )

            t0 = time.monotonic()
            if iter_func == 2:
                src_iterator = source.iter_features_sorted(
                    self.get_feature_path, limit=limit
                )
            else:
                src_iterator = source.iter_features()

            t1 = time.monotonic()
            click.echo(f"Source setup in {t1-t0:.1f}s")

            click.echo("Starting git-fast-import...")
            p = subprocess.Popen(
                [
                    "git",
                    "fast-import",
                    "--date-format=now",
                    "--done",
                    "--stats",
                    f"--max-pack-size={max_pack_size}",
                ],
                cwd=repo.path,
                stdin=subprocess.PIPE,
                bufsize=1,  # line
            )

            user = repo.default_signature

            header = (
                "commit refs/heads/master\n"
                f"committer {user.name} <{user.email}> now\n"
                f"data <<EOM\nImport from {Path(source.source).name} to {path}/\nEOM\n"
            )
            p.stdin.write(header.encode("utf8"))

            if not repo.is_empty:
                # start with the existing tree/contents
                p.stdin.write(b"from refs/heads/master^0\n")

            for blob_path, blob_data in self.import_iter_meta_blobs(repo, source):
                p.stdin.write(
                    f"M 644 inline {blob_path}\ndata {len(blob_data)}\n".encode("utf8")
                )
                p.stdin.write(blob_data)
                p.stdin.write(b"\n")

            # features
            t2 = time.monotonic()
            for i, (blob_path, blob_data) in enumerate(
                self.import_iter_feature_blobs(src_iterator, source)
            ):
                p.stdin.write(
                    f"M 644 inline {blob_path}\ndata {len(blob_data)}\n".encode("utf8")
                )
                p.stdin.write(blob_data)
                p.stdin.write(b"\n")

                if i and i % 100000 == 0:
                    click.echo(f"  {i:,d} features... @{time.monotonic()-t2:.1f}s")

                if limit is not None and i == (limit - 1):
                    click.secho(f"  Stopping at {limit:,d} features", fg="yellow")
                    break

            p.stdin.write(b"\ndone\n")
            t3 = time.monotonic()
            click.echo(f"Added {num_rows:,d} Features to index in {t3-t2:.1f}s")
            click.echo(f"Overall rate: {(num_rows/(t3-t2 or 1E-3)):.0f} features/s)")

            p.stdin.close()
            p.wait()
            if p.returncode != 0:
                raise subprocess.CalledProcessError(
                    f"Error! {p.returncode}", "git-fast-import"
                )
            t4 = time.monotonic()
            click.echo(f"Closed in {(t4-t3):.0f}s")

    RTREE_INDEX_EXTENSIONS = ("sno-idxd", "sno-idxi")

    def build_spatial_index(self, path):
        """
        Internal proof-of-concept method for building a spatial index across the repository.

        Uses Rtree (libspatialindex underneath): http://toblerity.org/rtree/index.html
        """
        import rtree

        if not self.has_geometry:
            raise ValueError("No geometry to index")

        def _indexer():
            t0 = time.monotonic()

            c = 0
            for (pk, geom) in self.feature_tuples(
                [self.primary_key, self.geom_column_name]
            ):
                c += 1
                if geom is None:
                    continue

                e = gpkg.geom_envelope(geom)
                yield (pk, e, None)

                if c % 50000 == 0:
                    print(f"  {c} features... @{time.monotonic()-t0:.1f}s")

        p = rtree.index.Property()
        p.dat_extension = self.RTREE_INDEX_EXTENSIONS[0]
        p.idx_extension = self.RTREE_INDEX_EXTENSIONS[1]
        p.leaf_capacity = 1000
        p.fill_factor = 0.9
        p.overwrite = True
        p.dimensionality = 2

        t0 = time.monotonic()
        idx = rtree.index.Index(path, _indexer(), properties=p, interleaved=False)
        t1 = time.monotonic()
        b = idx.bounds
        c = idx.count(b)
        del idx
        t2 = time.monotonic()
        print(f"Indexed {c} features ({b}) in {t1-t0:.1f}s; flushed in {t2-t1:.1f}s")

    def get_spatial_index(self, path):
        """
        Retrieve a spatial index built with build_spatial_index().

        Query with .nearest(coords), .intersection(coords), .count(coords)
        http://toblerity.org/rtree/index.html
        """
        import rtree

        p = rtree.index.Property()
        p.dat_extension = self.RTREE_INDEX_EXTENSIONS[0]
        p.idx_extension = self.RTREE_INDEX_EXTENSIONS[1]

        idx = rtree.index.Index(path, properties=p)
        return idx


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
            return gpkg.geom_to_ogr(data)
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

    def index_entry_to_pk(self, index_entry):
        feature_path = index_entry.path.split("/.sno-table/", maxsplit=1)[1]
        if feature_path.startswith("meta/"):
            return 'META'
        return self.decode_pk(os.path.basename(feature_path))

    def encode_pk(self, pk):
        pk_enc = msgpack.packb(pk, use_bin_type=True)  # encode pk value via msgpack
        pk_str = base64.urlsafe_b64encode(pk_enc).decode("utf8")  # filename safe
        return pk_str

    def decode_pk(self, encoded):
        return msgpack.unpackb(base64.urlsafe_b64decode(encoded), raw=False)

    def get_feature_path(self, pk):
        pk = self.cast_primary_key(pk)
        pk_enc = self.encode_pk(pk)
        pk_hash = hashlib.sha1(
            pk_enc.encode("utf8")
        ).hexdigest()  # hash to randomly spread filenames
        return "/".join([".sno-table", pk_hash[:2], pk_hash[2:4], pk_enc])

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
        object_path = self.get_feature_path(pk)
        index.remove("/".join([self.path, object_path]))

    def repo_feature_to_dict(self, pk, blob, ogr_geoms=False):
        feature = {
            self.primary_key: self.decode_pk(pk),
        }
        bin_feature = msgpack.unpackb(
            blob.data,
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

        pk_enc = self.encode_pk(pk_value)
        pk_hash = hashlib.sha1(
            pk_enc.encode("utf8")
        ).hexdigest()  # hash to randomly spread filenames

        te = self.tree / ".sno-table" / pk_hash[:2] / pk_hash[2:4] / pk_enc
        if not isinstance(te, pygit2.Blob):
            raise IntegrityError(
                f"Unexpected TreeEntry type={te.type_str} in feature tree {pk_enc}"
            )

        return pk_enc, te

    def get_feature(self, pk_value, *, ogr_geoms=True):
        pk_enc, blob = self._get_feature(pk_value)
        return pk_enc, self.repo_feature_to_dict(pk_enc, blob, ogr_geoms=ogr_geoms)

    def get_feature_tuples(self, pk_values, col_names, *, ignore_missing=False):
        tupleizer = self.build_feature_tupleizer(col_names)
        for pk in pk_values:
            try:
                pk_enc, blob = self._get_feature(pk)
            except KeyError:
                if ignore_missing:
                    continue
                else:
                    raise

            yield tupleizer(pk_enc, blob)

    def build_feature_tupleizer(self, tuple_cols, ogr_geoms=False):
        field_cid_map = self.field_cid_map

        ftuple_order = []
        for field_name in tuple_cols:
            if field_name == self.primary_key:
                ftuple_order.append(-1)
            else:
                ftuple_order.append(field_cid_map[field_name])
        ftuple_order = tuple(ftuple_order)

        def tupleizer(pk, blob):
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
                    self.decode_pk(pk) if c == -1 else bin_feature[c]
                    for c in ftuple_order
                ]
            )

        return tupleizer

    def _features(self, feature_builder, fast):
        top_tree = self.tree / ".sno-table"

        # .sno-table/
        #   [hex(pk-hash):2]/
        #     [hex(pk-hash):2]/
        #       [base64(pk-value)]=[msgpack({attribute-id: attribute-value, ...})]
        URLSAFE_B64 = r"A-Za-z0-9_\-"

        RE_L = re.compile(r"([0-9a-f]{2})?$")
        RE_F = re.compile(
            fr"(?:[{URLSAFE_B64}]{{4}})*(?:[{URLSAFE_B64}]{{2}}==|[{URLSAFE_B64}]{{3}}=)?$"
        )

        for l1e in top_tree:
            if l1e.type != pygit2.GIT_OBJ_TREE or not RE_L.match(l1e.name):
                continue

            for l2e in l1e:
                if l2e.type != pygit2.GIT_OBJ_TREE or not RE_L.match(l2e.name):
                    continue

                for fbe in l2e:
                    if not fast:
                        if not RE_F.match(fbe.name):
                            continue
                        elif fbe.type != pygit2.GIT_OBJ_BLOB:
                            self.L.warn(
                                "features: Unexpected TreeEntry type=%s in feature tree '%s/%s/%s'",
                                fbe.type_str,
                                l1e.name,
                                l2e.name,
                                fbe.name,
                            )
                            continue

                    yield feature_builder(fbe.name, fbe)

    def features(self, *, ogr_geoms=False, **kwargs):
        """ Feature iterator yielding (feature-key, feature-dict) pairs """
        return self._features(
            lambda pk, blob: (
                pk,
                self.repo_feature_to_dict(pk, blob, ogr_geoms=ogr_geoms),
            ),
            fast=False,
        )

    def feature_tuples(self, col_names, **kwargs):
        """ Optimised feature iterator yielding tuples, ordered by the columns from col_names """
        tupleizer = self.build_feature_tupleizer(col_names)
        return self._features(tupleizer, fast=True)

    def feature_count(self, fast=True):
        return sum(self._features(lambda pk, blob: 1, fast=fast))

    def encode_feature(
        self, feature, field_cid_map=None, geom_cols=None, primary_key=None
    ):
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

    def write_feature(
        self,
        row,
        repo,
        index,
        *,
        field_cid_map=None,
        geom_cols=None,
        primary_key=None,
        **kwargs,
    ):
        path = f"{self.path}/.sno-table"

        if field_cid_map is None:
            field_cid_map = self.field_cid_map
        if geom_cols is None:
            geom_cols = [self.geom_column_name]
        if primary_key is None:
            primary_key = self.primary_key

        pk_enc = self.encode_pk(row[primary_key])
        pk_hash = hashlib.sha1(
            pk_enc.encode("utf8")
        ).hexdigest()  # hash to randomly spread filenames

        feature_path = f"{path}/{pk_hash[0:2]}/{pk_hash[2:4]}/{pk_enc}"

        bin_feature = self.encode_feature(row, field_cid_map, geom_cols, primary_key)
        blob_id = repo.create_blob(bin_feature)
        entry = pygit2.IndexEntry(feature_path, blob_id, pygit2.GIT_FILEMODE_BLOB)
        index.add(entry)
        return [entry]

    def import_iter_feature_blobs(self, resultset, source):
        path = f"{self.path}/.sno-table"

        pk_field = source.primary_key

        for row in resultset:
            pk_enc = self.encode_pk(row[pk_field])
            pk_hash = hashlib.sha1(
                pk_enc.encode("utf8")
            ).hexdigest()  # hash to randomly spread filenames

            feature_path = f"{path}/{pk_hash[0:2]}/{pk_hash[2:4]}/{pk_enc}"
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

    def write_index(self, dataset_diff, index, repo, callback=None):
        pk_field = self.primary_key

        for k, (obj_old, obj_new) in dataset_diff["META"].items():
            object_path = f"{self.meta_path}/{k}"
            value = json.dumps(obj_new).encode("utf8")

            blob = repo.create_blob(value)
            idx_entry = pygit2.IndexEntry(object_path, blob, pygit2.GIT_FILEMODE_BLOB)
            index.add(idx_entry)

            if callback:
                callback(self, "META", object_path=object_path)

        for _, obj_old in dataset_diff["D"].items():
            object_path = "/".join(
                [self.path, self.get_feature_path(obj_old[pk_field])]
            )
            index.remove(object_path)

            if callback:
                callback(self, "D", object_path=object_path, obj_old=obj_old)

        for obj in dataset_diff["I"]:
            object_path = "/".join([self.path, self.get_feature_path(obj[pk_field])])
            bin_feature = self.encode_feature(obj)
            blob_id = repo.create_blob(bin_feature)
            entry = pygit2.IndexEntry(object_path, blob_id, pygit2.GIT_FILEMODE_BLOB)
            index.add(entry)

            if callback:
                callback(self, "I", object_path=object_path, obj_new=obj)

        for _, (obj_old, obj_new) in dataset_diff["U"].items():
            object_path = "/".join(
                [self.path, self.get_feature_path(obj_old[pk_field])]
            )
            index.remove(object_path)

            object_path = "/".join(
                [self.path, self.get_feature_path(obj_new[pk_field])]
            )
            bin_feature = self.encode_feature(obj_new)
            blob_id = repo.create_blob(bin_feature)
            entry = pygit2.IndexEntry(object_path, blob_id, pygit2.GIT_FILEMODE_BLOB)
            index.add(entry)

            if callback:
                callback(
                    self, "U", object_path=object_path, obj_old=obj_old, obj_new=obj_new
                )

        if callback:
            callback(self, "INDEX")

    def diff(self, other, pk_filter=None, reverse=False):
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
                my_pk = this.decode_pk(os.path.basename(d.old_file.path))

                self.L.debug("diff(): D %s (%s)", d.old_file.path, my_pk)

                _, my_obj = this.get_feature(my_pk, ogr_geoms=False)

                candidates_del[str(my_pk)].append((str(my_pk), my_obj))
            elif d.status == pygit2.GIT_DELTA_MODIFIED:
                my_pk = this.decode_pk(os.path.basename(d.old_file.path))
                other_pk = other.decode_pk(os.path.basename(d.new_file.path))

                self.L.debug(
                    "diff(): M %s (%s) -> %s (%s)",
                    d.old_file.path,
                    my_pk,
                    d.new_file.path,
                    other_pk,
                )

                _, my_obj = this.get_feature(my_pk, ogr_geoms=False)
                _, other_obj = other.get_feature(other_pk, ogr_geoms=False)

                candidates_upd[str(my_pk)] = (my_obj, other_obj)
            elif d.status == pygit2.GIT_DELTA_ADDED:
                other_pk = other.decode_pk(os.path.basename(d.new_file.path))

                self.L.debug("diff(): A %s (%s)", d.new_file.path, other_pk)

                _, other_obj = other.get_feature(other_pk, ogr_geoms=False)

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
