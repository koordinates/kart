import base64
import functools
import hashlib
import json
import logging
import os
import re
import subprocess
import time
import uuid
from collections import deque
from pathlib import Path

import click
import msgpack
import pygit2

from . import core, gpkg


class RepositoryStructure:
    def __init__(self, repo):
        self.L = logging.getLogger(__class__.__qualname__)

        self.repo = repo

    def __getitem__(self, path):
        """ Get a specific dataset by path """
        root = self.repo.head.peel(pygit2.Tree)
        try:
            tree_entry = root[path]
        except KeyError:
            raise

        if tree_entry.type == 'tree':
            ds = self._load_dataset(tree_entry.obj, path)
            if ds:
                return ds

        raise KeyError(f"No valid dataset found at '{path}'")

    def _load_dataset(self, tree, path):
        for version_klass in DatasetStructure.all_versions():
            ds = version_klass.instantiate(tree, path)
            if ds is not None:
                return ds

        return None

    def __iter__(self):
        """ Iterate over available datasets in this repository """
        to_examine = deque([('', self.repo.head.peel(pygit2.Tree))])

        while to_examine:
            path, tree = to_examine.popleft()

            for te in tree:
                # ignore everything other than directories
                if te.type == "tree":

                    if path:
                        te_path = '/'.join([path, te.name])
                    else:
                        te_path = te.name

                    try:
                        ds = self._load_dataset(te.obj, te_path)
                        if ds is not None:
                            yield ds
                            continue
                    except IntegrityError:
                        self.L.warn("Error loading dataset from %s, ignoring tree", te_path, exc_info=True)
                        continue

                    # examine inside this directory
                    to_examine.append((te_path, te.obj))


class IntegrityError(ValueError):
    pass


class DatasetStructure:
    DEFAULT_IMPORT_VERSION = '0.0.1'
    META_PATH = 'meta'

    def __init__(self, tree, path):
        self.tree = tree
        self.path = path.strip('/')
        self.name = self.path.rsplit('/', 1)[-1]
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
        versions.sort(key=lambda v: (v == DatasetStructure.DEFAULT_IMPORT_VERSION, [int(i) for i in v.split('.')]), reverse=True)
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
                te_version = (tree[version_klass.VERSION_PATH])
            except KeyError:
                continue
            else:
                L.debug("Found candidate %s tree at: %s", version_klass.VERSION_SPECIFIER, path)

            if te_version.type != 'blob':
                raise IntegrityError(f"{version_klass.__name__}: {path}/{version_klass.VERSION_PATH} isn't a blob ({te_version.type})")

            blob = te_version.obj
            try:
                d = json.loads(blob.data)
            except Exception as e:
                raise IntegrityError(f"{version_klass.__name__}: Couldn't load version file from: {path}/{version_klass.VERSION_PATH}") from e

            version = d.get('version', None)
            if version and version.startswith(version_klass.VERSION_SPECIFIER):
                L.debug("Found %s dataset at: %s", version, path)
                return version_klass(tree, path)
            else:
                continue

        raise ValueError(f"{path}: Couldn't find any Dataset Structure version that matched")

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
            te = meta_tree / name
        except KeyError:
            return None

        if te.type != 'blob':
            raise ValueError(f"meta/{name} is a {te.type}, expected blob")

        return json.loads(te.obj.data)

    @property
    @functools.lru_cache(maxsize=1)
    def geom_column_name(self):
        meta_geom = self.get_meta_item("gpkg_geometry_columns")
        return meta_geom["column_name"] if meta_geom else None

    def get_feature(self, pk_value):
        raise NotImplementedError()

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
            entry = pygit2.IndexEntry(
                blob_path,
                blob_id,
                pygit2.GIT_FILEMODE_BLOB
            )
            index.add(entry)

    def import_meta_items(self, source):
        return source.build_meta_info(repo_version=self.VERSION_IMPORT)

    def import_iter_meta_blobs(self, repo, source):
        for name, value in self.import_meta_items(source):
            yield (f"{self.path}/{self.META_PATH}/{name}", value.encode('utf8'))

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

        click.echo(f"Importing {source} to {path}/ ...")

        with source:
            index = pygit2.Index()
            if head_tree:
                index.read_tree(head_tree)

            click.echo("Writing meta bits...")
            self.import_meta(repo, index, source)

            row_count = source.row_count
            click.echo(f"Found {row_count} features in {table}")

            # iterate features
            t0 = time.time()
            t1 = None
            for i, source_feature in enumerate(source.iter_features()):
                if not t1:
                    t1 = time.time()
                    click.echo(f"Query ran in {t1-t0:.1f}s")

                self.import_feature(source_feature, repo, index, source, path)

                if i and i % 500 == 0:
                    click.echo(f"  {i+1:,d} features... @{time.time()-t1:.1f}s")

            t2 = time.time()

            click.echo(f"Added {i+1} Features to index in {t2-t1:.1f}s")
            click.echo(f"Overall rate: {((i+1)/(t2-t0)):.0f} features/s)")

            click.echo("Writing tree...")
            tree_id = index.write_tree(repo)
            del index
            t3 = time.time()
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
            t4 = time.time()
            click.echo(f"Commit: {commit} (in {(t4-t3):.0f}s)")

            click.echo(f"Garbage-collecting...")
            subprocess.check_call(["git", "-C", repo.path, "gc"])
            t5 = time.time()
            click.echo(f"GC completed in {(t5-t4):.1f}s")

    def fast_import_table(self, repo, source, iter_func=1, max_pack_size='2G', limit=None):

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
                click.echo(f"Importing {num_rows}/{source.row_count} features from {source} to {path}/ ...")
            else:
                num_rows = source.row_count
                click.echo(f"Importing {num_rows} features from {source} to {path}/ ...")

            t0 = time.time()
            if iter_func == 2:
                src_iterator = source.iter_features_sorted(self.get_feature_path, limit=limit)
            else:
                src_iterator = source.iter_features()
            t1 = time.time()
            click.echo(f"Source setup in {t1-t0:.1f}s")

            click.echo("Starting git-fast-import...")
            p = subprocess.Popen(
                [
                    "git", "fast-import",
                    "--date-format=now",
                    "--done",
                    "--stats",
                    f"--max-pack-size={max_pack_size}"
                ],
                cwd=repo.path,
                stdin=subprocess.PIPE,
                bufsize=1,  # line
            )

            user = repo.default_signature

            header = (
                'commit refs/heads/master\n'
                f'committer {user.name} <{user.email}> now\n'
                f'data <<EOM\nImport from {Path(source.source).name} to {path}/\nEOM\n'
            )
            p.stdin.write(header.encode('utf8'))

            if not repo.is_empty:
                # start with the existing tree/contents
                p.stdin.write(b'from refs/heads/master^0\n')

            for blob_path, blob_data in self.import_iter_meta_blobs(repo, source):
                p.stdin.write(f'M 644 inline {blob_path}\ndata {len(blob_data)}\n'.encode('utf8'))
                p.stdin.write(blob_data)
                p.stdin.write(b'\n')

            # features
            t2 = time.time()
            for i, (blob_path, blob_data) in enumerate(self.import_iter_feature_blobs(src_iterator, source)):
                p.stdin.write(f'M 644 inline {blob_path}\ndata {len(blob_data)}\n'.encode('utf8'))
                p.stdin.write(blob_data)
                p.stdin.write(b'\n')

                if i and i % 100000 == 0:
                    click.echo(f"  {i:,d} features... @{time.time()-t2:.1f}s")

                if limit is not None and i == (limit-1):
                    click.secho(f"  Stopping at {limit} features", fg='yellow')
                    break

            p.stdin.write(b'\ndone\n')
            t3 = time.time()
            click.echo(f"Added {num_rows} Features to index in {t3-t2:.1f}s")
            click.echo(f"Overall rate: {(num_rows/(t3-t2)):.0f} features/s)")

            p.stdin.close()
            p.wait()
            if p.returncode != 0:
                raise subprocess.CalledProcessError(f"Error! {p.returncode}", "git-fast-import")
            t4 = time.time()
            click.echo(f"Closed in {(t4-t3):.0f}s")


class Dataset00(DatasetStructure):
    VERSION_PATH = "meta/version"
    VERSION_SPECIFIER = "0.0."
    VERSION_IMPORT = '0.0.1'

    def repo_feature_to_dict(self, pk, tree):
        tree_entries = [te for te in tree if te.type == 'blob']
        return core.feature_blobs_to_dict(tree_entries, self.geom_column_name)

    def features(self):
        # layer-name/
        #   features/
        #     {uuid[:4]}/
        #       {uuid}/
        #         {field} => value
        #         ...
        #       ...
        #     ...
        for te_ftree_prefix in (self.tree / "features").obj:
            if te_ftree_prefix.type != "tree":
                continue

            ftree_prefix = te_ftree_prefix.obj

            for te_ftree in ftree_prefix:
                feature = self.repo_feature_to_dict(None, te_ftree.obj)
                yield (te_ftree.name, feature)

    @property
    @functools.lru_cache(maxsize=1)
    def primary_key(self):
        schema = self.get_meta_item('sqlite_table_info')
        field = next(f for f in schema if f['pk'])
        return field['name']

    def get_feature(self, pk_value):
        pk_field = self.primary_key
        for tree, path, subtrees, blobs in core.walk_tree((self.tree / 'features').obj, path='features'):
            m = re.match(r'features/([0-9a-f]{4})/([0-9a-f-]{36})$', path)
            if m:
                pk_blob = (tree / pk_field).obj
                if json.loads(pk_blob.data) == pk_value:
                    return m.group(2), self.repo_feature_to_dict(None, tree)

        raise KeyError(pk_value)

    def import_feature(self, row, repo, index, source, path=None):
        """
            layer-name/
              features/
                {uuid[:4]}/
                  {uuid}/
                    {field} => value
                    ...
                  ...
                ...
        """
        feature_id = str(uuid.uuid4())
        path = path or self.path

        for field in row.keys():
            object_path = f"{path}/features/{feature_id[:4]}/{feature_id}/{field}"

            value = row[field]
            if not isinstance(value, bytes):  # blob
                value = json.dumps(value).encode("utf8")

            blob_id = repo.create_blob(value)
            entry = pygit2.IndexEntry(
                object_path, blob_id, pygit2.GIT_FILEMODE_BLOB
            )
            index.add(entry)
        # click.echo(feature_id, object_path, field, value, entry)

    def import_iter_feature_blobs(self, resultset, source):
        path = self.path

        for row in resultset:
            feature_id = str(uuid.uuid4())

            for field in row.keys():
                object_path = f"{path}/features/{feature_id[:4]}/{feature_id}/{field}"

                value = row[field]
                if not isinstance(value, bytes):  # blob
                    value = json.dumps(value).encode("utf8")

                yield (object_path, value)


class Dataset01(DatasetStructure):
    """
    Experimental repository structure:
    - messagePack
    - primary key values
    - add at any location: `snowdrop import GPKG:my.gpkg:mytable path/to/mylayer`

    any/structure/mylayer/
      .sno-table/
        meta/
          primary_key
          fields/
            {field}  # map to attribute-id
            ...
        {pk-hash:2}/
          {pk-hash:2}/
            {pk-value}/
              {attribute-id}

    """
    VERSION_PATH = ".sno-table/meta/version"
    VERSION_SPECIFIER = "0.1."
    VERSION_IMPORT = '0.1.0'

    MSGPACK_EXT_GEOM = 71  # 'G'
    META_PATH = '.sno-table/meta'

    def _msgpack_unpack_ext(self, code, data):
        if code == self.MSGPACK_EXT_GEOM:
            return data  # bytes
        else:
            self.L.warn("Unexpected msgpack extension: %d", code)
            return msgpack.ExtType(code, data)

    @property
    @functools.lru_cache(maxsize=1)
    def field_cid_map(self):
        cid_map = {}
        for te in (self.meta_tree / 'fields').obj:
            if te.type != 'blob':
                self.L.warn("field_cid_map: Unexpected TreeEntry type=%s @ meta/fields/%s", te.type, te.name)
                continue

            cid = json.loads(te.obj.data)
            field_name = te.name
            cid_map[cid] = field_name
        return cid_map

    @property
    def primary_key(self):
        return self.get_meta_item('primary_key')

    def encode_pk(self, pk):
        pk_enc = msgpack.packb(pk, use_bin_type=True)  # encode pk value via msgpack
        pk_str = base64.urlsafe_b64encode(pk_enc).decode('utf8')  # filename safe
        return pk_str

    def decode_pk(self, encoded):
        return msgpack.unpackb(base64.urlsafe_b64decode(encoded), raw=False)

    def get_feature_path(self, pk):
        pk_enc = self.encode_pk(pk)
        pk_hash = hashlib.sha1(pk_enc.encode('utf8')).hexdigest()  # hash to randomly spread filenames
        return os.path.join('.sno-table', pk_hash[:2], pk_hash[2:4], pk_enc)

    def repo_feature_to_dict(self, pk, tree):
        feature = {
            self.primary_key: self.decode_pk(pk),
        }
        for te in tree:
            if te.type != "blob":
                self.L.warn("repo_feature_to_dict: Unexpected TreeEntry type=%s in feature tree '%s'", te.type, pk)
                continue

            blob = te.obj
            colid = int(te.name, 16)
            field_name = self.field_cid_map[colid]
            feature[field_name] = msgpack.unpackb(
                blob.data,
                ext_hook=self._msgpack_unpack_ext,
                raw=False
            )

        return feature

    def get_feature(self, pk_value):
        pk_enc = self.encode_pk(pk_value)
        pk_hash = hashlib.sha1(pk_enc.encode('utf8')).hexdigest()  # hash to randomly spread filenames

        te = (self.tree / '.sno-table' / pk_hash[:2] / pk_hash[2:4] / pk_enc)
        if te.type != 'tree':
            raise IntegrityError(f"Unexpected TreeEntry type={te.type} in feature tree {pk_enc}")

        return pk_enc, self.repo_feature_to_dict(pk_enc, te.obj)

    def features(self):
        top_tree = (self.tree / '.sno-table').obj

        # .sno-table/
        #   {hex(pk-hash):2}/
        #     {hex(pk-hash):2}/
        #       {base64(pk-value)}/
        #         {hex(field-id)}
        URLSAFE_B64 = r"A-Za-z0-9_\-"
        RE_FEATURE_PATH = re.compile(fr'(?:[{URLSAFE_B64}]{{4}})*(?:[{URLSAFE_B64}]{{2}}==|[{URLSAFE_B64}]{{3}}=)?$')
        RE_PREFIX_PATH = re.compile(r'([0-9a-f]{2})(?:/([0-9a-f]{2}))?$')

        for tree, path, subtrees, blobs in core.walk_tree(top_tree, path=''):
            m = RE_PREFIX_PATH.match(path)
            if m and m.group(2) is not None:
                for d in subtrees:
                    if RE_FEATURE_PATH.match(d):
                        te_d = (tree / d)
                        if te_d.type != 'tree':
                            self.L.warn("features: Unexpected TreeEntry type=%s in feature tree '%s/%s'", te_d.type, path, d)
                            continue

                        if hashlib.sha1(d.encode('utf8')).hexdigest()[0:4] != f"{m.group(1)}{m.group(2)}":
                            self.L.warn("features: feature prefix doesn't match hash(pk): %s/%s", path, d)

                        yield d, self.repo_feature_to_dict(d, te_d.obj)
            elif m or path == '':
                subtrees[:] = [d for d in subtrees if RE_PREFIX_PATH.match(os.path.join(path, d))]
                continue

            subtrees.clear()

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
            yield (f'fields/{colname}', json.dumps(colid))

        pk_field = source.primary_key
        yield ('primary_key', json.dumps(pk_field))

    def import_feature(self, row, repo, index, source, path):
        path = f"{self.path}/.sno-table"

        pk_field = source.primary_key

        pk_enc = self.encode_pk(row[pk_field])
        pk_hash = hashlib.sha1(pk_enc.encode('utf8')).hexdigest()  # hash to randomly spread filenames

        feature_path = f"{path}/{pk_hash[0:2]}/{pk_hash[2:4]}/{pk_enc}"
        for field in row.keys():
            if field == pk_field:
                continue

            field_id = source.field_cid_map[field]
            object_path = f"{feature_path}/{field_id:x}"
            value = row[field]
            if field in source.geom_cols:
                if value is not None:
                    value = msgpack.ExtType(self.MSGPACK_EXT_GEOM, value)

            blob_id = repo.create_blob(msgpack.packb(value, use_bin_type=True))
            entry = pygit2.IndexEntry(
                object_path, blob_id, pygit2.GIT_FILEMODE_BLOB
            )
            index.add(entry)

            # click.echo(pk_val, object_path, field, value, entry)

    def import_iter_feature_blobs(self, resultset, source):
        path = f"{self.path}/.sno-table"

        pk_field = source.primary_key

        for row in resultset:
            pk_enc = self.encode_pk(row[pk_field])
            pk_hash = hashlib.sha1(pk_enc.encode('utf8')).hexdigest()  # hash to randomly spread filenames

            feature_path = f"{path}/{pk_hash[0:2]}/{pk_hash[2:4]}/{pk_enc}"

            for field in row.keys():
                if field == pk_field:
                    continue

                field_id = source.field_cid_map[field]
                object_path = f"{feature_path}/{field_id:x}"
                value = row[field]
                if field in source.geom_cols:
                    if value is not None:
                        value = msgpack.ExtType(self.MSGPACK_EXT_GEOM, value)

                yield (object_path, msgpack.packb(value, use_bin_type=True))


class Dataset02(Dataset01):
    """
    Experimental repository structure:
    - messagePack
    - primary key values
    - blob per feature
    - add at any location: `snowdrop import GPKG:my.gpkg:mytable path/to/mylayer`

    any/structure/mylayer/
      .sno-table/
        meta/
          primary_key
          fields/
            {field}  # map to attribute-id
            ...
        {pk-hash:2}/
          {pk-hash:2}/
            {pk-value}  # {attribute-id: attribute-value, ...}

    """
    VERSION_SPECIFIER = "0.2."
    VERSION_IMPORT = '0.2.0'

    def repo_feature_to_dict(self, pk, blob):
        feature = {
            self.primary_key: self.decode_pk(pk),
        }
        bin_feature = msgpack.unpackb(
            blob.data,
            ext_hook=self._msgpack_unpack_ext,
            raw=False
        )
        for colid, value in bin_feature.items():
            field_name = self.field_cid_map[colid]
            feature[field_name] = value

        return feature

    def get_feature(self, pk_value):
        pk_enc = self.encode_pk(pk_value)
        pk_hash = hashlib.sha1(pk_enc.encode('utf8')).hexdigest()  # hash to randomly spread filenames

        te = (self.tree / '.sno-table' / pk_hash[:2] / pk_hash[2:4] / pk_enc)
        if te.type != 'blob':
            raise IntegrityError(f"Unexpected TreeEntry type={te.type} in feature tree {pk_enc}")

        return pk_enc, self.repo_feature_to_dict(pk_enc, te.obj)

    def features(self):
        top_tree = (self.tree / '.sno-table').obj

        # .sno-table/
        #   {hex(pk-hash):2}/
        #     {hex(pk-hash):2}/
        #       {base64(pk-value)}/
        #         {hex(field-id)}
        URLSAFE_B64 = r"A-Za-z0-9_\-"
        RE_FEATURE_PATH = re.compile(fr'(?:[{URLSAFE_B64}]{{4}})*(?:[{URLSAFE_B64}]{{2}}==|[{URLSAFE_B64}]{{3}}=)?$')
        RE_PREFIX_PATH = re.compile(r'([0-9a-f]{2})(?:/([0-9a-f]{2}))?$')

        for tree, path, subtrees, blobs in core.walk_tree(top_tree, path=''):
            m = RE_PREFIX_PATH.match(path)
            if m and m.group(2) is not None:
                for b in blobs:
                    if RE_FEATURE_PATH.match(b):
                        te_b = (tree / b)
                        if te_b.type != 'blob':
                            self.L.warn("features: Unexpected TreeEntry type=%s in feature tree '%s/%s'", te_b.type, path, b)
                            continue

                        if hashlib.sha1(b.encode('utf8')).hexdigest()[0:4] != f"{m.group(1)}{m.group(2)}":
                            self.L.warn("features: feature prefix doesn't match hash(pk): %s/%s", path, b)

                        yield b, self.repo_feature_to_dict(b, te_b.obj)
            elif m or path == '':
                subtrees[:] = [d for d in subtrees if RE_PREFIX_PATH.match(os.path.join(path, d))]
                continue

            subtrees.clear()

    def import_feature(self, row, repo, index, source, path):
        path = f"{self.path}/.sno-table"

        pk_field = source.primary_key

        pk_enc = self.encode_pk(row[pk_field])
        pk_hash = hashlib.sha1(pk_enc.encode('utf8')).hexdigest()  # hash to randomly spread filenames

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

        blob_id = repo.create_blob(msgpack.packb(bin_feature, use_bin_type=True))
        entry = pygit2.IndexEntry(
            feature_path, blob_id, pygit2.GIT_FILEMODE_BLOB
        )
        index.add(entry)

        # click.echo(pk_val, feature_path, bin_feature, entry)

    def import_iter_feature_blobs(self, resultset, source):
        path = f"{self.path}/.sno-table"

        pk_field = source.primary_key

        for row in resultset:
            pk_enc = self.encode_pk(row[pk_field])
            pk_hash = hashlib.sha1(pk_enc.encode('utf8')).hexdigest()  # hash to randomly spread filenames

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
