import contextlib
import functools

import json
import logging
import re
import subprocess
import time
from collections import deque
from pathlib import Path

import click
import pygit2

from . import core, gpkg
from .exceptions import NotFound, NO_COMMIT

L = logging.getLogger("sno.structure")


def fast_import_tables(
    repo, sources, max_pack_size="2G", limit=None, message=None, *, version
):
    for path, source in sources.items():
        if not source.table:
            raise ValueError("No table specified")

        if not repo.is_empty:
            if path in repo.head.peel(pygit2.Tree):
                raise ValueError(f"{path}/ already exists")

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
    )
    try:
        user = repo.default_signature

        if message is None:
            if len(sources) == 1:
                for path, source in sources.items():
                    message = f"Import from {Path(source.source).name} to {path}/"
            else:
                message = f"Import {len(sources)} datasets from '{Path(source.source).name}':\n"
                for path, source in sources.items():
                    if path == source.table:
                        message += f"\n* {path}/"
                    else:
                        message += f"\n* {path} (from {source.table})"

        header = (
            "commit refs/heads/master\n"
            f"committer {user.name} <{user.email}> now\n"
            f"data {len(message.encode('utf8'))}\n{message}\n"
        )
        p.stdin.write(header.encode("utf8"))

        if not repo.is_empty:
            # start with the existing tree/contents
            p.stdin.write(b"from refs/heads/master^0\n")
        for path, source in sources.items():
            dataset = DatasetStructure.for_version(version)(tree=None, path=path)

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
                src_iterator = source.iter_features()

                t1 = time.monotonic()
                click.echo(f"Source setup in {t1-t0:.1f}s")

                for i, (blob_path, blob_data) in enumerate(
                    dataset.import_iter_meta_blobs(repo, source)
                ):
                    p.stdin.write(
                        f"M 644 inline {blob_path}\ndata {len(blob_data)}\n".encode(
                            "utf8"
                        )
                    )
                    p.stdin.write(blob_data)
                    p.stdin.write(b"\n")

                # features
                t2 = time.monotonic()
                for i, (blob_path, blob_data) in enumerate(
                    dataset.import_iter_feature_blobs(src_iterator, source)
                ):
                    p.stdin.write(
                        f"M 644 inline {blob_path}\ndata {len(blob_data)}\n".encode(
                            "utf8"
                        )
                    )
                    p.stdin.write(blob_data)
                    p.stdin.write(b"\n")

                    if i and i % 100000 == 0:
                        click.echo(f"  {i:,d} features... @{time.monotonic()-t2:.1f}s")

                    if limit is not None and i == (limit - 1):
                        click.secho(f"  Stopping at {limit:,d} features", fg="yellow")
                        break
                t3 = time.monotonic()
                click.echo(f"Added {num_rows:,d} Features to index in {t3-t2:.1f}s")
                click.echo(
                    f"Overall rate: {(num_rows/(t3-t2 or 1E-3)):.0f} features/s)"
                )

        p.stdin.write(b"\ndone\n")
    except BrokenPipeError as e:
        # if git-fast-import dies early, we get an EPIPE here
        # we'll deal with it below
        pass
    else:
        p.stdin.close()
    p.wait()
    if p.returncode != 0:
        raise subprocess.CalledProcessError(f"Error! {p.returncode}", "git-fast-import")
    t4 = time.monotonic()
    click.echo(f"Closed in {(t4-t3):.0f}s")


class RepositoryStructure:
    @staticmethod
    def lookup(repo, key):
        L.debug(f"key={key}")
        if isinstance(key, pygit2.Oid):
            key = key.hex
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
        self.L = logging.getLogger(self.__class__.__qualname__)
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
        elif self._tree is not None:
            return f"RepoStructure<{self.repo.path}@tree={self._tree.id}>"
        else:
            return f"RepoStructure<{self.repo.path} <empty>>"

    def decode_path(self, path):
        """
        Given a path in the sno repository - eg "table_A/.sno-table/49/3e/Bg==" -
        returns a tuple in either of the following forms:
        1. (table, "feature", primary_key)
        2. (table, "meta", metadata_file_path)
        """
        table, table_path = path.split("/.sno-table/", 1)
        return (table,) + self.get(table).decode_path(table_path)

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

    @property
    def id(self):
        obj = self._commit or self._tree
        return obj.id if obj is not None else None

    @property
    def short_id(self):
        obj = self._commit or self._tree
        return obj.short_id if obj is not None else None

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

    def create_tree_from_diff(self, diff, orig_tree=None, callback=None):
        """
        Given a tree and a diff, returns a new tree created by applying the diff.

        Doesn't create any commits or modify the working copy at all.

        If orig_tree is not None, the diff is applied from that tree.
        Otherwise, uses the tree at the head of the repo.
        """
        if orig_tree is None:
            orig_tree = self.tree

        git_index = pygit2.Index()
        git_index.read_tree(orig_tree)

        for ds in self.iter_at(orig_tree):
            ds.write_index(diff[ds], git_index, self.repo, callback=callback)

        L.info("Writing tree...")
        new_tree_oid = git_index.write_tree(self.repo)
        L.info(f"Tree sha: {new_tree_oid}")
        return new_tree_oid

    def commit(
        self,
        wcdiff,
        message,
        *,
        author=None,
        committer=None,
        allow_empty=False,
        update_working_copy_head=True,
    ):
        tree = self.tree

        git_index = pygit2.Index()
        git_index.read_tree(tree)

        wc = self.working_copy
        commit_callback = None
        if update_working_copy_head and wc:
            context = wc.session()
            commit_callback = wc.commit_callback
        else:
            # This happens when commit is called from `sno apply` in a bare repo
            context = contextlib.nullcontext()
        with context:
            new_tree_oid = self.create_tree_from_diff(wcdiff, callback=commit_callback)

            if commit_callback:
                commit_callback(None, "TREE", tree=new_tree_oid)

            L.info("Committing...")
            user = self.repo.default_signature
            # this will also update the ref (branch) to point to the current commit
            new_commit = self.repo.create_commit(
                "HEAD",  # reference_name
                author or user,  # author
                committer or user,  # committer
                message,  # message
                new_tree_oid,  # tree
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
        from . import dataset1  # noqa

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

    def import_meta_items(self, source):
        yield ("version", {"version": self.VERSION_IMPORT})
        for name, value in source.build_meta_info():
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
