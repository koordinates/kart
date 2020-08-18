import functools
import logging
import time
from collections import deque

import click
import pygit2

from . import core
from .diff_structs import DatasetDiff, DeltaDiff, Delta
from .exceptions import (
    InvalidOperation,
    NotFound,
    NotYetImplemented,
    NO_COMMIT,
    PATCH_DOES_NOT_APPLY,
)
from .filter_util import UNFILTERED
from .geometry import geom_envelope
from .schema import Schema
from .serialise_util import ensure_bytes, json_pack
from .structure_version import get_structure_version


L = logging.getLogger("sno.structure")


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
        name = f"RepoStructureV{self.version}"
        if self._commit is not None:
            return f"{name}<{self.repo.path}@{self._commit.id}>"
        elif self._tree is not None:
            return f"{name}<{self.repo.path}@tree={self._tree.id}>"
        else:
            return f"{name}<{self.repo.path} <empty>>"

    @property
    def version(self):
        """Returns the dataset version to use for this entire repo."""
        return get_structure_version(self.repo, self.tree, maybe_v0=False)

    @property
    def dataset_dirname(self):
        return DatasetStructure.dataset_dirname(self.version)

    def decode_path(self, full_path):
        """
        Given a path in the sno repository - eg "path/to/dataset/.sno-dataset/49/3e/Bg==" -
        returns a tuple in either of the following forms:
        1. (dataset_path, "feature", primary_key)
        2. (dataset_path, "meta", meta_item_path)
        """
        dataset_dirname = self.dataset_dirname
        dataset_path, rel_path = full_path.split(f"/{dataset_dirname}/", 1)
        rel_path = f"{dataset_dirname}/{rel_path}"
        return (dataset_path,) + self.get(dataset_path).decode_path(rel_path)

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
            ds = DatasetStructure.instantiate(o, path, self.version)
            return ds

        raise KeyError(f"No valid dataset found at '{path}'")

    def __iter__(self):
        """ Iterate over available datasets in this repository """
        return self.iter_at(self.tree)

    def iter_at(self, tree):
        """ Iterate over available datasets in this repository using a specified Tree """
        to_examine = deque([("", tree)])

        dataset_version = self.version
        dataset_dirname = self.dataset_dirname

        while to_examine:
            path, tree = to_examine.popleft()

            for o in tree:
                # ignore everything other than directories
                if isinstance(o, pygit2.Tree):

                    if path:
                        te_path = "/".join([path, o.name])
                    else:
                        te_path = o.name

                    if dataset_dirname in o:
                        ds = DatasetStructure.instantiate(o, te_path, dataset_version)
                        yield ds
                    else:
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
            self._working_copy = WorkingCopy.get(self.repo)

        return self._working_copy

    @working_copy.deleter
    def working_copy(self):
        wc = self.working_copy
        if wc:
            wc.delete()
        del self._working_copy

    def create_tree_from_diff(self, diff, orig_tree=None):
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
            ds_diff = diff.get(ds.path)
            if ds_diff:
                ds.write_index(ds_diff, git_index, self.repo)

        L.info("Writing tree...")
        new_tree_oid = git_index.write_tree(self.repo)
        L.info(f"Tree sha: {new_tree_oid}")
        return new_tree_oid

    def commit(
        self, wcdiff, message, *, author=None, committer=None, allow_empty=False,
    ):
        """
        Update the repository structure and write the updated data to the tree
        as a new commit, setting HEAD to the new commit.
        NOTE: Doesn't update working-copy meta or tracking tables, this is the
        responsibility of the caller.
        """
        tree = self.tree

        git_index = pygit2.Index()
        git_index.read_tree(tree)

        new_tree_oid = self.create_tree_from_diff(wcdiff)
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
    def for_version(cls, version):
        from .dataset1 import Dataset1
        from .dataset2 import Dataset2

        version = int(version)
        if version == 1:
            return Dataset1
        elif version == 2:
            return Dataset2

        raise ValueError(f"No DatasetStructure found for version={version}")

    @classmethod
    def instantiate(cls, tree, path, version):
        """ Load a DatasetStructure from a Tree """
        if not isinstance(tree, pygit2.Tree):
            raise TypeError(f"Expected Tree object, got {type(tree)}")

        dataset_dirname = cls.dataset_dirname(version)
        if dataset_dirname not in tree:
            raise KeyError(f"No dataset at {path} - missing {dataset_dirname} tree")

        version_klass = cls.for_version(version)
        return version_klass(tree, path)

    @classmethod
    def dataset_dirname(cls, version):
        return cls.for_version(version).DATASET_DIRNAME

    # useful methods

    def full_path(self, rel_path):
        """Given a path relative to this dataset, returns its full path from the repo root."""
        return f"{self.path}/{rel_path}"

    def rel_path(self, full_path):
        """Given a full path to something in this dataset, returns its path relative to the dataset."""
        if not full_path.startswith(f"{self.path}/"):
            raise ValueError(f"{full_path} is not a descendant of {self.path}")
        return full_path[len(self.path) + 1 :]

    def ensure_rel_path(self, path):
        """Given either a relative path or a full path, return the equivalent relative path."""
        if path.startswith(self.DATASET_PATH):
            return path
        return self.rel_path(path)

    def decode_path(self, rel_path):
        """
        Given a path in this layer of the sno repository - eg ".sno-dataset/49/3e/Bg==" -
        returns a tuple in either of the following forms:
        1. ("feature", primary_key)
        2. ("meta", metadata_file_path)
        """
        if rel_path.startswith(self.DATASET_PATH):
            rel_path = rel_path[len(self.DATASET_PATH) :]
        if rel_path.startswith("meta/"):
            return ("meta", rel_path[len("meta/") :])
        pk = self.decode_path_to_1pk(rel_path)
        return ("feature", pk)

    @property
    def version(self):
        """Returns a version string eg '1.0'."""
        # TODO - get rid of per-dataset version - just use structure_version.
        raise NotImplementedError()

    @property
    @functools.lru_cache(maxsize=1)
    def meta_tree(self):
        return self.tree / self.META_PATH

    def _iter_meta_items(self, exclude=()):
        """Iterate over all meta items found in the meta tree."""
        exclude = set(exclude)
        for top_tree, top_path, subtree_names, blob_names in core.walk_tree(
            self.meta_tree
        ):
            for name in blob_names:
                if name in exclude:
                    continue

                meta_path = "/".join([top_path, name]) if top_path else name
                yield (meta_path, self.get_meta_item(meta_path))

            subtree_names[:] = [n for n in subtree_names if n not in exclude]

    def iter_meta_items(self):
        """
        Iterates through all meta items as (name, contents) tuples.
        This implementation returns everything stored in the meta tree.
        Subclasses can extend to also return generated meta-items,
        or to hide meta-items that are implementation details.
        """
        yield from self._iter_meta_items()

    def get_meta_item(self, name, missing_ok=False):
        """
        Returns the meta item with the given name.
        Subclasses can extend to generate meta-items on the fly,
        or to return the meta item in a format other than bytes.
        """
        leaf = None
        try:
            leaf = self.meta_tree / str(name)
            return leaf.data
        except (KeyError, AttributeError) as e:
            if missing_ok:
                return None
            raise KeyError(f"No meta-item found named {name}, type={type(leaf)}") from e

    @property
    @functools.lru_cache(maxsize=1)
    def has_geometry(self):
        return self.geom_column_name is not None

    @property
    @functools.lru_cache(maxsize=1)
    def geom_column_name(self):
        meta_geom = self.get_meta_item("gpkg_geometry_columns")
        return meta_geom["column_name"] if meta_geom else None

    def get_feature(self, pk_value):
        raise NotImplementedError()

    def feature_tuples(self, col_names, **kwargs):
        """ Feature iterator yielding tuples, ordered by the columns from col_names """

        # not optimised in V0
        for k, f in self.features():
            yield tuple(f[c] for c in col_names)

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

                e = geom_envelope(geom)
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

    _INSERT_UPDATE_DELETE = (
        pygit2.GIT_DELTA_ADDED,
        pygit2.GIT_DELTA_MODIFIED,
        pygit2.GIT_DELTA_DELETED,
    )
    _INSERT_UPDATE = (pygit2.GIT_DELTA_ADDED, pygit2.GIT_DELTA_MODIFIED)
    _UPDATE_DELETE = (pygit2.GIT_DELTA_MODIFIED, pygit2.GIT_DELTA_DELETED)

    def diff(self, other, ds_filter=UNFILTERED, reverse=False):
        """
        Generates a Diff from self -> other.
        If reverse is true, generates a diff from other -> self.
        """

        ds_diff = DatasetDiff()
        ds_diff["meta"] = self.diff_meta(other, reverse=reverse)

        ds_filter = ds_filter or UNFILTERED
        pk_filter = ds_filter.get("feature", ())

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
        # TODO - call diff_index.find_similar() to detect renames.

        if reverse:
            old, new = other, self
        else:
            old, new = self, other

        feature_diff = DeltaDiff()

        for d in diff_index.deltas:
            self.L.debug(
                "diff(): %s %s %s", d.status_char(), d.old_file.path, d.new_file.path
            )

            if d.old_file and d.old_file.path.startswith(self.META_PATH):
                continue
            elif d.new_file and d.new_file.path.startswith(self.META_PATH):
                continue

            if d.status in self._INSERT_UPDATE_DELETE:

                if d.status in self._UPDATE_DELETE:
                    old_path = d.old_file.path
                    old_pk = old.decode_path_to_1pk(old_path)
                else:
                    old_pk = None

                if d.status in self._INSERT_UPDATE:
                    new_path = d.new_file.path
                    new_pk = new.decode_path_to_1pk(d.new_file.path)
                else:
                    new_pk = None

                if old_pk not in pk_filter and new_pk not in pk_filter:
                    continue

                if d.status == pygit2.GIT_DELTA_ADDED:
                    self.L.debug("diff(): insert %s (%s)", new_path, new_pk)
                elif d.status == pygit2.GIT_DELTA_MODIFIED:
                    self.L.debug(
                        "diff(): update %s %s -> %s %s",
                        old_path,
                        old_pk,
                        new_path,
                        new_pk,
                    )
                elif d.status == pygit2.GIT_DELTA_DELETED:
                    self.L.debug("diff(): delete %s %s", old_path, old_pk)

                if d.status in self._UPDATE_DELETE:
                    old_feature_promise = functools.partial(
                        old.get_feature, old_pk, path=old_path, ogr_geoms=False,
                    )
                    old_half_delta = old_pk, old_feature_promise
                else:
                    old_half_delta = None

                if d.status in self._INSERT_UPDATE:
                    new_feature_promise = functools.partial(
                        new.get_feature, new_pk, path=new_path, ogr_geoms=False,
                    )
                    new_half_delta = new_pk, new_feature_promise
                else:
                    new_half_delta = None

                feature_diff.add_delta(Delta(old_half_delta, new_half_delta))

            else:
                # GIT_DELTA_RENAMED
                # GIT_DELTA_COPIED
                # GIT_DELTA_IGNORED
                # GIT_DELTA_TYPECHANGE
                # GIT_DELTA_UNMODIFIED
                # GIT_DELTA_UNREADABLE
                # GIT_DELTA_UNTRACKED
                raise NotImplementedError(f"Delta status: {d.status_char()}")

        # TODO - detect renames by comparing blob ID

        ds_diff["feature"] = feature_diff
        return ds_diff

    def diff_meta(self, other, reverse=False):
        """
        Generates a diff from self -> other, but only for meta items.
        If reverse is true, generates a diff from other -> self.
        """
        if reverse:
            old, new = other, self
        else:
            old, new = self, other

        meta_old = dict(old.iter_meta_items()) if old else {}
        meta_new = dict(new.iter_meta_items()) if new else {}
        return DeltaDiff.diff_dicts(meta_old, meta_new)

    def write_index(self, dataset_diff, index, repo):
        """
        Given a diff that only affects this dataset, write it to the given index + repo.
        Blobs will be created in the repo, and referenced in the index, but the index is
        not committed - this is the responsibility of the caller.
        """
        # TODO - support multiple primary keys.
        # TODO - support writing new schemas
        pk_field = self.primary_key

        conflicts = False
        encode_kwargs = {}

        if "meta" in dataset_diff and self.version < 2:
            raise NotYetImplemented(
                f"Meta changes are not supported for version {self.version}"
            )

        for delta in dataset_diff.get("meta", {}).values():
            name = delta.key
            rel_path = f"{self.META_PATH}{name}"
            full_path = self.full_path(rel_path)
            if delta.type == "delete":
                if full_path not in index:
                    conflicts = True
                    click.echo(
                        f"{self.path}: Trying to delete nonexistent meta item: {name}"
                    )
                    continue
                index.remove(full_path)

            elif delta.type == "insert":
                if full_path in index:
                    conflicts = True
                    click.echo(
                        f"{self.path}: Trying to create meta item that already exists: {name}"
                    )
                    continue
                if full_path.endswith(".json"):
                    blob_id = repo.create_blob(json_pack(delta.new.value))
                else:
                    blob_id = repo.create_blob(ensure_bytes(delta.new.value))
                entry = pygit2.IndexEntry(full_path, blob_id, pygit2.GIT_FILEMODE_BLOB)
                index.add(entry)

            elif delta.type == "update":
                if full_path not in index:
                    conflicts = True
                    click.echo(
                        f"{self.path}: Trying to update nonexistent meta item: {name}"
                    )
                    continue

                if self.get_meta_item(name) != delta.old.value:
                    conflicts = True
                    click.echo(
                        f"{self.path}: Trying to update already-changed meta item: {name}"
                    )
                    continue
                index.remove(full_path)
                if name == "schema.json":
                    old_schema = Schema.from_column_dicts(delta.old.value)
                    new_schema = Schema.from_column_dicts(delta.new.value)
                    if not old_schema.is_pk_compatible(new_schema):
                        raise NotYetImplemented(
                            "Schema changes that involve primary key changes are not yet supported"
                        )

                    encode_kwargs = {"schema": new_schema}
                    to_write = [
                        self.encode_schema(new_schema),
                        self.encode_legend(new_schema.legend),
                    ]
                elif full_path.endswith(".json"):
                    to_write = [(full_path, json_pack(delta.new.value))]
                else:
                    to_write = [(full_path, ensure_bytes(delta.new.value))]
                for path, data in to_write:
                    blob_id = repo.create_blob(data)
                    entry = pygit2.IndexEntry(path, blob_id, pygit2.GIT_FILEMODE_BLOB)
                    index.add(entry)

        geom_column_name = self.geom_column_name
        for delta in dataset_diff.get("feature", {}).values():
            if delta.type == "delete":
                pk = delta.old.value[pk_field]
                feature_path = self.encode_1pk_to_path(pk)
                if feature_path not in index:
                    conflicts = True
                    click.echo(
                        f"{self.path}: Trying to delete nonexistent feature: {pk}"
                    )
                    continue
                index.remove(feature_path)

            elif delta.type == "insert":
                pk = delta.new.value[pk_field]
                feature_path, feature_data = self.encode_feature(
                    delta.new.value, **encode_kwargs
                )
                if feature_path in index:
                    conflicts = True
                    click.echo(
                        f"{self.path}: Trying to create feature that already exists: {pk}"
                    )
                    continue
                blob_id = repo.create_blob(feature_data)
                entry = pygit2.IndexEntry(
                    feature_path, blob_id, pygit2.GIT_FILEMODE_BLOB
                )
                index.add(entry)

            elif delta.type == "update":
                old_pk = delta.old.value[pk_field]
                old_feature_path = self.encode_1pk_to_path(old_pk)
                if old_feature_path not in index:
                    conflicts = True
                    click.echo(
                        f"{self.path}: Trying to update nonexistent feature: {old_pk}"
                    )
                    continue

                actual_existing_feature = self.get_feature(old_pk)
                old_feature = delta.old.value
                if geom_column_name:
                    # FIXME: actually compare the geometries here.
                    # Turns out this is quite hard - geometries are hard to compare sanely.
                    # Even if we add hacks to ignore endianness, WKB seems to vary a bit,
                    # and ogr_geometry.Equal(other) can return false for seemingly-identical geometries...
                    actual_existing_feature.pop(geom_column_name)
                    old_feature = old_feature.copy()
                    old_feature.pop(geom_column_name)
                if actual_existing_feature != old_feature:
                    conflicts = True
                    click.echo(
                        f"{self.path}: Trying to update already-changed feature: {old_pk}"
                    )
                    continue

                index.remove(old_feature_path)
                new_feature_path, new_feature_data = self.encode_feature(
                    delta.new.value, **encode_kwargs
                )
                blob_id = repo.create_blob(new_feature_data)
                entry = pygit2.IndexEntry(
                    new_feature_path, blob_id, pygit2.GIT_FILEMODE_BLOB
                )
                index.add(entry)

        if conflicts:
            raise InvalidOperation(
                "Patch does not apply", exit_code=PATCH_DOES_NOT_APPLY,
            )
