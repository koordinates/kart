import logging
from collections import deque
import re
from typing import Optional

import click
import pygit2

from .exceptions import (
    NO_CHANGES,
    NO_COMMIT,
    PATCH_DOES_NOT_APPLY,
    SCHEMA_VIOLATION,
    InvalidOperation,
    NotFound,
    NotYetImplemented,
)
from .pack_util import packfile_object_builder
from .tabular.schema import Schema
from .tabular.version import extra_blobs_for_version, dataset_class_for_version
from .structs import CommitWithReference
from .unsupported_dataset import UnsupportedDataset

L = logging.getLogger("kart.structure")


_DOT = r"\."
_NON_SLASHES = "[^/]*"
DATASET_DIRNAME_PATTERN = re.compile(rf"{_DOT}{_NON_SLASHES}-dataset{_NON_SLASHES}")
DATASET_PATH_PATTERN = re.compile(f"/{DATASET_DIRNAME_PATTERN.pattern}/")


class RepoStructure:
    """
    The internal structure of a Kart repository, at a particular revision.
    The Kart revision's structure is almost entirely comprised of its datasets, but this may change.
    The datasets can be accessed at self.datasets, but there is also a shortcut that skips this class - instead of:

    >>> kart_repo.structure(commit_hash).datasets

    You can use:

    >>> kart_repo.datasets(commit_hash)
    """

    @staticmethod
    def resolve_refish(repo, refish):
        """
        Given a ref / refish / commit / tree / OID, returns as many as possible of the following:
        >>> (ref, commit, tree)
        """
        if refish is None or refish == "HEAD":
            return "HEAD", repo.head_commit, repo.head_tree

        # We support X^?  - meaning X^ if X^ exists otherwise [EMPTY]
        if isinstance(refish, str) and refish.endswith("^?"):
            commit = CommitWithReference.resolve(repo, refish[:-2]).commit
            try:
                if commit.parents:
                    refish = refish[:-1]  # Commit has parents - use X^.
                else:
                    refish = "[EMPTY]"  # Commit has no parents - use [EMPTY]
            except KeyError:
                # One or more parents doesn't exist.
                # This is okay if this is the first commit of a shallow clone (how to tell?)
                refish = "[EMPTY]"

        # We support [EMPTY] meaning the empty tree.
        if refish == "[EMPTY]":
            return "[EMPTY]", None, repo.empty_tree

        if isinstance(refish, pygit2.Oid):
            refish = refish.hex

        if isinstance(refish, (pygit2.Commit, pygit2.Tree)):
            return (None, *RepoStructure._peel_obj(refish))

        try:
            obj, reference = repo.resolve_refish(refish)
            if isinstance(reference, pygit2.Reference):
                reference = reference.name
            return (reference, *RepoStructure._peel_obj(obj))
        except KeyError:
            pass

        try:
            obj = repo.revparse_single(refish)
            return (None, *RepoStructure._peel_obj(obj))
        except KeyError:
            pass

        raise NotFound(f"{refish} is not a ref, commit or tree", exit_code=NO_COMMIT)

    @staticmethod
    def resolve_commit(repo, refish):
        """
        Given a string that describes a commit, return the parent of that commit -
        or, return the empty tree if that commit has no parent.
        """
        if refish is None or refish == "HEAD":
            return repo.head_commit

        try:
            obj, reference = repo.resolve_refish(refish)
            return obj.peel(pygit2.Commit)
        except (pygit2.InvalidSpecError, KeyError):
            pass

        try:
            obj = repo.revparse_single(refish)
            return obj.peel(pygit2.Commit)
        except (pygit2.InvalidSpecError, KeyError):
            pass

        raise NotFound(f"{refish} is not a commit", exit_code=NO_COMMIT)

    @staticmethod
    def _peel_obj(obj):
        commit, tree = None, None
        try:
            commit = obj.peel(pygit2.Commit)
        except pygit2.InvalidSpecError:
            pass
        try:
            tree = obj.peel(pygit2.Tree)
        except pygit2.InvalidSpecError:
            pass
        return commit, tree

    def __init__(
        self,
        repo,
        refish,
        force_dataset_class=None,
    ):
        self.L = logging.getLogger(self.__class__.__qualname__)
        self.repo = repo

        self.ref, self.commit, self.tree = RepoStructure.resolve_refish(repo, refish)
        self.datasets = Datasets(
            repo, self.tree, force_dataset_class=force_dataset_class
        )

    def __eq__(self, other):
        return other and (self.repo.path == other.repo.path) and (self.id == other.id)

    def __repr__(self):
        if self.ref == "[EMPTY]":
            at_desc = "@<empty>"
        elif self.ref is not None:
            at_desc = f"@{self.ref}={self.commit.id}"
        elif self.commit is not None:
            at_desc = f"@{self.commit.id}"
        elif self.tree is not None:
            at_desc = f"@tree:{self.tree.id}"
        else:
            at_desc = " <empty>"

        return f"RepoStructure<{self.repo.path}{at_desc}>"

    def decode_path(self, full_path):
        """
        Given a path in the Kart repository - eg "path/to/dataset/.table-dataset/feature/49/3e/Bg==" -
        returns a tuple in one of the following forms (depending on the dataset type):
        1. (dataset_path, "meta", meta_item_path)
        2. (dataset_path, "feature", primary_key)
        3. (dataset_path, "tile", tile_name)
        """
        match = DATASET_PATH_PATTERN.search(full_path)
        dataset_path = full_path[: match.start()]
        rel_path = full_path[match.start() + 1 :]
        return (dataset_path, *self.datasets[dataset_path].decode_path(rel_path))

    @property
    def ref_or_id(self):
        return self.ref or self.id

    @property
    def id(self):
        obj = self.commit or self.tree
        return obj.id if obj is not None else None

    @property
    def short_id(self):
        obj = self.commit or self.tree
        return obj.short_id if obj is not None else None

    def create_tree_from_diff(
        self,
        repo_diff,
        *,
        resolve_missing_values_from_rs: Optional["RepoStructure"] = None,
        object_builder=None,
    ):
        """
        Given a diff, returns a new tree created by applying the diff to self.tree -
        Doesn't create any commits or modify the working copy at all.

        If resolve_missing_values_from_rs is provided, we check each new-only delta
        (i.e. an insertion) by pulling an old value for the same feature from the given
        RepoStructure. If an old value is present, the delta is treated as an update rather
        than an insert, and we check if that update conflicts with any changes for the same
        feature in the current RepoStructure.

        This supports patches generated with `kart create-patch --patch-type=minimal`,
        which can be (significantly) smaller.

        object_builder - if supplied, this ObjectBuilder will be used instead of the default.
        """
        if object_builder is None:
            with packfile_object_builder(self.repo, self.tree) as object_builder:
                return self.create_tree_from_diff(
                    repo_diff,
                    resolve_missing_values_from_rs=resolve_missing_values_from_rs,
                    object_builder=object_builder,
                )

        if not self.tree:
            # This is the first commit to this branch - we may need to add extra blobs
            # to the tree to mark this data as being of a particular version.
            extra_blobs = extra_blobs_for_version(self.version)
            for path, blob in extra_blobs:
                object_builder.insert(path, blob)

        for ds_path, ds_diff in repo_diff.items():
            schema_delta = ds_diff.recursive_get(["meta", "schema.json"])
            if schema_delta and self.repo.table_dataset_version < 2:
                # This should have been handled already, but just to be safe.
                raise NotYetImplemented(
                    "Meta changes are not supported until table datasets V2"
                )

            if schema_delta and schema_delta.type == "delete":
                object_builder.remove(ds_path)
                continue

            if schema_delta and schema_delta.type == "insert":
                schema = Schema.from_column_dicts(schema_delta.new_value)
                dataset = dataset_class_for_version(
                    self.repo.table_dataset_version
                ).new_dataset_for_writing(ds_path, schema)
            else:
                dataset = self.datasets[ds_path]

            resolve_missing_values_from_ds = None
            if resolve_missing_values_from_rs is not None:
                try:
                    resolve_missing_values_from_ds = (
                        resolve_missing_values_from_rs.datasets[ds_path]
                    )
                except KeyError:
                    pass

            dataset.apply_diff(
                ds_diff,
                object_builder,
                resolve_missing_values_from_ds=resolve_missing_values_from_ds,
            )
            object_builder.flush()

        tree = object_builder.flush()
        L.info(f"Tree sha: {tree.hex}")
        return tree

    def check_values_match_schema(self, repo_diff):
        all_features_valid = True
        violations = {}

        for ds_path, ds_diff in repo_diff.items():
            ds_violations = {}
            violations[ds_path] = ds_violations

            schema_delta = ds_diff.recursive_get(["meta", "schema.json"])
            if schema_delta:
                if self.repo.table_dataset_version < 2:
                    # This should have been handled already, but just to be safe.
                    raise NotYetImplemented(
                        "Meta changes are not supported until datasets V2"
                    )
                elif schema_delta.type == "delete":
                    new_schema = None
                else:
                    new_schema = Schema.from_column_dicts(schema_delta.new_value)
            else:
                new_schema = self.datasets[ds_path].schema

            feature_diff = ds_diff.get("feature") or {}
            for feature_delta in feature_diff.values():
                new_value = feature_delta.new_value
                if new_value is None:
                    continue
                if new_schema is None:
                    raise InvalidOperation(
                        f"Can't {feature_delta.type} feature {feature_delta.new_key} in deleted dataset {ds_path}",
                        exit_code=PATCH_DOES_NOT_APPLY,
                    )
                all_features_valid &= new_schema.validate_feature(
                    new_value, ds_violations
                )

        if not all_features_valid:
            for ds_path, ds_violations in violations.items():
                for message in ds_violations.values():
                    click.echo(f"{ds_path}: {message}", err=True)
            raise InvalidOperation(
                "Schema violation - values do not match schema",
                exit_code=SCHEMA_VIOLATION,
            )

    def commit_diff(
        self,
        wcdiff,
        message,
        *,
        author=None,
        committer=None,
        allow_empty=False,
        resolve_missing_values_from_rs: Optional["RepoStructure"] = None,
    ):
        """
        Update the repository structure and write the updated data to the tree
        as a new commit, setting HEAD to the new commit.
        NOTE: Doesn't update working-copy meta or tracking tables, this is the
        responsibility of the caller.

        `self.ref` must be a key that works with repo.references, i.e.
        either "HEAD" or "refs/heads/{branchname}"
        """
        if not self.ref:
            raise RuntimeError("Can't commit diff - no reference to add commit to")

        self.check_values_match_schema(wcdiff)

        with packfile_object_builder(self.repo, self.tree) as object_builder:
            new_tree = self.create_tree_from_diff(
                wcdiff,
                resolve_missing_values_from_rs=resolve_missing_values_from_rs,
                object_builder=object_builder,
            )
            if (not allow_empty) and new_tree == self.tree:
                raise NotFound("No changes to commit", exit_code=NO_CHANGES)

            L.info("Committing...")

            if self.ref == "HEAD":
                parent_commit = self.repo.head_commit
            else:
                parent_commit = self.repo.references[self.ref].peel(pygit2.Commit)
            parents = [parent_commit.oid] if parent_commit is not None else []

            # This will also update the ref (branch) to point to the new commit
            new_commit = object_builder.commit(
                self.ref,
                author or self.repo.author_signature(),
                committer or self.repo.committer_signature(),
                message,
                parents,
            )

        L.info(f"Commit: {new_commit.id.hex}")
        return new_commit


class Datasets:
    """
    The collection of datasets found in a particular tree. Can be used as an iterator, or by subscripting:

    >>> [ds.path for ds in structure.datasets]
    or
    >>> structure.datasets[path_to_dataset]
    or
    >>> structure.datasets.get(path_to_dataset)
    """

    def __init__(self, repo, tree, force_dataset_class=None):
        self.repo = repo
        self.tree = tree
        self.force_dataset_class = force_dataset_class

    def __getitem__(self, ds_path):
        """Get a specific dataset by path."""
        result = self.get(ds_path)
        if not result:
            raise KeyError(f"No dataset found at '{ds_path}'")
        return result

    def is_dataset_dirname(self, dirname):
        return DATASET_DIRNAME_PATTERN.fullmatch(dirname)

    def get_dataset_class_for_dirname(self, dirname):

        if dirname in (".table-dataset", ".sno-dataset"):
            return dataset_class_for_version(self.repo.table_dataset_version)
        if dirname == ".point-cloud-dataset.v1":
            from kart.point_cloud.dataset1 import PointCloudV1

            return PointCloudV1

        return UnsupportedDataset

    def get(self, ds_path):
        """Get a specific dataset by path, or return None."""
        if not self.tree:
            return None
        try:
            ds_tree = self.tree / ds_path
        except KeyError:
            return None
        if ds_tree.type_str != "tree":
            return None

        return self._get_for_tree(ds_tree, ds_path)

    def _get_for_tree(self, ds_tree, ds_path):
        """
        Try to load a dataset that has the given outer_tree and outer_path.
        For instance, this succeeds when given the tree at a/b/c, if there is a child tree a/b/c/.table-dataset/ -
        It will return a dataset with path "a/b/c".
        """
        if self.force_dataset_class is not None:
            if self.force_dataset_class.is_dataset_tree(ds_tree):
                return self.force_dataset_class(
                    ds_tree,
                    ds_path,
                    self.force_dataset_class.DATASET_DIRNAME,
                    repo=self.repo,
                )
        else:
            for child_tree in ds_tree:
                dirname = child_tree.name
                if self.is_dataset_dirname(dirname):
                    dataset_class = self.get_dataset_class_for_dirname(dirname)
                    return dataset_class(ds_tree, ds_path, dirname, repo=self.repo)
        return None

    def __len__(self):
        return sum(1 for _ in self)

    def __iter__(self):
        """Iterate over all available datasets in self.tree."""
        if self.tree is None:
            return

        to_examine = deque([(self.tree, "")])

        while to_examine:
            tree, path = to_examine.popleft()

            for child in tree:
                # Ignore everything other than directories
                if child.type_str != "tree":
                    continue
                # Ignore "hidden" directories.
                if child.name.startswith("."):
                    continue

                if path:
                    child_path = "/".join([path, child.name])
                else:
                    child_path = child.name

                # Examine inside this directory
                to_examine.append((child, child_path))

                ds = self._get_for_tree(child, child_path)
                if ds is not None:
                    yield ds
