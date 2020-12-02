import logging
from collections import deque

import click
import pygit2

from .base_dataset import BaseDataset
from .exceptions import (
    InvalidOperation,
    NotFound,
    NotYetImplemented,
    NO_CHANGES,
    NO_COMMIT,
    PATCH_DOES_NOT_APPLY,
    SCHEMA_VIOLATION,
)
from .rich_tree_builder import RichTreeBuilder
from .repository_version import get_repo_version, extra_blobs_for_version
from .schema import Schema


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

    def __init__(self, repo, commit=None, tree=None, version=None, dataset_class=None):
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
            self._commit = self.repo.head_commit

        if version is not None:
            self._version = version
        else:
            self._version = self.repo.version

        if dataset_class is not None:
            self._dataset_class = dataset_class
        else:
            self._dataset_class = BaseDataset.for_version(self._version)

    def __getitem__(self, path):
        """ Get a specific dataset by path """
        if self.tree is None:
            raise KeyError(path)
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
        return self._version

    @property
    def dataset_class(self):
        """Returns the dataset implementation to use for this entire repo."""
        return self._dataset_class

    def decode_path(self, full_path):
        """
        Given a path in the sno repository - eg "path/to/dataset/.sno-dataset/49/3e/Bg==" -
        returns a tuple in either of the following forms:
        1. (dataset_path, "feature", primary_key)
        2. (dataset_path, "meta", meta_item_path)
        """
        dataset_dirname = self.dataset_class.DATASET_DIRNAME
        dataset_path, rel_path = full_path.split(f"/{dataset_dirname}/", 1)
        rel_path = f"{dataset_dirname}/{rel_path}"
        return (dataset_path,) + self.get(dataset_path).decode_path(rel_path)

    def get(self, path):
        if self.tree is None:
            return None
        try:
            return self.get_at(path, self.tree)
        except KeyError:
            return None

    def get_at(self, path, tree):
        """ Get a specific dataset by path using a specified Tree """
        try:
            tree = tree / path
            if self.dataset_class.is_dataset_tree(tree):
                return self.dataset_class(tree, path)
        except KeyError:
            pass

        raise KeyError(f"No valid dataset found at '{path}'")

    def __iter__(self):
        """ Iterate over available datasets in this repository """
        return self.iter_at(self.tree)

    def iter_at(self, tree):
        """ Iterate over available datasets in this repository using a specified Tree """
        if tree is None:
            return

        to_examine = deque([(tree, "")])

        while to_examine:
            tree, path = to_examine.popleft()

            for child in tree:
                # Ignore everything other than directories
                if child.type_str != "tree":
                    continue

                if path:
                    child_path = "/".join([path, child.name])
                else:
                    child_path = child.name

                if self.dataset_class.is_dataset_tree(child):
                    ds = self.dataset_class(child, child_path)
                    yield ds
                else:
                    # Examine inside this directory
                    to_examine.append((child, child_path))

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

    def create_tree_from_diff(self, repo_diff, *, allow_missing_old_values=False):
        """
        Given a diff, returns a new tree created by applying the diff to self.tree -
        Doesn't create any commits or modify the working copy at all.

        If allow_missing_old_values=True, deltas are not checked for conflicts
        if they have no old_value. This allows for patches to be generated without
        reference to the old values, which can be (significantly) more efficient.
        However, it can also be more prone to data loss if the patch isn't generated
        from the same base revision.
        """
        tree_builder = RichTreeBuilder(self.repo, self.tree)
        dataset_class = BaseDataset.for_version(self.version)

        if not self.tree:
            # This is the first commit to this branch - we may need to add extra blobs
            # to the tree to mark this data as being of a particular version.
            extra_blobs = extra_blobs_for_version(self.version)
            for path, blob in extra_blobs:
                tree_builder.insert(path, blob)

        for ds_path, ds_diff in repo_diff.items():
            schema_delta = ds_diff.recursive_get(["meta", "schema.json"])
            if schema_delta and self.version < 2:
                # This should have been handled already, but just to be safe.
                raise NotYetImplemented(
                    "Meta changes are not supported until datasets V2"
                )

            if schema_delta and schema_delta.type == "delete":
                tree_builder.remove(ds_path)
                continue

            if schema_delta and schema_delta.type == "insert":
                dataset = dataset_class(tree=None, path=ds_path)
            else:
                dataset = self[ds_path]

            dataset.apply_diff(
                ds_diff, tree_builder, allow_missing_old_values=allow_missing_old_values
            )
            tree_builder.flush()

        tree = tree_builder.flush()
        L.info(f"Tree sha: {tree.oid}")
        return tree.oid

    def check_values_match_schema(self, repo_diff):
        all_features_valid = True
        violations = {}

        for ds_path, ds_diff in repo_diff.items():
            ds_violations = {}
            violations[ds_path] = ds_violations

            schema_delta = ds_diff.recursive_get(["meta", "schema.json"])
            if schema_delta:
                if self.version < 2:
                    # This should have been handled already, but just to be safe.
                    raise NotYetImplemented(
                        "Meta changes are not supported until datasets V2"
                    )
                elif schema_delta.type == "delete":
                    new_schema = None
                else:
                    new_schema = Schema.from_column_dicts(schema_delta.new_value)
            else:
                new_schema = self.get(ds_path).schema

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

    def commit(
        self,
        wcdiff,
        message,
        *,
        author=None,
        committer=None,
        allow_empty=False,
        allow_missing_old_values=False,
        ref="HEAD",
    ):
        """
        Update the repository structure and write the updated data to the tree
        as a new commit, setting HEAD to the new commit.
        NOTE: Doesn't update working-copy meta or tracking tables, this is the
        responsibility of the caller.

        `ref` should be a key that works with repo.references, i.e.
        either "HEAD" or "refs/heads/{branchname}"
        """
        self.check_values_match_schema(wcdiff)

        old_tree_oid = self.tree.oid if self.tree is not None else None
        new_tree_oid = self.create_tree_from_diff(
            wcdiff,
            allow_missing_old_values=allow_missing_old_values,
        )
        if (not allow_empty) and new_tree_oid == old_tree_oid:
            raise NotFound("No changes to commit", exit_code=NO_CHANGES)

        L.info("Committing...")

        if ref == "HEAD":
            parent_commit = self.repo.head_commit
        else:
            parent_commit = self.repo.references[ref].peel(pygit2.Commit)
        parents = [parent_commit.oid] if parent_commit is not None else []

        # this will also update the ref (branch) to point to the new commit
        new_commit = self.repo.create_commit(
            ref,
            author or self.repo.author_signature(),
            committer or self.repo.committer_signature(),
            message,
            new_tree_oid,
            parents,
        )
        L.info(f"Commit: {new_commit}")

        return new_commit
