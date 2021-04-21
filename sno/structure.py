import logging
from collections import deque

import click
import pygit2

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
from .repo_version import extra_blobs_for_version
from .schema import Schema


L = logging.getLogger("sno.structure")


class RepoStructure:
    """
    The internal structure of a Kart repository, at a particular revision.
    The Kart revision's structure is almost entirely comprised of its datasets, but this may change.
    The datasets can be accessed at self.datasets, but there is also a shortcut that skips this class - instead of:

    >>> sno_repo.structure(commit_hash).datasets

    You can use:

    >>> sno_repo.datasets(commit_hash)
    """

    @staticmethod
    def resolve_refish(repo, refish):
        """
        Given a ref / refish / commit / tree / OID, returns as many as possible of the following:
        >>> (ref, commit, tree)
        """
        if refish is None or refish == "HEAD":
            return "HEAD", repo.head_commit, repo.head_tree

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
        dataset_class,
    ):
        self.L = logging.getLogger(self.__class__.__qualname__)
        self.repo = repo

        self.ref, self.commit, self.tree = RepoStructure.resolve_refish(repo, refish)

        self.dataset_class = dataset_class
        self.version = dataset_class.VERSION
        self.datasets = Datasets(self.tree, self.dataset_class)

    def __eq__(self, other):
        return other and (self.repo.path == other.repo.path) and (self.id == other.id)

    def __repr__(self):
        name = f"RepoStructure"
        if self.ref is not None:
            return f"{name}<{self.repo.path}@{self.ref}={self.commit.id}>"
        elif self.commit is not None:
            return f"{name}<{self.repo.path}@{self.commit.id}>"
        elif self.tree is not None:
            return f"{name}<{self.repo.path}@tree={self.tree.id}>"
        else:
            return f"{name}<{self.repo.path} <empty>>"

    def decode_path(self, full_path):
        """
        Given a path in the Kart repository - eg "path/to/dataset/.sno-dataset/49/3e/Bg==" -
        returns a tuple in either of the following forms:
        1. (dataset_path, "feature", primary_key)
        2. (dataset_path, "meta", meta_item_path)
        """
        dataset_dirname = self.dataset_class.DATASET_DIRNAME
        dataset_path, rel_path = full_path.split(f"/{dataset_dirname}/", 1)
        rel_path = f"{dataset_dirname}/{rel_path}"
        return (dataset_path, *self.datasets[dataset_path].decode_path(rel_path))

    @property
    def id(self):
        obj = self.commit or self.tree
        return obj.id if obj is not None else None

    @property
    def short_id(self):
        obj = self.commit or self.tree
        return obj.short_id if obj is not None else None

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
                dataset = self.dataset_class(tree=None, path=ds_path)
            else:
                dataset = self.datasets[ds_path]

            dataset.apply_diff(
                ds_diff, tree_builder, allow_missing_old_values=allow_missing_old_values
            )
            tree_builder.flush()

        tree = tree_builder.flush()
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
        allow_missing_old_values=False,
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

        new_tree = self.create_tree_from_diff(
            wcdiff,
            allow_missing_old_values=allow_missing_old_values,
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
        new_commit_id = self.repo.create_commit(
            self.ref,
            author or self.repo.author_signature(),
            committer or self.repo.committer_signature(),
            message,
            new_tree.id,
            parents,
        )
        new_commit = self.repo[new_commit_id]

        L.info(f"Commit: {new_commit.hex}")
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

    def __init__(self, tree, dataset_class):
        self.tree = tree
        self.dataset_class = dataset_class

    def __getitem__(self, ds_path):
        """Get a specific dataset by path."""
        try:
            ds_tree = self.tree / ds_path if self.tree is not None else None
        except KeyError:
            ds_tree = None

        if self.dataset_class.is_dataset_tree(ds_tree):
            return self.dataset_class(ds_tree, ds_path)

        raise KeyError(f"No valid dataset found at '{ds_path}'")

    def get(self, ds_path):
        try:
            return self.__getitem__(ds_path)
        except KeyError:
            return None

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
