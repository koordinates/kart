import logging
from collections import deque

import pygit2

from . import git_util
from .base_dataset import BaseDataset
from .exceptions import (
    NotFound,
    NotYetImplemented,
    NO_CHANGES,
    NO_COMMIT,
)
from .rich_tree_builder import RichTreeBuilder
from .repository_version import get_repo_version


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
        return get_repo_version(self.repo, self.tree, maybe_v0=False)

    @property
    def dataset_dirname(self):
        return BaseDataset.dataset_dirname(self.version)

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
        if self.tree is None:
            return None
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
            ds = BaseDataset.instantiate(o, path, self.version)
            return ds

        raise KeyError(f"No valid dataset found at '{path}'")

    def __iter__(self):
        """ Iterate over available datasets in this repository """
        return self.iter_at(self.tree)

    def iter_at(self, tree):
        """ Iterate over available datasets in this repository using a specified Tree """
        if tree is None:
            return

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
                        ds = BaseDataset.instantiate(o, te_path, dataset_version)
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

    def create_tree_from_diff(self, repo_diff):
        """
        Given a diff, returns a new tree created by applying the diff to self.tree -
        Doesn't create any commits or modify the working copy at all.
        """
        tree_builder = RichTreeBuilder(self.repo, self.tree)
        dataset_class = BaseDataset.for_version(self.version)

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

            dataset.apply_diff(ds_diff, tree_builder)
            tree_builder.flush()

        tree = tree_builder.flush()
        L.info(f"Tree sha: {tree.oid}")
        return tree.oid

    def commit(
        self, wcdiff, message, *, author=None, committer=None, allow_empty=False,
    ):
        """
        Update the repository structure and write the updated data to the tree
        as a new commit, setting HEAD to the new commit.
        NOTE: Doesn't update working-copy meta or tracking tables, this is the
        responsibility of the caller.
        """
        old_tree_oid = self.tree.oid if self.tree is not None else None
        new_tree_oid = self.create_tree_from_diff(wcdiff)
        if (not allow_empty) and new_tree_oid == old_tree_oid:
            raise NotFound("No changes to commit", exit_code=NO_CHANGES)

        L.info("Committing...")

        parent_commit = git_util.get_head_commit(self.repo)
        parents = [parent_commit.oid] if parent_commit is not None else []

        # this will also update the ref (branch) to point to the current commit
        new_commit = self.repo.create_commit(
            "HEAD",  # reference_name
            author or git_util.author_signature(self.repo),
            committer or git_util.committer_signature(self.repo),
            message,
            new_tree_oid,
            parents,
        )
        L.info(f"Commit: {new_commit}")

        # TODO: update reflog
        return new_commit
