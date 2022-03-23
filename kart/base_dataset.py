import logging
import sys

import click

from kart.exceptions import InvalidOperation, UNSUPPORTED_VERSION


class BaseDataset:
    """
    Common interface for all datasets.

    A Dataset instance is immutable since it is a view of a particular git tree.
    To get a new version of a dataset, commit the desired changes,
    then instantiate a new Dataset instance that references the new git tree.

    A dataset has a user-defined path eg `path/to/dataset`, and inside that it
    has a hidden folder with a special name - cls.DATASET_DIRNAME.
    The path to this folder is the inner_path: `path/to/dataset/DATASET_DIRNAME`.
    Similarly, a dataset's tree is the tree at `path/to/dataset`,
    and its inner_tree is the tree at `path/to/dataset/DATASET_DIRNAME`.

    All relative paths are defined as being relative to the inner_path / inner_tree.
    """

    # Subclasses should override these fields to properly implement the dataset interface.

    DATASET_TYPE = None  # Example: "hologram"
    VERSION = None  # Example: 1
    DATASET_DIRNAME = None  # Example: ".hologram-dataset.v1".
    # (This should match the pattern DATASET_DIRNAME_PATTERN in kart.structure.)

    # Paths - these are generally relative to self.inner_tree, but datasets may choose to put extra data in the outer
    # tree also where it will eventually be user-visible (once attachments are fully supported).

    # Where meta-items are stored - blobs containing metadata about the structure or schema of the dataset.
    META_PATH = "meta/"

    # There are no other paths that are common to all types of dataset.

    @classmethod
    def is_dataset_tree(cls, tree):
        """Returns True if the given tree seems to contain a dataset of this type."""
        if tree is None:
            return False
        return (
            cls.DATASET_DIRNAME in tree
            and (tree / cls.DATASET_DIRNAME).type_str == "tree"
        )

    def __init__(self, tree, path, repo, dirname=None):
        """
        Initialise a dataset which has the given path and the contents from the given tree.

        tree - pygit2.Tree or similar, if supplied it must contains a subtree of name dirname.
               If set to None, this dataset will be completely empty, but this could still be useful as a placeholder or
               as a starting point from which to write a new dataset.
        path - a string eg "path/to/dataset". Should be the path to the given tree, if a tree is provided.
        repo - the repo in which this dataset is found, or is to be created. Since a dataset is a view of a particular
               tree, the repo's functionality is not generally needed, but this is used for obtaining repo.empty_tree.
        dirname - the name of the subtree in which the dataset data is kept eg ".hologram-dataset.v1".
                  If this is None, it defaults to the DATASET_DIRNAME from the class.
                  If this is also None, then inner_tree is set to the same as tree - this is not the normal structure of
                  a dataset, but is supported for legacy reasons.
        """
        assert path is not None
        assert repo is not None
        if dirname is None:
            dirname = self.DATASET_DIRNAME
        path = path.strip("/")

        self.L = logging.getLogger(self.__class__.__qualname__)

        self.tree = tree
        self.path = path
        self.dirname = dirname
        self.repo = repo

        self.inner_path = f"{path}/{dirname}" if dirname else path
        if self.tree is not None:
            self.inner_tree = self.tree / dirname if dirname else self.tree
        else:
            self.inner_tree = None

        self._empty_tree = repo.empty_tree

        self.ensure_only_supported_capabilities()

    def ensure_only_supported_capabilities(self):
        # TODO - loosen this restriction. A dataset with capabilities that we don't support should (at worst) be treated
        # the same as any other unsupported dataset.
        capabilities = self.get_meta_item("capabilities.json", missing_ok=True)
        if capabilities is not None:
            from .cli import get_version
            from .output_util import dump_json_output

            click.echo(
                f"The dataset at {self.path} requires the following capabilities which Kart {get_version()} does not support:",
                err=True,
            )
            dump_json_output(capabilities, sys.stderr)
            raise InvalidOperation(
                "Download the latest Kart to work with this dataset",
                exit_code=UNSUPPORTED_VERSION,
            )
