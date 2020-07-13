import click
import pygit2
import json

STRUCTURE_VERSION_PATH = ".sno-format"

STRUCTURE_VERSIONS = (0, 1, 2)
# Only versions 1 and 2 are currently supported by any commands. If you have version 0, use sno upgrade 00-02
STRUCTURE_VERSIONS_CHOICE = click.Choice(["1", "2"])

DEFAULT_STRUCTURE_VERSION = 1


def encode_structure_version(version):
    return STRUCTURE_VERSION_PATH, f"{version}\n".encode('utf8')


def extra_blobs_for_version(version):
    """Returns the extra blobs that should be written to a repository for the given version."""
    version = int(version)
    if version <= 1:
        # Version 1 never had a repo-wide version blob. We'll leave it that way, no need to change it.
        return []

    # Versions 2 and up have their version number stored in STRUCTURE_VERSION_PATH.
    return [encode_structure_version(version)]


def get_structure_version(repo, tree=None):
    """
    Returns the repo version from the blob at <repo-root>/.sno-format -
    not a file in the BARE repository itself, but in the git tree.
    """
    if tree is None:
        if repo.is_empty:
            return DEFAULT_STRUCTURE_VERSION
        try:
            tree = repo.head.peel(pygit2.Tree)
        except pygit2.GitError:
            return DEFAULT_STRUCTURE_VERSION  # Empty branch.

    if STRUCTURE_VERSION_PATH not in tree:
        # Versions less than 2 don't have ".sno-version" files.
        # TODO: distinguish between 1 and 0
        return 1

    return json.loads((tree / STRUCTURE_VERSION_PATH).data)
