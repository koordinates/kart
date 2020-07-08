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


def get_structure_version(repo):
    """
    Returns the repo version from the blob at <repo-root>/.sno-format -
    not a file in the BARE repository itself, but in the git tree.
    """
    if repo.is_empty:
        return None
    try:
        tree = repo.head.peel(pygit2.Tree)
    except pygit2.GitError:
        return None  # Empty branch.

    if STRUCTURE_VERSION_PATH not in tree:
        # Versions less than 2 don't have ".sno-version" files.
        # TODO: distinguish between 1 and 0
        return 1

    return json.loads((tree / STRUCTURE_VERSION_PATH).data)
