import itertools

import json

from .core import walk_tree

from .exceptions import (
    InvalidOperation,
    UNSUPPORTED_VERSION,
)

# We look for the repostructure version blob in either of these two places:
REPOSTRUCTURE_VERSION_BLOB_PATHS = {
    2: ".sno.repository.version",  # Default path for V2
    3: ".kart.repostructure.version",  # Default path for V3
}

# Currently Datasets v2 and v3 are supported by all commands.
MIN_SUPPORTED_REPO_VERSION = 2
MAX_SUPPORTED_REPO_VERSION = 3
SUPPORTED_REPO_VERSIONS = range(
    MIN_SUPPORTED_REPO_VERSION, MAX_SUPPORTED_REPO_VERSION + 1
)
SUPPORTED_REPO_VERSION_DESC = "v2 or v3"

DEFAULT_NEW_REPO_VERSION = 3


# Datasets v0 and v1 are also recognized. Datasets v0, v1 and v2 and can be upgradeed to v3.
MIN_RECOGNIZED_REPO_VERSION = 0
MAX_RECOGNIZED_REPO_VERSION = 3


def encode_repo_version(version):
    return REPOSTRUCTURE_VERSION_BLOB_PATHS[version], f"{version}\n".encode("utf8")


def extra_blobs_for_version(version):
    """Returns the extra blobs that should be written to a repository for the given version."""
    version = int(version)
    if version <= 1:
        # Version 1 never had a repo-wide version blob. We'll leave it that way, no need to change it.
        return []

    # Versions 2 and up have their version number stored at REPOSTRUCTURE_VERSION_BLOB_PATH
    return [encode_repo_version(version)]


def get_repo_version(repo, tree=None):
    """
    Returns the repo version from the blob at <repo-root>/REPOSTRUCTURE_VERSION_BLOB_PATH -
    (note that this is not user-visible in the file-system since we keep it hidden via sparse / bare checkouts).
    """
    if tree is None:
        tree = repo.head_tree
        if tree is None:  # Empty repo / empty branch.
            return _get_repo_version_from_config(repo)

    for r in REPOSTRUCTURE_VERSION_BLOB_PATHS.values():
        if r in tree:
            return json.loads((tree / r).data)

    # Versions less than 2 don't have a REPOSTRUCTURE_VERSION_BLOB, so must be 0 or 1.
    # We don't support these versions except when performing a `kart upgrade`.
    return _distinguish_v0_v1(tree)


def dataset_class_for_version(version):
    """
    Returns the Dataset class that implements a particular repository version.
    """
    assert MIN_SUPPORTED_REPO_VERSION <= version <= MAX_SUPPORTED_REPO_VERSION
    if version == 2:
        from kart.dataset2 import Dataset2

        return Dataset2

    if version == 3:
        from kart.dataset3 import Dataset3

        return Dataset3


def ensure_supported_repo_version(version):
    from .cli import get_version

    if not MIN_SUPPORTED_REPO_VERSION <= version <= MAX_SUPPORTED_REPO_VERSION:
        message = (
            f"This Kart repo uses Datasets v{version}, "
            f"but Kart {get_version()} only supports Datasets {SUPPORTED_REPO_VERSION_DESC}.\n"
        )
        if (
            version < MIN_SUPPORTED_REPO_VERSION
            and version >= MIN_RECOGNIZED_REPO_VERSION
        ):
            message += "Use `kart upgrade SOURCE DEST` to upgrade this repo to the supported version."
        else:
            message += "Get the latest version of Kart to work with this repo."
        raise InvalidOperation(message, exit_code=UNSUPPORTED_VERSION)


def _get_repo_version_from_config(repo):
    from kart.repo import KartConfigKeys

    repo_cfg = repo.config
    if KartConfigKeys.KART_REPOSTRUCTURE_VERSION in repo_cfg:
        return repo_cfg.get_int(KartConfigKeys.KART_REPOSTRUCTURE_VERSION)
    elif KartConfigKeys.SNO_REPOSITORY_VERSION in repo_cfg:
        return repo_cfg.get_int(KartConfigKeys.SNO_REPOSITORY_VERSION)
    else:
        return DEFAULT_NEW_REPO_VERSION


def _distinguish_v0_v1(tree):
    WALK_LIMIT = 100
    for top_tree, top_path, subtree_names, blob_names in itertools.islice(
        walk_tree(tree), 0, WALK_LIMIT
    ):
        dir_name = top_tree.name
        if dir_name == "meta" or dir_name == "features":
            # "meta" exists in v1 too, but only inside ".sno-table" - so report the one we get to first.
            return 0
        elif dir_name == ".sno-table":
            return 1
    # Maybe this isn't even a Kart repo?
    return 1
