import itertools
import json

from kart.core import walk_tree
from kart.exceptions import (
    UNSUPPORTED_VERSION,
    NO_REPOSITORY,
    InvalidOperation,
    NotFound,
)

# Kart repos have a repo-wide marker - either in the .kart/config file or in a blob in the ODB -
# that stores which version all of the table-datasets are, if they are V2 or V3.
# This type of repo-wide marker wasn't present for V0 or V1 table-datasets, and it won't be extended to work for
# other dataset types going forward.

# It has the following advantage:
# - it marks an entire repo as being compatible or incompatible with a particular Kart version, so that Kart can simply
#   announce "you need to download newer Kart" or "you need to upgrade this repo to a supported version".

# But, it has the following disadvantage:
# - slightly ugly: extra blobs unrelated to the commit content is added to the first commit
# - wasn't designed to store versions for multiple different dataset types eg table-dataset.v3, point-cloud-dataset.v1.
# - was specifically designed to prevent a mixture of dataset versions in one repo eg table-dataset.v1 and .v2
#   Going forward, it probably makes more sense to allow this, rather than always requiring the user to upgrade old
#   datasets (which breaks synchronisation with remotes).

# We look for the repostructure version blob in either of these two places:
TABLE_DATASET_VERSION_BLOB_PATHS = {
    2: ".sno.repository.version",  # Default path for V2
    3: ".kart.repostructure.version",  # Default path for V3
}

# Currently table-Datasets v2 and v3 are supported by all commands.
MIN_SUPPORTED_VERSION = 2
MAX_SUPPORTED_VERSION = 3
SUPPORTED_VERSIONS = range(MIN_SUPPORTED_VERSION, MAX_SUPPORTED_VERSION + 1)
SUPPORTED_VERSION_DESC = "v2 or v3"

DEFAULT_NEW_REPO_VERSION = 3


# Table-datasets v0 and v1 are also recognized. Table-datasets v0, v1 and v2 and can be upgradeed to v3.
MIN_RECOGNIZED_VERSION = 0
MAX_RECOGNIZED_VERSION = 3


def encode_repo_version(version):
    return TABLE_DATASET_VERSION_BLOB_PATHS[version], f"{version}\n".encode("utf8")


def extra_blobs_for_version(version):
    """Returns the extra blobs that should be written to a repository for the given version."""
    version = int(version)
    if version <= 1:
        # Version 1 never had a repo-wide version blob. We'll leave it that way, no need to change it.
        return []

    # Versions 2 and up have their version number stored at REPOSTRUCTURE_VERSION_BLOB_PATH
    return [encode_repo_version(version)]


def get_repo_wide_version(repo, tree=None):
    """
    Returns the repo version from the blob at <repo-root>/REPOSTRUCTURE_VERSION_BLOB_PATH -
    (note that this is not user-visible in the file-system since we keep it hidden via sparse / bare checkouts).
    """
    if tree is None:
        tree = repo.head_tree
        if tree is None:  # Empty repo / empty branch.
            return _get_repo_wide_version_from_config(repo)

    for r in TABLE_DATASET_VERSION_BLOB_PATHS.values():
        if r in tree:
            try:
                return json.loads((tree / r).data)
            except KeyError:
                # Must be some kind of filtered clone. Try our best not to crash:
                return _get_repo_wide_version_from_config(repo)

    # Versions less than 2 don't have a REPOSTRUCTURE_VERSION_BLOB, so must be 0 or 1.
    # We don't support these versions except when performing a `kart upgrade`.
    return _distinguish_v0_v1(tree)


def dataset_class_for_version(version):
    """
    Returns the Dataset class that implements a particular repository version.
    """
    assert MIN_SUPPORTED_VERSION <= version <= MAX_SUPPORTED_VERSION
    if version == 2:
        from kart.tabular.v2 import TableV2

        return TableV2

    if version == 3:
        from kart.tabular.v3 import TableV3

        return TableV3


def ensure_supported_repo_wide_version(version):
    from kart.cli import get_version

    if not MIN_SUPPORTED_VERSION <= version <= MAX_SUPPORTED_VERSION:
        message = (
            f"This Kart repo uses Datasets v{version}, "
            f"but Kart {get_version()} only supports Datasets {SUPPORTED_VERSION_DESC}.\n"
        )
        if version < MIN_SUPPORTED_VERSION and version >= MIN_RECOGNIZED_VERSION:
            message += "Use `kart upgrade SOURCE DEST` to upgrade this repo to the supported version."
        else:
            message += "Get the latest version of Kart to work with this repo."
        raise InvalidOperation(message, exit_code=UNSUPPORTED_VERSION)


def _get_repo_wide_version_from_config(repo):
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
    # No evidence that this is a Kart repo, but, it's possible if you mess with Kart repo internals that you could
    # get here by corrupting your HEAD commit - so the message provide a tiny bit of context to help diagnose:
    raise NotFound(
        "Current directory is not a Kart repository (no Kart datasets found at HEAD commit)",
        exit_code=NO_REPOSITORY,
    )
