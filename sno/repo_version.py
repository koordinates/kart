import pygit2

REPO_VERSION_PATH = ".sno-version"

# Earliest and latest version in compatible ranges:
COMPATIBLE_VERSIONS = [
    ("0.0", "0.1"),
    ("0.2", "0.4"),
    ("0.5", None),
]


def get_repo_version(repo):
    """
    Returns the repo version from the blob at <repo-root>/.sno-version -
    not a file in the BARE repository itself, but in the git tree.
    """
    if repo.is_empty:
        return None
    try:
        tree = repo.head.peel(pygit2.Tree)
    except pygit2.GitError:
        return None  # Empty branch.

    if REPO_VERSION_PATH not in tree:
        # Versions less than 0.5 don't have ".sno-version" files.
        # TODO - distinguish between at least 0.0-0.1 and 0.2-0.4
        return "0.2"

    return (tree / REPO_VERSION_PATH).data.decode('utf8').strip()
