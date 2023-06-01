from contextlib import contextmanager
from enum import IntEnum

from kart.exceptions import NotFound, SubprocessError
from kart import subprocess_util as subprocess


class LibgitSubcode(IntEnum):
    """Extra error subcodes from pygit2 (or at least the version of it built into Kart)."""

    # TODO - move into pygit2 constants.

    # No object with this path exists, since some ancestor object has no TreeEntry with this path.
    ENOSUCHPATH = -3001
    # A TreeEntry with this path exists, but for some reason, it points to an object not found in the object store.
    EOBJECTMISSING = -3002
    # A TreeEntry with this path exists, but it is not present locally.
    # It is marked as promised, and presumably can be fetched from the remote.
    EOBJECTPROMISED = -3003


def object_is_promised(object_error):
    """Given an error loading an object, returns True if it signals EOBJECTPROMISED."""
    return (
        isinstance(object_error, KeyError)
        and getattr(object_error, "subcode", 0) == LibgitSubcode.EOBJECTPROMISED
    )


def get_promisor_remote(repo):
    """Returns the name of the remote from which promised objects should be fetched."""
    head_remote_name = repo.head_remote_name
    all_remote_names = (r.name for r in repo.remotes)

    config = repo.config
    for name in sorted(all_remote_names, key=lambda name: name != head_remote_name):
        key = f"remote.{name}.promisor"
        if key in config and config.get_bool(key):
            return name
    raise NotFound(
        "Some objects are missing+promised, but no promisor remote is configured"
    )


def get_partial_clone_filter(repo):
    """Returns the value stored at remote.(promisor-remote).partialclonefilter, if any."""
    config = repo.config
    try:
        name = get_promisor_remote(repo)
    except NotFound:
        return None

    key = f"remote.{name}.partialclonefilter"
    if key not in config:
        return None
    return config[key]


def get_partial_clone_envelope(repo):
    """
    Parses the envelope from remote.(promisor-remote).partialclonefilter, which tells us
    the spatial filter envelope that was used during the clone operation.
    """
    pcf = get_partial_clone_filter(repo)
    if not pcf:
        return None
    prefix = "extension:spatial="
    if not pcf.startswith(prefix):
        return None
    spatial_str = pcf[len(prefix) :]
    parts = spatial_str.split(",", maxsplit=4)

    if len(parts) != 4:
        raise ValueError(
            f"Repository config contains invalid spatial filter: {spatial_str}"
        )

    try:
        envelope = [float(p) for p in parts]
    except ValueError:
        raise ValueError(
            f"Repository config contains invalid spatial filter: {spatial_str}"
        )

    return envelope


class FetchPromisedBlobsProcess:
    """
    Fetches requested blobs from the promisor remote in a git fetch process.
    Any number of blobs can be requested asynchronously by calling fetch,
    all the fetches will block until complete when finish is called.
    """

    def __init__(self, repo):
        self.repo = repo
        self.promisor_remote = get_promisor_remote(self.repo)
        self.cmd = [
            "git",
            "-c",
            "fetch.negotiationAlgorithm=noop",
            "fetch",
            self.promisor_remote,
            "--no-tags",
            "--no-write-fetch-head",
            "--recurse-submodules=no",
            "--stdin",
        ]
        # We have to use binary mode since git always expects '\n' line endings, even on windows.
        # This means we can't use line buffering, except that we know each line is 41 bytes long.
        self.proc = subprocess.Popen(
            self.cmd,
            cwd=self.repo.path,
            stdin=subprocess.PIPE,
            bufsize=41,  # Works as line buffering
        )

    def fetch(self, promised_blob_id):
        try:
            self.proc.stdin.write(f"{promised_blob_id}\n".encode())
        except (BrokenPipeError, OSError):
            # if git-fetch dies early, we get an EPIPE here
            # we'll deal with it below
            pass

    def finish(self):
        try:
            self.proc.stdin.close()
        except (BrokenPipeError, OSError):
            pass
        self.proc.wait()
        return_code = self.proc.returncode
        if return_code != 0:
            raise SubprocessError(
                f"git-fetch error! {return_code}", exit_code=return_code
            )


@contextmanager
def fetch_promised_blobs_process(repo):
    fetch_proc = FetchPromisedBlobsProcess(repo)
    yield fetch_proc
    fetch_proc.finish()


def fetch_promised_blobs(repo, promised_blob_ids):
    with fetch_promised_blobs_process(repo) as p:
        for promised_blob_id in promised_blob_ids:
            p.fetch(promised_blob_id)
