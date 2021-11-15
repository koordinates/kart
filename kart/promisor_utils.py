from contextlib import contextmanager
from enum import IntEnum
import subprocess

from .cli_util import tool_environment
from .exceptions import NotFound, SubprocessError


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
    return (
        isinstance(object_error, KeyError)
        and getattr(object_error, 'subcode', 0) == LibgitSubcode.EOBJECTPROMISED
    )


def get_promisor_remote(repo):
    config = repo.config
    for r in repo.remotes:
        key = f"remote.{r.name}.promisor"
        if key in config and config.get_bool(key):
            return r.url
    raise NotFound(
        "Some objects are missing+promised, but no promisor remote is configured"
    )


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
            "--filter=blob:none",
            "--stdin",
        ]
        self.proc = subprocess.Popen(
            self.cmd,
            cwd=self.repo.path,
            stdin=subprocess.PIPE,
            env=tool_environment(),
            text=True,
            bufsize=1,  # Line buffering
        )

    def fetch(self, promised_blob_id):
        try:
            self.proc.stdin.write(f"{promised_blob_id}\n")
        except BrokenPipeError:
            # if git-fetch dies early, we get an EPIPE here
            # we'll deal with it below
            pass

    def finish(self):
        try:
            self.proc.stdin.close()
        except BrokenPipeError:
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
