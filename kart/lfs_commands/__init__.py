import functools
import itertools
import os
import pygit2
import re
import sys
import tempfile

import click

from kart.cli_util import KartGroup, add_help_subcommand
from kart.exceptions import SubprocessError, InvalidOperation
from kart.lfs_util import (
    pointer_file_bytes_to_dict,
    get_hash_from_pointer_file,
    get_local_path_from_lfs_oid,
)
from kart.lfs_commands.url_redirector import UrlRedirector
from kart.object_builder import ObjectBuilder
from kart.rev_list_objects import rev_list_tile_pointer_files
from kart.repo import KartRepoState
from kart.s3_util import fetch_multiple_from_s3
from kart.spatial_filter import SpatialFilter
from kart.structs import CommitWithReference
from kart import subprocess_util as subprocess
from kart.tile import ALL_TILE_DATASET_TYPES

EMPTY_SHA = "0" * 40


@add_help_subcommand
@click.group("lfs+", hidden=True, cls=KartGroup)
@click.pass_context
def lfs_plus(ctx, **kwargs):
    """Git-LFS commands re-implemented in Kart to allow for spatial filtering."""


@lfs_plus.command("ls-files")
@click.pass_context
@click.option(
    "--size", "-s", "show_size", is_flag=True, help="Show the size of each LFS file"
)
@click.option("--all", is_flag=True, help="Scan all refs and HEAD")
@click.argument("ref1", required=False)
@click.argument("ref2", required=False)
def ls_files(ctx, show_size, all, ref1, ref2):
    repo = ctx.obj.get_repo(allowed_states=KartRepoState.ALL_STATES)

    if all:
        start_commits = ["--all"]
        stop_commits = []
    elif not ref1:
        # No refs supplied: search current branch.
        start_commits = ["HEAD"]
        stop_commits = []
    elif ref1 and not ref2:
        # One ref supplied: search that commit.
        start_commits = [ref1, "--no-walk"]
        stop_commits = []
    elif ref1 and ref2:
        # Two refs supplied - search for changes between them.
        ref1 = CommitWithReference.resolve(repo, ref1)
        ref2 = CommitWithReference.resolve(repo, ref2)
        ancestor_id = repo.merge_base(ref1.id, ref2.id)
        if not ancestor_id:
            raise InvalidOperation(f"Commits {ref1.id} and {ref2.id} aren't related.")
        start_commits = [ref1.id.hex, ref2.id.hex]
        stop_commits = [ancestor_id.hex]

    @functools.lru_cache()
    def is_present(lfs_hash):
        return get_local_path_from_lfs_oid(repo, lfs_hash).is_file()

    for commit_id, path_match_result, pointer_blob in rev_list_tile_pointer_files(
        repo, start_commits, stop_commits
    ):
        if show_size:
            pointer_dict = pointer_file_bytes_to_dict(
                pointer_blob, decode_extra_values=False
            )
            lfs_hash = get_hash_from_pointer_file(pointer_dict)
            size = pointer_dict["size"]

        lfs_hash = get_hash_from_pointer_file(pointer_blob)
        indicator = "*" if is_present(lfs_hash) else "-"
        filepath = path_match_result.group(0)

        if show_size:
            click.echo(f"{lfs_hash} {indicator} {filepath} ({size})")
        else:
            click.echo(f"{lfs_hash} {indicator} {filepath}")


@lfs_plus.command()
@click.pass_context
@click.option(
    "--dry-run",
    is_flag=True,
    help="Don't push anything, just show what would be pushed",
)
@click.argument("remote_name", required=False)
@click.argument("remote_url", required=False)
def pre_push(ctx, remote_name, remote_url, dry_run):
    """
    Re-implementation of git-lfs pre-push - but, only searches for pointer blobs at **/.<tile-based-dataset>/tile/**
    (In contrast with git-lfs pre-push, which scans any and all blobs, looking for pointer files).
    This means it won't encounter any features that are missing due to spatial filtering, which git-lfs stumbles over.
    """

    if not remote_name and not remote_url:
        raise RuntimeError(
            "kart lfs+ pre-push should be run through Kart's pre-push hook."
        )

    if os.environ.get("GIT_LFS_SKIP_PUSH", False):
        return

    dry_run = dry_run or os.environ.get("GIT_LFS_DRY_RUN_PUSH", False)

    repo = ctx.obj.get_repo(allowed_states=KartRepoState.ALL_STATES)

    start_commits, stop_commits = get_start_and_stop_commits(sys.stdin)

    # This is a dicts {lfs_oid: file_size} - see _lfs_blobs() function:
    to_push = {}

    for commit_id, path_match_result, pointer_blob in rev_list_tile_pointer_files(
        repo, start_commits, [f"--remotes={remote_name}", *stop_commits]
    ):
        # Because of the way a Kart repo is laid out, we know that:
        # All LFS pointer files are blobs inside **/.*-dataset.v?/tile/** and conversely,
        # All blobs inside **/.*-dataset.v?/tile/** are LFS pointer files.
        pointer_dict = pointer_file_bytes_to_dict(pointer_blob)
        if pointer_dict.get("url"):
            # Currently, the rule is that we never push pointer files that contain a URL.
            # If anyone - any clone of this repo - needs the blob, they can fetch it directly from the URL.
            # We may decide to allow for more complicated flows in a later version of Kart.
            continue
        lfs_oid = get_hash_from_pointer_file(pointer_dict)
        to_push[lfs_oid] = pointer_dict["size"]

    if dry_run:
        click.echo(
            f"Running pre-push with --dry-run: found {_lfs_blobs(to_push)} to push"
        )
        for lfs_oid in to_push:
            click.echo(lfs_oid)
        return

    if to_push:
        push_lfs_oids(repo, remote_name, to_push)


def get_start_and_stop_commits(input_iter):
    start_commits = set()
    stop_commits = set()
    for line in input_iter:
        if not line:
            continue
        local_ref, local_sha, remote_ref, remote_sha = line.split()
        start_commits.add(local_sha)
        stop_commits.add(remote_sha)

    start_commits.discard(EMPTY_SHA)
    stop_commits.discard(EMPTY_SHA)
    start_commits -= stop_commits
    return start_commits, stop_commits


def push_lfs_oids(repo, remote_name, lfs_oids):
    """
    Given a list of OIDs of LFS blobs (not the pointer files, but the LFS blobs themselves)
    push all of those LFS blobs from the local cache to the given remote.
    """
    # Older git-lfs doesn't support stdin so we fall back to using args if we somehow have an older version.
    if _git_lfs_supports_stdin(repo):
        _push_lfs_oids_using_stdin(repo, remote_name, lfs_oids)
    else:
        _push_lfs_oids_using_args(repo, remote_name, lfs_oids)


def _git_lfs_supports_stdin(repo):
    r = subprocess.run(
        ["git-lfs", "push", "?", "--object-id", "--stdin"],
        cwd=repo.workdir_path,
        capture_output=True,
        text=True,
    )
    return "unknown flag: --stdin" not in r.stderr


def _push_lfs_oids_using_stdin(repo, remote_name, lfs_oids):
    with tempfile.TemporaryFile() as oid_file:
        oid_file.write("\n".join(lfs_oids).encode("utf-8"))
        oid_file.write(b"\n")
        oid_file.seek(0)

        try:
            # TODO - capture progress reporting and do our own.
            subprocess.check_call(
                ["git-lfs", "push", remote_name, "--object-id", "--stdin"],
                cwd=repo.workdir_path,
                stdin=oid_file,
            )
        except subprocess.CalledProcessError as e:
            raise SubprocessError(
                f"There was a problem with git-lfs push: {e}", called_process_error=e
            )


def _push_lfs_oids_using_args(repo, remote_name, lfs_oids):
    try:
        # TODO - capture progress reporting and do our own.
        subprocess.check_call(
            ["git-lfs", "push", remote_name, "--object-id", *lfs_oids],
            cwd=repo.workdir_path,
        )
    except subprocess.CalledProcessError as e:
        raise SubprocessError(
            f"There was a problem with git-lfs push: {e}", called_process_error=e
        )


@lfs_plus.command()
@click.pass_context
@click.option("--remote", help="Remote to fetch the LFS blobs from")
@click.option(
    "--spatial-filter/--no-spatial-filter",
    "do_spatial_filter",
    is_flag=True,
    default=True,
    show_default=True,
    help="Respect the current spatial filter - don't fetch tiles that are outside it.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Don't fetch anything, just show what would be fetched",
)
@click.argument("commits", nargs=-1)
def fetch(ctx, remote, do_spatial_filter, dry_run, commits):
    """Fetch LFS files referenced by the given commit(s) from a remote."""
    repo = ctx.obj.get_repo(allowed_states=KartRepoState.ALL_STATES)

    if not commits:
        commits = ["HEAD"]

    fetch_lfs_blobs_for_commits(
        repo,
        commits,
        do_spatial_filter=do_spatial_filter,
        remote_name=remote,
        dry_run=dry_run,
    )


def fetch_lfs_blobs_for_commits(
    repo,
    commits,
    *,
    remote_name=None,
    do_spatial_filter=True,
    dry_run=False,
    quiet=False,
):
    """
    Given a list of commits (or commit OIDS), fetch all the tiles from those commits that
    are not already present in the local cache.
    """
    if not commits:
        return

    if not remote_name:
        remote_name = repo.head_remote_name_or_default

    spatial_filter = (
        repo.spatial_filter if do_spatial_filter else SpatialFilter.MATCH_ALL
    )

    dataset_to_pointer_file_oids = {}
    for commit in commits:
        for dataset in repo.datasets(
            commit, filter_dataset_type=ALL_TILE_DATASET_TYPES
        ):
            pointer_file_oids = dataset_to_pointer_file_oids.setdefault(
                dataset.path, set()
            )
            for blob in dataset.tile_pointer_blobs(spatial_filter=spatial_filter):
                pointer_file_oids.add(blob.hex)

    fetch_lfs_blobs_for_pointer_files(
        repo, dataset_to_pointer_file_oids, dry_run=dry_run, quiet=quiet
    )


def fetch_lfs_blobs_for_pointer_files(
    repo, dataset_to_pointer_file_oids, *, remote_name=None, dry_run=False, quiet=False
):
    """
    Given a dict in the format: {dataset-path: set(pointer-file-oid-1, pointer-file-oid-2, ...)}
    Where dataset-path is the path to a dataset, and each pointer-file-oid is the OID of the pointer file itself
    (not the LFS oid that the pointer file points to) that is present in that dataset:
    Fetches all the tiles that those pointer files point to that are not already present in the local cache.
    """
    if not dataset_to_pointer_file_oids:
        return

    if not remote_name:
        remote_name = repo.head_remote_name_or_default

    dry_run_output = []
    urls = []
    non_urls = []

    # These are dicts {lfs_oid: file_size} - see _lfs_blobs() function:
    urls_sizes = {}
    non_urls_sizes = {}

    pointer_files_to_datasets = _invert_pointer_file_oid_dict(
        dataset_to_pointer_file_oids
    )
    url_redirector = UrlRedirector(repo)

    for pointer_file, datasets in pointer_files_to_datasets.items():
        if isinstance(pointer_file, str):
            pointer_blob = repo[pointer_file]
        elif getattr(pointer_file, "type", None) == pygit2.GIT_OBJ_BLOB:
            pointer_blob = pointer_file
        else:
            raise TypeError("pointer_file should be an OID or a blob object")

        pointer_dict = pointer_file_bytes_to_dict(pointer_blob)
        url = pointer_dict.get("url")
        url = url_redirector.apply_redirect(url, datasets)

        lfs_oid = get_hash_from_pointer_file(pointer_dict)
        pointer_file_oid = pointer_blob.hex
        lfs_path = get_local_path_from_lfs_oid(repo, lfs_oid)
        if lfs_path.is_file():
            continue  # Already fetched.

        if url:
            urls.append((url, lfs_oid))
            urls_sizes[lfs_oid] = pointer_dict["size"]
        else:
            non_urls.append((pointer_file_oid, lfs_oid))
            non_urls_sizes[lfs_oid] = pointer_dict["size"]

        if dry_run:
            if url:
                dry_run_output.append(f"{lfs_oid} ({pointer_blob.hex}) → {url}")
            else:
                dry_run_output.append(f"{lfs_oid} ({pointer_blob.hex})")

    if dry_run:
        click.echo("Running fetch with --dry-run:")
        if urls:
            click.echo(f"  Found {_lfs_blobs(urls_sizes)} to fetch from specific URLs")
        if non_urls:
            found_non_urls = (
                f"  Found {_lfs_blobs(non_urls_sizes)} to fetch from the remote"
            )
            found_non_urls += "" if remote_name else " - but no remote is configured"
            click.echo(found_non_urls)
        if not urls and not non_urls:
            click.echo("  Found nothing to fetch")

        if dry_run_output:
            click.echo()
            click.echo(
                "LFS blob OID:                                                    (Pointer file OID):"
            )
            for line in sorted(dry_run_output):
                click.echo(line)
        return

    if urls:
        _do_fetch_from_urls(repo, urls, quiet=quiet)
    if non_urls and remote_name:
        _do_fetch_from_remote(repo, non_urls, remote_name, quiet=quiet)


def _invert_pointer_file_oid_dict(dataset_to_pointer_file_oids):
    result = {}
    for dataset, pointer_file_oids in dataset_to_pointer_file_oids.items():
        assert isinstance(dataset, str)
        for pointer_file_oid in pointer_file_oids:
            existing = result.setdefault(pointer_file_oid, dataset)
            if dataset != existing:
                if isinstance(existing, str):
                    result[pointer_file_oid] = {existing, dataset}
                elif isinstance(existing, set):
                    existing.add(dataset)
    return result


def _do_fetch_from_urls(repo, urls_and_lfs_oids, quiet=False):
    non_s3_url = next(
        (url for (url, lfs_oid) in urls_and_lfs_oids if not url.startswith("s3://")),
        None,
    )
    if non_s3_url:
        raise NotImplementedError(
            f"Invalid URL - only S3 URLs are currently supported for linked-storage datasets: {non_s3_url}"
        )

    urls_and_paths_and_oids = [
        (url, get_local_path_from_lfs_oid(repo, lfs_oid), lfs_oid)
        for (url, lfs_oid) in urls_and_lfs_oids
    ]
    path_parents = {path.parent for url, path, lfs_oid in urls_and_paths_and_oids}
    for path_parent in path_parents:
        path_parent.mkdir(parents=True, exist_ok=True)
    fetch_multiple_from_s3(urls_and_paths_and_oids, quiet=quiet)


def _do_fetch_from_remote(repo, pointer_file_and_lfs_oids, remote_name, quiet=False):
    # TODO - directly instruct Git-LFS to fetch blobs instead of creating a tree to point Git-LFS to,
    # as and when Git-LFS supports this.

    next_blob_name = (str(i) for i in itertools.count(start=0, step=1))
    object_builder = ObjectBuilder(repo, None)
    for pointer_file_oid, lfs_oid in pointer_file_and_lfs_oids:
        object_builder.insert(next(next_blob_name), repo[pointer_file_oid])
    tree = object_builder.flush()

    try:
        # TODO - capture progress reporting and do our own.
        extra_kwargs = {"stdout": subprocess.DEVNULL} if quiet else {}
        subprocess.check_call(
            ["git-lfs", "fetch", remote_name, tree.hex],
            cwd=repo.workdir_path,
            **extra_kwargs,
        )
        if not quiet:
            # git-lfs fetch generally leaves the cursor at the start of a line which it has already written on.
            click.echo()
    except subprocess.CalledProcessError as e:
        raise SubprocessError(
            f"There was a problem with git-lfs fetch: {e}", called_process_error=e
        )


LFS_OID_PATTERN = re.compile("[0-9a-fA-F]{64}")


@lfs_plus.command()
@click.pass_context
@click.option(
    "--dry-run",
    is_flag=True,
    help="Don't garbage-collect anything, just show what would be garbage-collected",
)
def gc(ctx, dry_run):
    """
    Delete (garbage-collect) LFS files that are not referenced at HEAD from the local cache.

    Point Cloud tiles are LFS files, and they will remain in the local cache until they are explicitly garbage
    collected. Tiles will be present but unreferenced if they part of a commit that was checked out previously, but not
    part of the current commit. The previously checked-out commit could be an earlier revision of the current branch,
    or on another branch entirely.
    """
    repo = ctx.obj.repo

    unpushed_lfs_oids = set()
    for commit_id, path_match_result, pointer_blob in rev_list_tile_pointer_files(
        repo, ["--branches"], ["--remotes"]
    ):
        pointer_dict = pointer_file_bytes_to_dict(pointer_blob)
        if pointer_dict.get("url"):
            continue
        unpushed_lfs_oids.add(get_hash_from_pointer_file(pointer_dict))

    spatial_filter = repo.spatial_filter
    checked_out_lfs_oids = set()
    non_checkout_datasets = repo.non_checkout_datasets
    for dataset in repo.datasets("HEAD", filter_dataset_type=ALL_TILE_DATASET_TYPES):
        if dataset.path not in non_checkout_datasets:
            checked_out_lfs_oids.update(dataset.tile_lfs_hashes(spatial_filter))

    # These are dicts {lfs_oid: file_size} - see _lfs_blobs() function.
    to_delete = {}
    to_delete_once_pushed = {}

    for file in (repo.gitdir_path / "lfs" / "objects").glob("**/*"):
        if not file.is_file() or not LFS_OID_PATTERN.fullmatch(file.name):
            continue  # Not an LFS blob at all.

        if file.name in checked_out_lfs_oids:
            continue  # Can't garbage-collect anything that's currently checked out.

        if file.name in unpushed_lfs_oids:
            to_delete_once_pushed[file] = file.stat().st_size
        else:
            to_delete[file] = file.stat().st_size

    if to_delete_once_pushed:
        click.echo(
            f"Can't delete {_lfs_blobs(to_delete_once_pushed)} from the cache since they have not been pushed to a remote"
        )

    if dry_run:
        click.echo(
            f"Running gc with --dry-run: found {_lfs_blobs(to_delete)} to delete from the cache"
        )
        for file in sorted(to_delete, key=lambda f: f.name):
            click.echo(file.name)
        return

    click.echo(f"Deleting {_lfs_blobs(to_delete)} from the cache...")
    for file in to_delete:
        file.unlink()


def _lfs_blobs(file_size_dict):
    """
    Returns a string looking something like "5 LFS blobs (1MiB)".
    Takes a dict of the form {lfs_oid: file_size_in_bytes}, where the length of the dict is the count of unique blobs.
    This is because building a dict like this is a straight-forward way of getting a unique set of OIDs along
    with a way of finding their total size; maintaining two separate variables - a set of OIDS and a total size -
    makes the code more complicated.
    """

    count = len(file_size_dict)
    total_size = sum(file_size_dict.values())

    blobs = "blob" if count == 1 else "blobs"
    size_desc = human_readable_bytes(total_size)
    return f"{count} LFS {blobs} ({size_desc})"


def human_readable_bytes(num):
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if num < 1024:
            return f"{num:.1f}{unit}B" if (unit and num < 10) else f"{num:.0f}{unit}B"
        num /= 1024.0
    return f"{num:.1f}YiB"
