import functools
import itertools
import os
import pygit2
import re
import subprocess
import sys
import tempfile

import click

from kart.cli_util import KartGroup, add_help_subcommand, tool_environment
from kart.exceptions import SubprocessError, InvalidOperation
from kart.lfs_util import (
    pointer_file_bytes_to_dict,
    get_hash_from_pointer_file,
    get_local_path_from_lfs_hash,
)
from kart.object_builder import ObjectBuilder
from kart.rev_list_objects import rev_list_tile_pointer_files
from kart.repo import KartRepoState
from kart.structs import CommitWithReference

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
        return get_local_path_from_lfs_hash(repo, lfs_hash).is_file()

    for (commit_id, path_match_result, pointer_blob) in rev_list_tile_pointer_files(
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
    Re-implementation of git-lfs pre-push - but, only searches for pointer blobs at **/.point-cloud-dataset.v?/tile/**
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

    lfs_oids = set()
    for (commit_id, path_match_result, pointer_blob) in rev_list_tile_pointer_files(
        repo, start_commits, [f"--remotes={remote_name}", *stop_commits]
    ):
        # Because of the way a Kart repo is laid out, we know that:
        # All LFS pointer files are blobs inside **/.point-cloud-dataset.v?/tile/**
        # All blobs inside **/.point-cloud-dataset.v?/tile/** are LFS pointer files.
        lfs_oids.add(get_hash_from_pointer_file(pointer_blob))

    if dry_run:
        click.echo(
            f"Running pre-push with --dry-run: pushing {len(lfs_oids)} LFS blobs"
        )
        for lfs_oid in lfs_oids:
            click.echo(lfs_oid)
        return

    if lfs_oids:
        push_lfs_oids(repo, remote_name, lfs_oids)


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
        env=tool_environment(),
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
                env=tool_environment(),
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
            env=tool_environment(),
            cwd=repo.workdir_path,
        )
    except subprocess.CalledProcessError as e:
        raise SubprocessError(
            f"There was a problem with git-lfs push: {e}", called_process_error=e
        )


@lfs_plus.command()
@click.pass_context
@click.option(
    "--dry-run",
    is_flag=True,
    help="Don't fetch anything, just show what would be fetched",
)
@click.option("--remote", help="Remote to fetch the LFS blobs from")
@click.argument("commits", nargs=-1)
def fetch(ctx, remote, commits, dry_run):
    """Fetch LFS files referenced by the given commit(s) from a remote."""
    repo = ctx.obj.get_repo(allowed_states=KartRepoState.ALL_STATES)

    if not commits:
        commits = ["HEAD"]

    fetch_lfs_blobs_for_commits(repo, commits, remote_name=remote, dry_run=dry_run)


def fetch_lfs_blobs_for_commits(
    repo, commits, *, remote_name=None, dry_run=False, quiet=False
):
    """
    Given a list of commits (or commit OIDS), fetch all the tiles from those commits that
    are not already present in the local cache.
    """
    if not commits:
        return

    if not remote_name:
        remote_name = repo.head_remote_name_or_default
    if not remote_name:
        return

    pointer_file_oids = set()
    for commit in commits:
        for dataset in repo.datasets(commit, filter_dataset_type="point-cloud"):
            pointer_file_oids.update(blob.hex for blob in dataset.tile_pointer_blobs())

    fetch_lfs_blobs_for_pointer_files(
        repo, pointer_file_oids, dry_run=dry_run, quiet=quiet
    )


def fetch_lfs_blobs_for_pointer_files(
    repo, pointer_files, *, remote_name=None, dry_run=False, quiet=False
):
    """
    Given a list of pointer files (or OIDs of pointer files themselves - not the OIDs they point to)
    fetch all the tiles that those pointer files point to that are not already present in the local cache.
    """
    if not pointer_files:
        return

    if not remote_name:
        remote_name = repo.head_remote_name_or_default
    if not remote_name:
        return

    next_blob_name = (str(i) for i in itertools.count(start=0, step=1))

    # TODO - directly instruct Git-LFS to fetch blobs instead of creating a tree to point Git-LFS to,
    # as and when Git-LFS supports this.
    object_builder = ObjectBuilder(repo, None)
    dry_run_output = []

    for pointer_file in pointer_files:
        if isinstance(pointer_file, str):
            pointer_blob = repo[pointer_file]
        elif getattr(pointer_file, "type", None) == pygit2.GIT_OBJ_BLOB:
            pointer_blob = pointer_file
        else:
            raise TypeError("pointer_file should be an OID or a blob object")
        lfs_oid = get_hash_from_pointer_file(pointer_blob)
        lfs_path = get_local_path_from_lfs_hash(repo, lfs_oid)
        if lfs_path.is_file():
            continue  # Already fetched.

        # TODO - don't fetch tiles that are outside the spatial filter.

        object_builder.insert(next(next_blob_name), pointer_blob)
        if dry_run:
            dry_run_output.append(f"{lfs_oid} ({pointer_blob.hex})")

    if dry_run:
        click.echo(
            f"Running fetch with --dry-run: fetching {len(dry_run_output)} LFS blobs"
        )
        if dry_run_output:
            click.echo(
                "LFS blob OID:                                                    (Pointer file OID):"
            )
            for line in sorted(dry_run_output):
                click.echo(line)
        return

    tree = object_builder.flush()
    if not tree:
        return

    try:
        # TODO - capture progress reporting and do our own.
        extra_kwargs = {"stdout": subprocess.DEVNULL} if quiet else {}
        subprocess.check_call(
            ["git-lfs", "fetch", remote_name, tree.hex],
            env=tool_environment(),
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
    help="Don't fetch anything, just show what would be fetched",
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

    remote_name = repo.head_remote_name_or_default
    if not remote_name:
        raise InvalidOperation(
            "LFS files cannot be garbage collected unless there is a remote to refetch them from."
        )

    unpushed_lfs_oids = set()
    for (commit_id, path_match_result, pointer_blob) in rev_list_tile_pointer_files(
        repo, ["--branches"], ["--remotes"]
    ):
        unpushed_lfs_oids.add(get_hash_from_pointer_file(pointer_blob))

    spatial_filter = repo.spatial_filter
    checked_out_lfs_oids = set()
    for dataset in repo.datasets("HEAD", filter_dataset_type="point-cloud"):
        checked_out_lfs_oids.update(dataset.tile_lfs_hashes(spatial_filter))

    to_delete = set()
    total_size_to_delete = 0

    to_delete_once_pushed = set()
    total_size_to_delete_once_pushed = 0

    for file in (repo.gitdir_path / "lfs" / "objects").glob("**/*"):
        if not file.is_file() or not LFS_OID_PATTERN.fullmatch(file.name):
            continue  # Not an LFS blob at all.

        if file.name in checked_out_lfs_oids:
            continue  # Can't garbage-collect anything that's currently checked out.

        if file.name in unpushed_lfs_oids:
            to_delete_once_pushed.add(file)
            total_size_to_delete_once_pushed += file.stat().st_size
        else:
            to_delete.add(file)
            total_size_to_delete += file.stat().st_size

    if to_delete_once_pushed:
        size_desc = human_readable_bytes(total_size_to_delete_once_pushed)
        click.echo(
            f"Can't delete {len(to_delete_once_pushed)} LFS blobs ({size_desc}) from the cache since they have not been pushed to the remote"
        )

    size_desc = human_readable_bytes(total_size_to_delete)
    if dry_run:
        click.echo(
            f"Running gc with --dry-run: deleting {len(to_delete)} LFS blobs ({size_desc}) from the cache"
        )
        for file in sorted(to_delete, key=lambda f: f.name):
            click.echo(file.name)
        return

    click.echo(f"Deleting {len(to_delete)} LFS blobs ({size_desc}) from the cache...")
    for file in to_delete:
        file.unlink()


def human_readable_bytes(num):
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if num < 1024:
            return f"{num:.1f}{unit}B" if (unit and num < 10) else f"{num:.0f}{unit}B"
        num /= 1024.0
    return f"{num:.1f}YiB"
