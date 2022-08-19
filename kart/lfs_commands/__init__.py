import os
import itertools
import pygit2
import subprocess
import sys

import click

from kart.cli_util import KartGroup, add_help_subcommand, tool_environment
from kart.exceptions import SubprocessError
from kart.lfs_util import get_hash_from_pointer_file, get_local_path_from_lfs_hash
from kart.object_builder import ObjectBuilder
from kart.rev_list_objects import rev_list_tile_pointer_files
from kart.repo import KartRepoState

EMPTY_SHA = "0" * 40


@add_help_subcommand
@click.group("lfs+", hidden=True, cls=KartGroup)
@click.pass_context
def lfs_plus(ctx, **kwargs):
    """Git-LFS commands re-implemented in Kart to allow for spatial filtering."""


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
        repo, start_commits, stop_commits
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
        try:
            # TODO - chunk lfs_oids so that we don't overflow the maximum argument size.
            # TODO - capture chunk progress and report our own overall progress
            subprocess.check_call(
                ["git-lfs", "push", remote_name, "--object-id", *lfs_oids],
                env=tool_environment(),
                cwd=repo.workdir_path,
            )
        except subprocess.CalledProcessError as e:
            raise SubprocessError(
                f"There was a problem with git-lfs push: {e}", called_process_error=e
            )


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
