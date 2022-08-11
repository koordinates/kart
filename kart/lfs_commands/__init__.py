import os
import subprocess
import sys

import click

from kart.cli_util import add_help_subcommand
from kart.lfs_util import get_hash_from_pointer_file
from kart.rev_list_objects import rev_list_tile_pointer_files
from kart.repo import KartRepoState

EMPTY_SHA = "0" * 40


@add_help_subcommand
@click.group("lfs+", hidden=True)
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
    This means it won't encounter any features that are missing due to spatial filtering, which git-lfs stumbles over.
    """

    if not remote_url and not remote_url:
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
        lfs_oids.add(get_hash_from_pointer_file(pointer_blob))

    if dry_run:
        click.echo(
            f"Running pre-push with --dry-run: pushing {len(lfs_oids)} LFS blobs"
        )
        for lfs_oid in lfs_oids:
            click.echo(lfs_oid)
        return

    if lfs_oids:
        # TODO - chunk lfs_oids so that we don't overflow the maximum argument size.
        # TODO - capture chunk progress and report our own overall progress
        subprocess.check_call(
            ["git-lfs", "push", remote_name, "--object-id", *lfs_oids]
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
