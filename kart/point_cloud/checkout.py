import shutil
import subprocess
import sys

import click

from kart.cli_util import tool_environment
from kart.exceptions import translate_subprocess_exit_code
from .v1 import PointCloudV1

# Handles point-cloud side of checkout.
# Some of this may move at some point to be part of "filesystem" checkout which will also handle attachments.


def checkout_point_clouds(repo):
    """Checks out point cloud tiles to working copy directory."""
    if repo.is_bare:
        return
    assert repo.workdir_path.is_dir()

    # TODO - checkout should check the existing worktree-index to do the following:
    # - see what the <commit>...<commit> diffs are of the checkout operation we are doing
    # - make sure they don't conflict with uncommitted WC diffs / make sure there are no WC diffs
    # - apply the <commit>...<commit> diffs to the WC (instead of checking out from scratch)
    # Right now we don't do any of this - we just write a new worktree and a new index over the top of the old one.
    # Nothing is properly tidied up - files from the previous commit that was checked out will stay there unless
    # they are replaced by the version of that file from the latest commit.

    reset_index_paths = []
    for dataset in repo.datasets():
        if not isinstance(dataset, PointCloudV1):
            continue

        reset_index_paths.append(dataset.path)
        wc_tiles_dir = repo.workdir_path / dataset.path
        (wc_tiles_dir).mkdir(parents=True, exist_ok=True)

        for tilename, lfs_path in dataset.tilenames_with_lfs_paths():
            if not lfs_path.is_file():
                click.echo(
                    f"Couldn't find tile {tilename} locally - skipping...", err=True
                )
                continue
            shutil.copy(lfs_path, wc_tiles_dir / tilename)

    # TODO - this marks everything inside these dataset folders as "clean" until the user makes an edit, which is then
    # "dirty". Since we aren't tidying up the folders properly, there could actually be untracked files in them still.
    # But it works well enough to show how diffing and committing works in principle - it just needs more work.
    reset_worktree_index(repo, reset_index_paths)


def reset_worktree_index(repo, reset_index_paths):
    """
    Creates a file <GIT-DIR>/worktree-index that is an index of that part of the contents of the workdir
    that is contained within the given update_index_paths (which can be files or folders).
    """
    worktree_index_file = repo.gitdir_file("worktree-index")

    # NOTE - we could also use pygit2.Index to do this, but this has been easier to get working so far.
    env = tool_environment()
    env["GIT_INDEX_FILE"] = str(worktree_index_file)

    # TODO - this may be inefficient - it might be possible to reuse some or all of the existing index,
    # rather than regenerating it from scratch.
    if worktree_index_file.exists():
        worktree_index_file.unlink()

    for path in reset_index_paths:
        try:
            args = ["git", "add", path]
            subprocess.check_call(
                args, env=env, cwd=repo.workdir_path, stdout=subprocess.DEVNULL
            )
        except subprocess.CalledProcessError as e:
            sys.exit(translate_subprocess_exit_code(e.returncode))


@click.command("point-cloud-checkout", hidden=True)
@click.pass_context
def point_cloud_checkout(ctx):
    """
    Basic checkout operation for point-clouds - can only checkout any-and-all point-cloud datasets at HEAD,
    as folders full of tiles.

    Not made for switching branch, and doesn't do any tidying up - automatically removing these tiles
    during a checkout, reset, or restore operation is not yet supported.
    """
    repo = ctx.obj.repo
    reset_wc_if_needed(repo)
