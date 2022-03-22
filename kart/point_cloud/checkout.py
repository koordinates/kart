import click
import shutil

from .dataset1 import PointCloudV1

# Handles point-cloud side of checkout.
# Some of this may move at some point to be part of "filesystem" checkout which will also handle attachments.


def reset_wc_if_needed(repo):
    """Checks out point cloud tiles to working copy directory."""
    if repo.is_bare:
        return
    assert repo.workdir_path.is_dir()

    for dataset in repo.datasets():
        if not isinstance(dataset, PointCloudV1):
            continue

        wc_tiles_dir = repo.workdir_path / dataset.path / "tiles"

        (wc_tiles_dir).mkdir(parents=True, exist_ok=True)

        for tilename, lfs_path in dataset.tilenames_with_lfs_paths():
            if not lfs_path.is_file():
                click.echo(
                    f"Couldn't find tile {tilename} locally - skipping...", err=True
                )
                continue
            shutil.copy(lfs_path, wc_tiles_dir / tilename)


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
