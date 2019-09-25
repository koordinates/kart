import os
import subprocess
from pathlib import Path

import click
import pygit2

from . import checkout
from .structure import RepositoryStructure


@click.command()
@click.pass_context
@click.option("--checkout/--no-checkout", "do_checkout", is_flag=True, default=True, help="Whether to checkout a working copy in the repository")
@click.argument("url", nargs=1)
@click.argument("directory", type=click.Path(exists=False, file_okay=False, writable=True), required=False)
def clone(ctx, do_checkout, url, directory):
    """ Clone a repository into a new directory """
    repo_dir = Path(directory or os.path.split(url)[1])
    if not repo_dir.suffix == ".snow":
        raise click.BadParameter("name should end in .snow", param_hint="directory")

    # we use subprocess because it deals with credentials much better & consistently than we can do at the moment.
    # pygit2.clone_repository() works fine except for that
    subprocess.check_call([
        "git", "clone",
        "--bare",
        "--config", "remote.origin.fetch=+refs/heads/*:refs/remotes/origin/*",
        url,
        repo_dir.resolve()
    ])

    repo = pygit2.Repository(str(repo_dir.resolve()))
    head_ref = repo.head.shorthand  # master
    repo.config[f"branch.{head_ref}.remote"] = "origin"
    repo.config[f"branch.{head_ref}.merge"] = f"refs/heads/{head_ref}"

    if do_checkout:
        # Checkout a working copy
        wc_path = f"{repo_dir.stem}.gpkg"

        click.echo(f'Checkout to {wc_path} as GPKG ...')

        checkout.checkout_new(
            repo_structure=RepositoryStructure(repo),
            path=str(wc_path),
            commit=repo.head.peel(pygit2.Commit),
        )
