import os
import subprocess
from pathlib import Path

import click
import pygit2

from . import checkout
from .structure import RepositoryStructure


def get_directory_from_url(url):
    if '/' in url:
        # 'sno@example.com:path/to/repo'
        return url.rsplit('/', 1)[1]
    elif ':' in url:
        # 'sno@example.com:repo'
        return url.rsplit(':', 1)[1]
    else:
        # 'otherdir' or 'C:\otherdir'
        return os.path.split(url)[1]


@click.command()
@click.pass_context
@click.option(
    "--checkout/--no-checkout",
    "do_checkout",
    is_flag=True,
    default=True,
    help="Whether to checkout a working copy in the repository",
)
@click.argument("url", nargs=1)
@click.argument(
    "directory",
    type=click.Path(exists=False, file_okay=False, writable=True),
    required=False,
)
def clone(ctx, do_checkout, url, directory):
    """ Clone a repository into a new directory """

    repo_path = Path(directory or get_directory_from_url(url))

    # we use subprocess because it deals with credentials much better & consistently than we can do at the moment.
    # pygit2.clone_repository() works fine except for that
    subprocess.check_call(
        [
            "git",
            "clone",
            "--bare",
            "--config",
            "remote.origin.fetch=+refs/heads/*:refs/remotes/origin/*",
            url,
            str(repo_path.resolve()),
        ]
    )

    repo = pygit2.Repository(str(repo_path.resolve()))
    if repo.head_is_unborn:
        # this happens when you clone an empty repo.
        # HEAD points to `refs/heads/master`, but that doesn't exist yet.
        # (but it gets created when you commit)
        # Calling `repo.head` raises a GitError here, so we just hardcode this one
        head_ref = 'master'
    else:
        head_ref = repo.head.shorthand  # master, probably
    repo.config[f"branch.{head_ref}.remote"] = "origin"
    repo.config[f"branch.{head_ref}.merge"] = f"refs/heads/{head_ref}"

    if do_checkout:
        # Checkout a working copy
        wc_path = f"{repo_path.stem}.gpkg"

        click.echo(f"Checkout to {wc_path} as GPKG ...")

        if repo.is_empty:
            checkout.checkout_empty_repo(repo, path=str(wc_path))
        else:
            checkout.checkout_new(
                repo_structure=RepositoryStructure(repo), path=str(wc_path),
            )
