import json
import os
import subprocess
import urllib.parse
from pathlib import Path

import click
import pygit2

from . import checkout


def get_repo_layer(repo):
    tree = repo.head.peel(pygit2.Tree)

    if len(tree) != 1 or tree[0].type != 'tree':
        raise ValueError("Repository structure error")

    layer = tree[0].name

    layer_tree = tree / layer
    meta_tree = layer_tree / "meta"
    meta_info = json.loads((meta_tree / "gpkg_contents").obj.data)

    if meta_info["table_name"] != layer:
        raise ValueError(f"Layer mismatch (table_name={meta_info['table_name']}; layer={layer}")

    return layer


@click.command()
@click.pass_context
@click.option("--checkout/--no-checkout", "do_checkout", is_flag=True, default=True, help="Whether to checkout a working copy in the repository")
@click.argument("url", nargs=1)
@click.argument("directory", type=click.Path(exists=False, file_okay=False, writable=True), required=False)
def clone(ctx, do_checkout, url, directory):
    """ Clone a repository into a new directory """
    layer = None
    url_parts = urllib.parse.urlsplit(url)
    if url_parts.fragment:
        layer = url_parts.fragment
        url = urllib.parse.urlunsplit(url_parts[:-1] + ('',))

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
        if not layer:
            layer = get_repo_layer(repo)

        # Checkout a working copy
        wc_path = repo_dir / f"{repo_dir.stem}.gpkg"

        click.echo(f'Checkout {layer} to {wc_path} as GPKG ...')

        try:
            checkout.checkout_new(
                repo=repo,
                working_copy=str(wc_path),
                layer=layer,
                commit=repo.head.peel(pygit2.Commit),
                fmt="GPKG"
            )
        except KeyError as e:
            raise click.ClickException(f"Couldn't find layer {e} to checkout.")
