import re
import subprocess
import sys

from pathlib import Path, PurePath
from urllib.parse import urlsplit

import click
import pygit2

from . import checkout
from .exceptions import translate_subprocess_exit_code
from .repository_version import get_repo_version
from .working_copy import WorkingCopy


def get_directory_from_url(url):
    if "://" in str(url):
        return urlsplit(str(url)).path.split("/")[-1]
    match = re.match(r"\w+@[^:]+?:(?:.*/)?(.+?)/?$", str(url))
    if match:
        # 'sno@example.com:path/to/repo'
        return match.group(1)

    # 'otherdir' or 'C:\otherdir'
    if not isinstance(url, Path):
        # Use PurePath so that the tests on mac/linux can test windows paths
        path = PurePath(url)

    return str(path.name or path.parent.name)


@click.command()
@click.pass_context
@click.option(
    "--bare",
    "--no-checkout/--checkout",
    is_flag=True,
    default=False,
    help='Whether the new repository should be "bare" and have no working copy',
)
@click.option(
    "--workingcopy-path",
    "wc_path",
    type=click.Path(dir_okay=False),
    help="Path where the working copy should be created",
)
@click.option("--workingcopy-version", "wc_version", type=int)
@click.option(
    "--progress/--quiet",
    "do_progress",
    is_flag=True,
    default=True,
    help="Whether to report progress to stderr",
)
@click.option(
    "--depth",
    type=click.INT,
    help="Create a shallow clone with a history truncated to the specified number of commits.",
)
@click.argument("url", nargs=1)
@click.argument(
    "directory",
    type=click.Path(exists=False, file_okay=False, writable=True),
    required=False,
)
def clone(ctx, bare, wc_path, wc_version, do_progress, depth, url, directory):
    """ Clone a repository into a new directory """

    repo_path = Path(directory or get_directory_from_url(url))
    args = [
        "git",
        "clone",
        "--progress" if do_progress else "--quiet",
        "--bare",
        "--config",
        "remote.origin.fetch=+refs/heads/*:refs/remotes/origin/*",
        url,
        str(repo_path.resolve()),
    ]
    if depth is not None:
        args.append(f"--depth={depth}")

    try:
        # we use subprocess because it deals with credentials much better & consistently than we can do at the moment.
        # pygit2.clone_repository() works fine except for that
        subprocess.check_call(args)
    except subprocess.CalledProcessError as e:
        sys.exit(translate_subprocess_exit_code(e.returncode))

    repo = pygit2.Repository(str(repo_path.resolve()))
    if repo.head_is_unborn:
        # this happens when you clone an empty repo.
        # HEAD points to `refs/heads/master`, but that doesn't exist yet.
        # (but it gets created when you commit)
        # Calling `repo.head` raises a GitError here, so we just hardcode this one
        head_ref = "master"
    else:
        head_ref = repo.head.shorthand  # master, probably
    repo.config[f"branch.{head_ref}.remote"] = "origin"
    repo.config[f"branch.{head_ref}.merge"] = f"refs/heads/{head_ref}"
    repo.config["sno.repository.version"] = get_repo_version(repo)

    WorkingCopy.write_config(repo, wc_path, bare)

    if not repo.is_empty:
        head_commit = repo.head.peel(pygit2.Commit)
        checkout.reset_wc_if_needed(repo, head_commit)
