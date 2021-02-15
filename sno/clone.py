import re

from pathlib import Path, PurePath
from urllib.parse import urlsplit

import click

from . import checkout
from .exceptions import InvalidOperation
from .repo import SnoRepo
from .working_copy.base import WorkingCopy


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
    is_flag=True,
    default=False,
    help='Whether the new repository should be "bare" and have no working copy',
)
@click.option(
    "--checkout/--no-checkout",
    "do_checkout",
    is_flag=True,
    default=True,
    help="Whether the new repository should immediately check out a working copy (only effects non-bare repos)",
)
@click.option(
    "--workingcopy-path",
    "--workingcopy",
    "wc_path",
    help="Path where the working copy should be created. "
    "This should be a GPKG file eg example.gpkg or a postgres URI including schema eg postgresql://[HOST]/DBNAME/SCHEMA",
)
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
@click.option(
    "-b",
    "--branch",
    metavar="NAME",
    help=(
        "Instead of pointing the newly created HEAD to the branch pointed to by the cloned repository's "
        "HEAD, point to NAME branch instead. In a non-bare repository, this is the branch that will be "
        "checked out.  --branch can also take tags and detaches the HEAD at that commit in the resulting "
        "repository. "
    ),
)
@click.argument("url", nargs=1)
@click.argument(
    "directory",
    type=click.Path(exists=False, file_okay=False, writable=True),
    required=False,
)
def clone(ctx, bare, do_checkout, wc_path, do_progress, depth, branch, url, directory):
    """ Clone a repository into a new directory """

    repo_path = Path(directory or get_directory_from_url(url)).resolve()

    if repo_path.exists() and any(repo_path.iterdir()):
        raise InvalidOperation(f'"{repo_path}" isn\'t empty', param_hint="directory")
    WorkingCopy.check_valid_creation_path(repo_path, wc_path)

    if not repo_path.exists():
        repo_path.mkdir(parents=True)

    args = ["--progress" if do_progress else "--quiet"]
    if depth is not None:
        args.append(f"--depth={depth}")
    if branch is not None:
        args.append(f"--branch={branch}")

    repo = SnoRepo.clone_repository(url, repo_path, args, wc_path, bare)

    # Create working copy, if needed.
    head_commit = repo.head_commit
    if head_commit is not None and do_checkout and not bare:
        checkout.reset_wc_if_needed(repo, head_commit)
