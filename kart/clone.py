import re
import os
from pathlib import Path, PurePath
from urllib.parse import urlsplit

import click

from .exceptions import InvalidOperation
from .repo import KartRepo, PotentialRepo
from .spatial_filter import SpatialFilterString, spatial_filter_help_text
from kart.cli_util import KartCommand


def get_directory_from_url(url, is_bare):
    if isinstance(url, PurePath):
        path = url
    elif "://" in str(url):
        # 'file:///PATH_TO_REPO'
        url = urlsplit(str(url))
        # file://C:\path\to\repo is non-standard - the path we want actually ends up in url.netloc
        path = url.path or url.netloc
    else:
        match = re.match(r"^\w+@[^:]+?:(.+)$", str(url))
        if match:
            # 'kart@example.com:PATH_TO_REPO'
            path = match.group(1)
        else:
            # 'PATH_TO_REPO'
            path = str(url)

    if not isinstance(path, PurePath):
        path = PurePath(path)

    # Return the directory name.
    name = str(path.name or path.parent.name)

    # Strip trailing ".git". Some hosts (notably github) add this to the end of URLs
    if name.endswith(".git"):
        name = name[:-4]
    if is_bare:
        name += ".git"
    return name


@click.command(cls=KartCommand)
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
    "--workingcopy-location",
    "--workingcopy-path",
    "--workingcopy",
    "wc_location",
    help="Location where the working copy should be created. This should be in one of the following formats:\n"
    "- PATH.gpkg\n"
    "- postgresql://HOST/DBNAME/DBSCHEMA\n"
    "- mssql://HOST/DBNAME/DBSCHEMA\n"
    "- mysql://HOST/DBNAME\n",
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
    "--filter",
    "filterspec",
    help=(
        "(Advanced users only.) Use a partial clone (don't fetch all objects). The supplied <filter-spec> "
        "is used for the partial clone filter. For example, --filter=blob:none will filter out all blobs. "
    ),
    metavar="<filter-spec>",
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
@click.option(
    "--spatial-filter",
    "spatial_filter_spec",
    type=SpatialFilterString(encoding="utf-8"),
    help=spatial_filter_help_text(),
)
@click.option(
    "--spatial-filter-after-clone/--spatial-filter-during-clone",
    is_flag=True,
    default=False,
    help=(
        "Spatial-filter-after-clone only applies the spatial filter once the entire remote repository has been cloned. "
        "This option means the spatial filter can easily be changed later without downloading any more data. This option "
        "may be necessary if the remote doesn't support spatially filtered clones."
    ),
)
@click.argument("url", nargs=1)
@click.argument(
    "directory",
    type=click.Path(exists=False, file_okay=False, writable=True),
    required=False,
)
def clone(
    ctx,
    bare,
    do_checkout,
    wc_location,
    do_progress,
    depth,
    filterspec,
    branch,
    spatial_filter_spec,
    spatial_filter_after_clone,
    url,
    directory,
):
    """Clone a repository into a new directory"""
    repo_path = Path(directory or get_directory_from_url(url, is_bare=bare)).resolve()

    if repo_path.exists() and any(repo_path.iterdir()):
        raise InvalidOperation(f'"{repo_path}" isn\'t empty', param_hint="directory")

    from kart.tabular.working_copy.base import TableWorkingCopy

    TableWorkingCopy.check_valid_creation_location(
        wc_location, PotentialRepo(repo_path)
    )

    if not repo_path.exists():
        repo_path.mkdir(parents=True)

    args = ["--progress" if do_progress else "--quiet"]
    if depth is not None:
        args.append(f"--depth={depth}")
    if branch is not None:
        args.append(f"--branch={branch}")
    if filterspec is not None:
        # git itself does reasonable validation of this, so we don't bother here
        # e.g. "fatal: invalid filter-spec 'hello'"
        # for the various forms it can take, see
        # https://git-scm.com/docs/git-rev-list#Documentation/git-rev-list.txt---filterltfilter-specgt
        args.append(f"--filter={filterspec}")

    repo = KartRepo.clone_repository(
        url,
        repo_path,
        args,
        wc_location,
        bare,
        spatial_filter_spec=spatial_filter_spec,
        spatial_filter_after_clone=spatial_filter_after_clone,
    )

    # Experimental point-cloud datasets:
    if os.environ.get("X_KART_POINT_CLOUDS"):
        from kart.lfs_util import install_lfs_hooks

        lfs_override = os.environ.get("X_KART_SET_LFS_FOR_NEW_REPOS")
        if lfs_override:
            repo.config["lfs.url"] = lfs_override

        install_lfs_hooks(repo)

    # Create working copy, if needed.
    parts_to_create = (
        repo.datasets().working_copy_part_types()
        if do_checkout and not repo.head_is_unborn
        else ()
    )
    repo.working_copy.reset_to_head(create_parts_if_missing=parts_to_create)
