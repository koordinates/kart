#!/usr/bin/env python3
import logging
import os
import subprocess

import click
import pygit2

from . import core  # noqa
from . import (
    apply,
    branch,
    checkout,
    clone,
    conflicts,
    commit,
    diff,
    init,
    fsck,
    merge,
    pull,
    resolve,
    show,
    status,
    query,
    upgrade,
)
from .cli_util import call_and_exit_flag
from .context import Context
from .exec import execvp


def print_version(ctx):
    import apsw
    import osgeo
    import rtree

    import sno

    with open(os.path.join(os.path.split(sno.__file__)[0], "VERSION")) as version_file:
        version = version_file.read().strip()

    click.echo(f"Sno v{version}, Copyright (c) Sno Contributors")

    git_version = (
        subprocess.check_output(["git", "--version"])
        .decode("ascii")
        .strip()
        .split()[-1]
    )

    sidx_version = rtree.index.__c_api_version__.decode("ascii")

    db = apsw.Connection(":memory:")
    dbcur = db.cursor()
    db.enableloadextension(True)
    dbcur.execute("SELECT load_extension(?)", (sno.spatialite_path,))
    spatialite_version = dbcur.execute("SELECT spatialite_version();").fetchone()[0]

    click.echo(
        (
            f"» GDAL v{osgeo._gdal.__version__}\n"
            f"» PyGit2 v{pygit2.__version__}; "
            f"Libgit2 v{pygit2.LIBGIT2_VERSION}; "
            f"Git v{git_version}\n"
            f"» APSW v{apsw.apswversion()}; "
            f"SQLite v{apsw.sqlitelibversion()}; "
            f"SpatiaLite v{spatialite_version}\n"
            f"» SpatialIndex v{sidx_version}"
        )
    )

    ctx.exit()


@click.group()
@click.option(
    "-C",
    "--repo",
    "repo_dir",
    type=click.Path(file_okay=False, dir_okay=True),
    default=None,
    metavar="PATH",
)
@call_and_exit_flag(
    "--version", callback=print_version, help="Show version information and exit.",
)
@click.option("-v", "--verbose", count=True, help="Repeat for more verbosity")
@click.pass_context
def cli(ctx, repo_dir, verbose):
    ctx.ensure_object(Context)
    if repo_dir:
        ctx.obj.user_repo_path = repo_dir

    # default == WARNING; -v == INFO; -vv == DEBUG
    log_level = logging.WARNING - min(10 * verbose, 20)
    logging.basicConfig(level=log_level)


# Commands from modules:
cli.add_command(apply.apply)
cli.add_command(branch.branch)
cli.add_command(checkout.checkout)
cli.add_command(checkout.restore)
cli.add_command(checkout.switch)
cli.add_command(checkout.workingcopy_set_path)
cli.add_command(clone.clone)
cli.add_command(conflicts.conflicts)
cli.add_command(commit.commit)
cli.add_command(diff.diff)
cli.add_command(fsck.fsck)
cli.add_command(init.import_table)
cli.add_command(init.init)
cli.add_command(merge.merge)
cli.add_command(pull.pull)
cli.add_command(resolve.resolve)
cli.add_command(show.show)
cli.add_command(status.status)
cli.add_command(query.query)
cli.add_command(upgrade.upgrade)


# aliases/shortcuts


@cli.command()
@click.pass_context
def reset(ctx):
    """ Discard changes made in the working copy (ie. reset to HEAD """
    ctx.invoke(checkout.checkout, force=True, refish="HEAD")


# straight process-replace commands


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def log(ctx, args):
    """ Show commit logs """
    execvp("git", ["git", "-C", ctx.obj.repo.path, "log"] + list(args))


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def push(ctx, args):
    """ Update remote refs along with associated objects """
    execvp("git", ["git", "-C", ctx.obj.repo.path, "push"] + list(args))


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def fetch(ctx, args):
    """ Download objects and refs from another repository """
    execvp("git", ["git", "-C", ctx.obj.repo.path, "fetch"] + list(args))


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def remote(ctx, args):
    """ Manage set of tracked repositories """
    execvp("git", ["git", "-C", ctx.obj.repo.path, "remote"] + list(args))


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def tag(ctx, args):
    """ Create, list, delete or verify a tag object signed with GPG """
    execvp("git", ["git", "-C", ctx.obj.repo.path, "tag"] + list(args))


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def config(ctx, args):
    """ Get and set repository or global options """
    params = ["git", "config"]
    if ctx.obj.user_repo_path:
        params[1:1] = ["-C", ctx.obj.user_repo_path]
    execvp("git", params + list(args))


if __name__ == "__main__":
    cli()
