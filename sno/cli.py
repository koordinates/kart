#!/usr/bin/env python3
import logging
import os
from pathlib import Path
import subprocess
import sys

import certifi
import click
import pygit2

from . import core  # noqa
from . import (
    checkout,
    clone,
    commit,
    diff,
    init,
    fsck,
    merge,
    pull,
    status,
    query,
    upgrade,
)
from .context import Context


def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return

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
            f"≫ GDAL v{osgeo._gdal.__version__}\n"
            f"≫ PyGit2 v{pygit2.__version__}; "
            f"Libgit2 v{pygit2.LIBGIT2_VERSION}; "
            f"Git v{git_version}\n"
            f"≫ APSW v{apsw.apswversion()}; "
            f"SQLite v{apsw.sqlitelibversion()}; "
            f"SpatiaLite v{spatialite_version}\n"
            f"≫ SpatialIndex v{sidx_version}"
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
@click.option(
    "--version",
    is_flag=True,
    callback=print_version,
    expose_value=False,
    is_eager=True,
    help="Show version information and exit.",
)
@click.option("-v", "--verbose", count=True, help="Repeat for more verbosity")
@click.pass_context
def cli(ctx, repo_dir, verbose):
    ctx.ensure_object(Context)
    if repo_dir:
        ctx.obj.repo_path = repo_dir

    # default == WARNING; -v == INFO; -vv == DEBUG
    log_level = logging.WARNING - min(10 * verbose, 20)
    logging.basicConfig(level=log_level)


# Commands from modules:
cli.add_command(checkout.checkout)
cli.add_command(checkout.restore)
cli.add_command(checkout.switch)
cli.add_command(checkout.workingcopy_set_path)
cli.add_command(clone.clone)
cli.add_command(commit.commit)
cli.add_command(diff.diff)
cli.add_command(fsck.fsck)
cli.add_command(init.import_gpkg)
cli.add_command(init.import_table)
cli.add_command(init.init)
cli.add_command(merge.merge)
cli.add_command(pull.pull)
cli.add_command(status.status)
cli.add_command(query.query)
cli.add_command(upgrade.upgrade)


# aliases/shortcuts


@cli.command()
@click.pass_context
def show(ctx):
    """ Show the current commit """
    ctx.invoke(log, args=["-1"])


@cli.command()
@click.pass_context
def reset(ctx):
    """ Discard changes made in the working copy (ie. reset to HEAD """
    ctx.invoke(checkout.checkout, force=True, refish="HEAD")


# straight process-replace commands


def _execvp(file, args):
    if "_SNO_NO_EXEC" in os.environ:
        # used in testing. This is pretty hackzy
        p = subprocess.run([file] + args[1:], capture_output=True, encoding="utf-8")
        sys.stdout.write(p.stdout)
        sys.stderr.write(p.stderr)
        sys.exit(p.returncode)
    else:
        os.execvp(file, args)


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def log(ctx, args):
    """ Show commit logs """
    repo_path = ctx.obj.repo_path
    repo = ctx.obj.repo

    _execvp("git", ["git", "-C", repo_path, "log"] + list(args))


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def push(ctx, args):
    """ Update remote refs along with associated objects """
    repo_path = ctx.obj.repo_path
    repo = ctx.obj.repo

    _execvp("git", ["git", "-C", repo_path, "push"] + list(args))


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def fetch(ctx, args):
    """ Download objects and refs from another repository """
    repo_path = ctx.obj.repo_path
    repo = ctx.obj.repo

    _execvp("git", ["git", "-C", repo_path, "fetch"] + list(args))


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def branch(ctx, args):
    """ List, create, or delete branches """
    repo_path = ctx.obj.repo_path
    repo = ctx.obj.repo

    # git's branch protection behaviour doesn't apply if it's a bare repository
    # attempt to apply it here.
    sargs = set(args)
    if sargs & {"-d", "--delete", "-D"}:
        branch = repo.head.shorthand
        if branch in sargs:
            raise click.ClickException(
                f"Cannot delete the branch '{branch}' which you are currently on."
            )

    _execvp("git", ["git", "-C", repo_path, "branch"] + list(args))


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def remote(ctx, args):
    """ Manage set of tracked repositories """
    repo_path = ctx.obj.repo_path
    repo = ctx.obj.repo

    _execvp("git", ["git", "-C", repo_path, "remote"] + list(args))


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def tag(ctx, args):
    """ Create, list, delete or verify a tag object signed with GPG """
    repo_path = ctx.obj.repo_path
    repo = ctx.obj.repo

    _execvp("git", ["git", "-C", repo_path, "tag"] + list(args))


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def config(ctx, args):
    """ Get and set repository or global options """
    repo_path = ctx.obj.repo_path
    params = ["git", "config"]
    if ctx.obj.has_repo_path:
        params[1:1] = ["-C", repo_path]
    _execvp("git", params + list(args))


if __name__ == "__main__":
    cli()
