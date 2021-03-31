#!/usr/bin/env python3
import logging
import os
import re
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
    data,
    diff,
    fsck,
    init,
    log,
    merge,
    meta,
    pull,
    resolve,
    show,
    status,
    query,
    upgrade,
)
from .cli_util import (
    add_help_subcommand,
    call_and_exit_flag,
    git_remote_environment,
    startup_load_git_init_config,
)
from .context import Context
from .exec import execvp, execvpe


def get_version():
    import sno

    with open(os.path.join(os.path.split(sno.__file__)[0], "VERSION")) as version_file:
        return version_file.read().strip()


def print_version(ctx):
    import osgeo
    import psycopg2
    import pysqlite3
    import rtree
    import sqlalchemy

    from sno.sqlalchemy.create_engine import gpkg_engine

    click.echo(f"Sno v{get_version()}, Copyright (c) Sno Contributors")

    git_version = (
        subprocess.check_output(["git", "--version"])
        .decode("ascii")
        .strip()
        .split()[-1]
    )

    sidx_version = rtree.index.__c_api_version__.decode("ascii")

    engine = gpkg_engine(":memory:")
    with engine.connect() as conn:
        spatialite_version = conn.scalar("SELECT spatialite_version();")

    pq_version = psycopg2.__libpq_version__
    pq_version = "{}.{}.{}".format(
        *[int(k) for k in re.findall(r"\d\d", str(psycopg2.__libpq_version__))]
    )

    proj_version = "{}.{}.{}".format(
        osgeo.osr.GetPROJVersionMajor(),
        osgeo.osr.GetPROJVersionMinor(),
        osgeo.osr.GetPROJVersionMicro(),
    )

    click.echo(
        (
            f"» GDAL v{osgeo._gdal.__version__}; "
            f"PROJ v{proj_version}\n"
            f"» PyGit2 v{pygit2.__version__}; "
            f"Libgit2 v{pygit2.LIBGIT2_VERSION}; "
            f"Git v{git_version}\n"
            f"» SQLAlchemy v{sqlalchemy.__version__}; "
            f"pysqlite3 v{pysqlite3.version}/v{pysqlite3.sqlite_version}; "
            f"SpatiaLite v{spatialite_version}; "
            f"Libpq v{pq_version}\n"
            f"» SpatialIndex v{sidx_version}"
        )
    )

    ctx.exit()


class SnoGroup(click.Group):
    def get_command(self, ctx, cmd_name):
        rv = super().get_command(ctx, cmd_name)
        if rv is not None:
            return rv

        # typo? Suggest similar commands.
        import difflib

        matches = difflib.get_close_matches(
            cmd_name, list(self.list_commands(ctx)), n=3
        )

        fail_message = f"sno: '{cmd_name}' is not a sno command. See 'sno --help'.\n"
        if matches:
            if len(matches) == 1:
                fail_message += "\nThe most similar command is\n"
            else:
                fail_message += "\nThe most similar commands are\n"
            for m in matches:
                fail_message += f"\t{m}\n"
        ctx.fail(fail_message)

    def invoke(self, ctx):
        try:
            import ipdb as pdb
        except ImportError:
            # ipdb is only installed in dev venvs, not releases
            import pdb

        if ctx.params.get("post_mortem"):
            try:
                return super().invoke(ctx)
            except Exception:
                pdb.post_mortem()
                raise
        else:
            return super().invoke(ctx)


@add_help_subcommand
@click.group(cls=SnoGroup)
@click.option(
    "-C",
    "--repo",
    "repo_dir",
    type=click.Path(file_okay=False, dir_okay=True),
    default=None,
    metavar="PATH",
)
@call_and_exit_flag(
    "--version",
    callback=print_version,
    help="Show version information and exit.",
)
@click.option("-v", "--verbose", count=True, help="Repeat for more verbosity")
# NOTE: this option isn't used in `cli`, but it is used in `PdbGroup` above.
@click.option(
    "--post-mortem",
    is_flag=True,
    hidden=True,
    help="Interactively debug uncaught exceptions",
)
@click.pass_context
def cli(ctx, repo_dir, verbose, post_mortem):
    ctx.ensure_object(Context)
    if repo_dir:
        ctx.obj.user_repo_path = repo_dir

    # default == WARNING; -v == INFO; -vv == DEBUG
    ctx.obj.verbosity = verbose
    log_level = logging.WARNING - min(10 * verbose, 20)
    logging.basicConfig(level=log_level)


# Commands from modules:
cli.add_command(apply.apply)
cli.add_command(branch.branch)
cli.add_command(checkout.checkout)
cli.add_command(checkout.create_workingcopy)
cli.add_command(checkout.reset)
cli.add_command(checkout.restore)
cli.add_command(checkout.switch)
cli.add_command(clone.clone)
cli.add_command(conflicts.conflicts)
cli.add_command(commit.commit)
cli.add_command(data.data)
cli.add_command(diff.diff)
cli.add_command(fsck.fsck)
cli.add_command(init.import_table)
cli.add_command(init.init)
cli.add_command(log.log)
cli.add_command(merge.merge)
cli.add_command(meta.commit_files)
cli.add_command(meta.meta)
cli.add_command(pull.pull)
cli.add_command(resolve.resolve)
cli.add_command(show.create_patch)
cli.add_command(show.show)
cli.add_command(status.status)
cli.add_command(query.query)
cli.add_command(upgrade.upgrade)
cli.add_command(upgrade.upgrade_to_tidy)


# straight process-replace commands


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.option(
    "--progress/--quiet",
    "do_progress",
    is_flag=True,
    default=True,
    help="Whether to report progress to stderr",
)
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def push(ctx, do_progress, args):
    """ Update remote refs along with associated objects """
    execvpe(
        "git",
        [
            "git",
            "-C",
            ctx.obj.repo.path,
            "push",
            "--progress" if do_progress else "--quiet",
            *args,
        ],
        git_remote_environment(),
    )


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.option(
    "--progress/--quiet",
    "do_progress",
    is_flag=True,
    default=True,
    help="Whether to report progress to stderr",
)
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def fetch(ctx, do_progress, args):
    """ Download objects and refs from another repository """
    execvpe(
        "git",
        [
            "git",
            "-C",
            ctx.obj.repo.path,
            "fetch",
            "--progress" if do_progress else "--quiet",
            *args,
        ],
        git_remote_environment(),
    )


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
def reflog(ctx, args):
    """ Manage reflog information """
    execvp("git", ["git", "-C", ctx.obj.repo.path, "reflog"] + list(args))


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def config(ctx, args):
    """ Get and set repository or global options """
    startup_load_git_init_config()
    params = ["git", "config"]
    if ctx.obj.user_repo_path:
        params[1:1] = ["-C", ctx.obj.user_repo_path]
    execvp("git", params + list(args))


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def gc(ctx, args):
    """ Cleanup unnecessary files and optimize the local repository """
    params = ["git", "gc"]
    if ctx.obj.user_repo_path:
        params[1:1] = ["-C", ctx.obj.user_repo_path]
    execvp("git", params + list(args))


if __name__ == "__main__":
    cli()
