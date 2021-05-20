#!/usr/bin/env python3
import importlib
import logging
import os
import re
import sys
import subprocess

import click
import pygit2

from . import core  # noqa
from .cli_util import (
    add_help_subcommand,
    call_and_exit_flag,
    tool_environment,
)
from .context import Context
from .exec import execvp

MODULE_COMMANDS = {
    "annotations.cli": {"build-annotations"},
    "apply": {"apply"},
    "branch": {"branch"},
    "checkout": {"checkout", "create-workingcopy", "reset", "restore", "switch"},
    "clone": {"clone"},
    "conflicts": {"conflicts"},
    "commit": {"commit"},
    "data": {"data"},
    "diff": {"diff"},
    "fsck": {"fsck"},
    "init": {"import", "init"},
    "log": {"log"},
    "merge": {"merge"},
    "meta": {"commit-files", "meta"},
    "pull": {"pull"},
    "resolve": {"resolve"},
    "show": {"create-patch", "show"},
    "status": {"status"},
    "query": {"query"},
    "upgrade": {"upgrade", "upgrade-to-tidy", "upgrade-to-kart"},
}


def _load_commands_from_module(mod_name):
    mod = importlib.import_module(f".{mod_name}", "kart")
    for k in MODULE_COMMANDS[mod_name]:
        k = k.replace("-", "_")
        if k == "import":
            # a special case
            k = "import_"
        command = getattr(mod, k)
        cli.add_command(command)


def _load_all_commands():
    for mod in MODULE_COMMANDS:
        _load_commands_from_module(mod)


def get_version():
    import kart

    with open(os.path.join(os.path.split(kart.__file__)[0], "VERSION")) as version_file:
        return version_file.read().strip()


def print_version(ctx):
    import osgeo
    import psycopg2
    import pysqlite3
    import rtree
    import sqlalchemy

    from kart.sqlalchemy.gpkg import Db_GPKG

    click.echo(f"Kart v{get_version()}, Copyright (c) Kart Contributors")

    git_version = (
        subprocess.check_output(["git", "--version"], env=tool_environment())
        .decode("ascii")
        .strip()
        .split()[-1]
    )

    sidx_version = rtree.index.__c_api_version__.decode("ascii")

    engine = Db_GPKG.create_engine(":memory:")
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


class KartGroup(click.Group):
    def get_command(self, ctx, cmd_name):
        rv = super().get_command(ctx, cmd_name)
        if rv is not None:
            return rv

        # typo? Suggest similar commands.
        import difflib

        matches = difflib.get_close_matches(
            cmd_name, list(self.list_commands(ctx)), n=3
        )

        fail_message = f"kart: '{cmd_name}' is not a kart command. See 'kart --help'.\n"
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
@click.group(cls=KartGroup)
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
    if verbose >= 2:
        fmt = "%(asctime)s T%(thread)d %(levelname)s %(name)s [%(filename)s:%(lineno)d] - %(message)s"
    else:
        fmt = "%(asctime)s %(levelname)s %(name)s - %(message)s"
    logging.basicConfig(level=log_level, format=fmt)

    if verbose >= 3:
        # enable SQLAlchemy query logging
        logging.getLogger("sqlalchemy.engine").setLevel("INFO")


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
    execvp(
        "git",
        [
            "git",
            "-C",
            ctx.obj.repo.path,
            "push",
            "--progress" if do_progress else "--quiet",
            *args,
        ],
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
    execvp(
        "git",
        [
            "git",
            "-C",
            ctx.obj.repo.path,
            "fetch",
            "--progress" if do_progress else "--quiet",
            *args,
        ],
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


def _hackily_parse_command(args):
    ignore_next = False
    for arg in args[1:]:
        if ignore_next:
            ignore_next = False
            continue
        if arg == "--help":
            return "help"
        elif arg.startswith("-"):
            if arg in ("--repo", "-C"):
                ignore_next = True
                continue
        else:
            return arg


def load_commands_from_args(args):
    command = _hackily_parse_command(args)
    if command == "help":
        _load_all_commands()
    elif command not in cli.commands:
        for mod, commands in MODULE_COMMANDS.items():
            if command in commands:
                _load_commands_from_module(mod)
                break
        else:
            _load_all_commands()


def entrypoint():
    load_commands_from_args(sys.argv)
    cli()


if __name__ == "__main__":
    entrypoint()
