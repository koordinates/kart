#!/usr/bin/env python3
import importlib
import importlib.util
import inspect
import logging
import os
import pathlib
import re
import subprocess
import sys
import traceback
from pathlib import Path

import click
import pygit2

from . import core, is_darwin, is_linux, is_windows  # noqa
from .cli_util import (
    add_help_subcommand,
    call_and_exit_flag,
    tool_environment,
    KartGroup,
)
from .context import Context
from kart.parse_args import PreserveDoubleDash
from kart.subprocess_util import run

MODULE_COMMANDS = {
    "annotations.cli": {"build-annotations"},
    "apply": {"apply"},
    "branch": {"branch"},
    "checkout": {"checkout", "reset", "restore", "switch"},
    "clone": {"clone"},
    "conflicts": {"conflicts"},
    "commit": {"commit"},
    "create_workingcopy": {"create-workingcopy"},
    "data": {"data"},
    "diff": {"diff"},
    "fsck": {"fsck"},
    "helper": {"helper"},
    "init": {"init"},
    "lfs_commands": {"lfs+"},
    "log": {"log"},
    "merge": {"merge"},
    "meta": {"commit-files", "meta"},
    "pull": {"pull"},
    "resolve": {"resolve"},
    "show": {"create-patch", "show"},
    "spatial_filter": {"spatial-filter"},
    "status": {"status"},
    "upgrade": {"upgrade"},
    "tabular.import_": {"import"},
    "point_cloud.import_": {"point-cloud-import"},
    "install": {"install"},
}

# These commands aren't valid Python symbols, even when we change dash to underscore.
COMMAND_TO_FUNCTION_NAME = {
    "import": "import_",
    "lfs+": "lfs_plus",
}


def _load_commands_from_module(mod_name):
    mod = importlib.import_module(f".{mod_name}", "kart")
    for k in MODULE_COMMANDS[mod_name]:
        k = COMMAND_TO_FUNCTION_NAME.get(k) or k.replace("-", "_")
        command = getattr(mod, k)
        cli.add_command(command)


def load_all_commands():
    for mod in MODULE_COMMANDS:
        _load_commands_from_module(mod)


def get_version():
    import kart

    with open(Path(kart.package_data_path) / "VERSION") as version_file:
        return version_file.read().strip()


def get_version_tuple():
    return tuple(get_version().split("."))


def print_version(ctx):
    import osgeo
    import psycopg2
    import pysqlite3

    import sqlalchemy
    from kart.sqlalchemy.gpkg import Db_GPKG

    click.echo(f"Kart v{get_version()}, Copyright (c) Kart Contributors")

    git_version = (
        subprocess.check_output(["git", "--version"], env=tool_environment())
        .decode("ascii")
        .strip()
        .split()[-1]
    )

    gitlfs_version = re.match(
        r"git-lfs/([^ ]+) \(",
        subprocess.check_output(
            ["git-lfs", "version"], env=tool_environment(), text=True
        ),
    ).group(1)

    pdal_version = (
        subprocess.check_output(["pdal", "--version"], env=tool_environment())
        .decode("ascii")
        .strip()
        .split()[2]
    )

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
            f"PROJ v{proj_version}; "
            f"PDAL v{pdal_version}\n"
            f"» PyGit2 v{pygit2.__version__}; "
            f"Libgit2 v{pygit2.LIBGIT2_VERSION}; "
            f"Git v{git_version}; "
            f"Git LFS v{gitlfs_version}\n"
            f"» SQLAlchemy v{sqlalchemy.__version__}; "
            f"pysqlite3 v{pysqlite3.version}/v{pysqlite3.sqlite_version}; "
            f"SpatiaLite v{spatialite_version}; "
            f"Libpq v{pq_version}"
        )
    )

    # report on whether this was run through helper mode
    helper_pid = os.environ.get("KART_HELPER_PID")
    if helper_pid:
        click.echo(f"Executed via helper, PID: {helper_pid}")

    ctx.exit()


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
    """Update remote refs along with associated objects"""
    ctx.invoke(
        git,
        args=[
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
    """Download objects and refs from another repository"""
    ctx.invoke(
        git,
        args=[
            "fetch",
            "--progress" if do_progress else "--quiet",
            *args,
        ],
    )


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def remote(ctx, args):
    """Manage set of tracked repositories"""
    ctx.invoke(git, args=["remote", *args])


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def tag(ctx, args):
    """Create, list, delete or verify a tag object signed with GPG"""
    ctx.invoke(git, args=["tag", *args])


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def reflog(ctx, args):
    """Manage reflog information"""
    ctx.invoke(git, args=["reflog", *args])


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def config(ctx, args):
    """Get and set repository or global options"""
    ctx.invoke(git, args=["config", *args])


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def gc(ctx, args):
    """Cleanup unnecessary files and optimize the local repository"""
    ctx.invoke(git, args=["gc", *args])


@cli.command(
    context_settings=dict(ignore_unknown_options=True),
    hidden=True,
    cls=PreserveDoubleDash,
)
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def git(ctx, args):
    """
    Run an arbitrary Git command, using kart's packaged Git
    """
    params = []
    if ctx.obj.user_repo_path:
        params += ["-C", ctx.obj.user_repo_path]
    run("git", [*params, *args])


@cli.command(context_settings=dict(ignore_unknown_options=True), hidden=True)
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def lfs(ctx, args):
    """
    Run an arbitrary Git LFS command, using Kart's packaged Git.
    Git LFS is not yet packaged with Kart so this will not work unless your Kart environment has Git LFS installed.
    """
    params = []
    if ctx.obj.user_repo_path:
        params += ["-C", ctx.obj.user_repo_path]
    params += ["lfs"]
    run("git", [*params, *args])


@cli.command(
    name="ext-run", context_settings=dict(ignore_unknown_options=True), hidden=True
)
@click.pass_context
@click.argument("script", type=click.Path(exists=True, dir_okay=False), required=True)
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def ext_run(ctx, script, args):
    """
    Invoke a main(ctx, args) function from an arbitrary Python script inside the
    Kart execution environment. There is no public API for Kart.
    """
    script = pathlib.Path(script)
    # strip all the suffixes
    module_name = "kart.ext_run." + script.name.split(".", 1)[0]

    def _format_exc_tb():
        # print a traceback from the ext module down
        extracts = traceback.extract_tb(sys.exc_info()[2])
        count = len(extracts)
        # find the first occurrence of the module file name
        for i, extract in enumerate(extracts):
            if extract[0] == script.name:
                break
            count -= 1
        # keep only the count of last lines
        return traceback.format_exc(limit=-count)

    spec = importlib.util.spec_from_file_location(module_name, script)
    module = importlib.util.module_from_spec(spec)

    # add script directory to sys.path so the extension can do imports
    script_dir = script.resolve().parent
    sys.path.append(str(script_dir))

    try:
        spec.loader.exec_module(module)
    except Exception:
        raise click.ClickException(f"loading {script}\n\n{_format_exc_tb()}")

    if not hasattr(module, "main") or not callable(module.main):
        raise click.ClickException(f"{script} does not have a main(ctx, args) function")

    try:
        f_sig = inspect.signature(module.main)
        f_sig.bind(ctx=ctx, args=args)
    except TypeError:
        raise click.ClickException(f"{script} requires a main(ctx, args) function")

    return module.main(ctx=ctx, args=args)


def _hackily_parse_command(args, skip_first_arg=True):
    ignore_next = skip_first_arg
    for arg in args:
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


def load_commands_from_args(args, skip_first_arg=True):
    command = _hackily_parse_command(args, skip_first_arg=skip_first_arg)
    if command == "help":
        load_all_commands()
    elif command not in cli.commands:
        for mod, commands in MODULE_COMMANDS.items():
            if command in commands:
                _load_commands_from_module(mod)
                break
        else:
            load_all_commands()


def entrypoint():
    load_commands_from_args(sys.argv)
    cli()


if __name__ == "__main__":
    entrypoint()
