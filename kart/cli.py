#!/usr/bin/env python3
import importlib
import importlib.util
import inspect
import io
import logging
import json
import os
import pathlib
import re
import signal
import subprocess
import sys
import traceback
import socket
from typing import IO
from .socket_utils import recv_fds
import time
from pathlib import Path

import click
import pygit2

from . import core, is_darwin, is_linux, is_windows  # noqa
from .cli_util import add_help_subcommand, call_and_exit_flag, tool_environment
from .context import Context
from .exec import run_and_wait
from kart.completion import Shells, install_callback

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
    "init": {"init"},
    "log": {"log"},
    "merge": {"merge"},
    "meta": {"commit-files", "meta"},
    "pull": {"pull"},
    "resolve": {"resolve"},
    "show": {"create-patch", "show"},
    "spatial_filter": {"spatial-filter"},
    "status": {"status"},
    "query": {"query"},
    "upgrade": {"upgrade"},
    "tabular.import_": {"import"},
    "point_cloud.import_": {"point-cloud-import"},
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


def get_version_tuple():
    return tuple(get_version().split("."))


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

    # report on whether this was run through helper mode
    helper_pid = os.environ.get('KART_HELPER_PID')
    if helper_pid:
        click.echo(f"Executed via helper, PID: {helper_pid}")

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
        if ctx.params.get("post_mortem"):
            try:
                return super().invoke(ctx)
            except Exception:
                try:
                    import ipdb as pdb
                except ImportError:
                    # ipdb is only installed in dev venvs, not releases
                    import pdb
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
    "--socket",
    "socket_filename",
    default=Path.home() / ".kart.socket",
    show_default=True,
    help="What socket to use",
)
@click.option(
    "--timeout",
    "timeout",
    default=300,
    show_default=True,
    help="Timeout and shutdown helper when no commands received with this time",
)
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def helper(ctx, socket_filename, timeout, args):
    """Start the background helper process to speed up interaction"""
    if is_windows:
        click.echo("Helper mode not currently supported on Windows")
        ctx.exit(1)

    load_commands_from_args(["--help"])

    # stash the required environment for used libs
    # TODO - this should be checked to ensure it is all that is needed
    #  is there anything beyond these which is needed, eg. PWD, USER, etc.
    required_environment = {
        k: v
        for k, v in os.environ.items()
        if k
        in [
            "PATH",
            "LD_LIBRARY_PATH",
            "LD_LIBRARY_PATH_ORIG",
            "SPATIALINDEX_C_LIBRARY",
            "GIT_CONFIG_NOSYSTEM",
            "GIT_EXEC_PATH",
            "GIT_TEMPLATE_DIR",
            "GIT_INDEX_FILE",
            "GDAL_DATA",
            "PROJ_LIB",
            "PROJ_NETWORK",
            "OGR_SQLITE_PRAGMA",
            "XDG_CONFIG_HOME",
            "GIT_EXEC_PATH",
            "GIT_TEMPLATE_DIR",
            "GIT_INDEX_FILE",
            "SSL_CERT_FILE",
        ]
    }

    sock = socket.socket(family=socket.AF_UNIX)
    os.umask(0o077)
    try:
        # TODO - this will take over a socket from an existing running helper process
        #  it should check to see if another helper is already running and bail out
        if os.path.exists(socket_filename):
            os.unlink(socket_filename)

        sock.bind(str(socket_filename))
    except OSError as e:
        print(dir(e))
        click.echo(f"Unable to bind to socket [{socket_filename}] [{e.strerror}]")
        ctx.exit(1)

    sock.listen()

    # ignore SIGCHLD so zombies don't remain when the child is complete
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)

    # import modules that are deferred loaded in normal kart execution
    from .tabular.working_copy.gpkg import WorkingCopy_GPKG
    import kart
    import osgeo
    import psycopg2
    import pysqlite3
    import rtree

    import sqlalchemy
    from kart.sqlalchemy.gpkg import Db_GPKG
    import pygments.token as token
    from pygments.formatters import TerminalFormatter
    import pygments.styles.default

    while True:
        # The helper will exit if no command received within timeout
        sock.settimeout(timeout)
        try:
            client, info = sock.accept()
            if os.fork() == 0:
                payload, fds = recv_fds(client, 8164, 4)
                # print("kart helper: handlng request...", payload)
                # TODO - what logging support should we have for the helper, to stdout doesn't work
                #  well as it will be starting in the background and output will show, log file?
                if not payload or len(fds) != 4:
                    click.echo("No payload or fds passed from kart_cli_helper")
                    sys.exit(-1)

                kart_helper_log = os.environ.get('KART_HELPER_LOG')
                if kart_helper_log:
                    kart_helper_log = open(kart_helper_log, 'a')
                else:
                    kart_helper_log = io.StringIO()

                # as there is a new process the child could drop permissions here or use a security system to set up
                # controls, chroot etc.

                # change to the calling processes working directory
                # TODO - pass as path for windows
                os.fchdir(fds[3])

                # set this processes stdin/stdout/stderr to the calling processes passed in fds
                # TODO - have these passed as named pipes paths, will work on windows as well
                

                # 0,1,2 are the wrong places since they were closed before the helper was attached
                
                sys.stdin = os.fdopen(fds[0], "r")
                sys.stdout = os.fdopen(fds[1], "w")
                sys.stderr = os.fdopen(fds[2], "w")

                try:
                    calling_environment = json.loads(payload)
                except (TypeError, ValueError) as e:
                    click.echo(
                        "kart helper: Unable to read command from kart_cli_helper", e
                    )
                else:
                    sys.argv[1:] = calling_environment["argv"][1:]
                    kart_helper_log.write(f"PID={calling_environment['pid']} CWD={os.getcwd()} CMD={' '.join(calling_environment['argv'])}\n")
                    os.environ.clear()
                    os.environ.update(
                        {**calling_environment["environ"], **required_environment, 'KART_HELPER_PID': str(os.getppid())}
                    )

                    import ctypes
                    import ctypes.util

                    libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
                    # 
                    if is_darwin:
                        SETVAL = 8
                    elif is_linux:
                        SETVAL = 16

                    class struct_semid_ds(ctypes.Structure):
                        pass

                    class struct_semun(ctypes.Union):
                        _fields_ = [
                            ("val", ctypes.c_uint32),
                            (
                                "buf",
                                ctypes.POINTER(struct_semid_ds),
                            ),
                            (
                                "array",
                                ctypes.POINTER(ctypes.c_uint16),
                            ),
                        ]

                    libc.semctl.argtypes = [
                        ctypes.c_int,
                        ctypes.c_int,
                        ctypes.c_int,
                        struct_semun,
                    ]
                    libc.semget.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_int]
                    semid = calling_environment["semid"]
                    try:
                        cli()
                    except SystemExit as system_exit:
                        """exit is called in the commands but we ignore as we need to clean up the caller"""

                        # TODO - do we ever see negative exit codes from cli (git etc)?
                        libc.semctl(
                            semid, 0, SETVAL, struct_semun(val=system_exit.code + 1000)
                        )

                    except Exception as e:
                        traceback.print_exc(file=sys.stdout)
                        print(
                            f"kart helper: unhandled exception [{e}]"
                        )  # TODO - should ext-run capture/handle this?
                        libc.semctl(semid, 0, SETVAL, struct_semun(val=1001))
                    try:
                        # send a signal to caller that we are done
                        os.kill(calling_environment["pid"], signal.SIGALRM)
                    except ProcessLookupError as e:
                        pass
                
                kart_helper_log.close()
                sys.exit()

        except socket.timeout:
            try:
                os.unlink(socket_filename)
            except FileNotFoundError:
                """already unlinked???"""
            sys.exit()


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
@click.option(
    "--install-tab-completion",
    type=click.Choice([s.value for s in Shells] + ["auto"]),
    callback=install_callback,
    expose_value=False,
    help="Install tab completion for the specific or current shell",
)
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


@cli.command(context_settings=dict(ignore_unknown_options=True), hidden=True)
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def git(ctx, args):
    """
    Run an arbitrary Git command, using kart's packaged Git
    """
    params = ["git"]
    if ctx.obj.user_repo_path:
        params += ["-C", ctx.obj.user_repo_path]
    run_and_wait("git", [*params, *args])


@cli.command(context_settings=dict(ignore_unknown_options=True), hidden=True)
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def lfs(ctx, args):
    """
    Run an arbitrary Git LFS command, using Kart's packaged Git.
    Git LFS is not yet packaged with Kart so this will not work unless your Kart environment has Git LFS installed.
    """
    params = ["git"]
    if ctx.obj.user_repo_path:
        params += ["-C", ctx.obj.user_repo_path]
    params += ["lfs"]
    run_and_wait("git", [*params, *args])


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
