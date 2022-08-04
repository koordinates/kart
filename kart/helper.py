from pathlib import Path
import socket
import signal
import json
import io
import os
import sys

import click

from .socket_utils import recv_fds
from .cli import load_commands_from_args, cli, is_windows, is_darwin, is_linux


@click.command(context_settings=dict(ignore_unknown_options=True))
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

    kart_helper_pid = os.environ.get("KART_HELPER_PID")
    if kart_helper_pid:
        click.echo(
            "Helper mode not available when already running as helper"
            f", existing helper PID {kart_helper_pid}"
        )
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

                kart_helper_log = os.environ.get("KART_HELPER_LOG")
                if kart_helper_log:
                    kart_helper_log = open(kart_helper_log, "a")
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
                    kart_helper_log.write(
                        f"PID={calling_environment['pid']} CWD={os.getcwd()} CMD={' '.join(calling_environment['argv'])}\n"
                    )
                    os.environ.clear()
                    os.environ.update(
                        {
                            **calling_environment["environ"],
                            **required_environment,
                            "KART_HELPER_PID": str(os.getppid()),
                        }
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
