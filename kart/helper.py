import errno
import json
import os
import signal
import socket
import sys
import traceback
from datetime import datetime
from pathlib import Path

import click

from .socket_utils import recv_json_and_fds
from .cli import load_commands_from_args, cli, is_windows, is_darwin, is_linux


log_filename = None


def _helper_log(msg):
    if log_filename:
        with open(log_filename, "at") as log_file:
            log_file.write(f"{datetime.now()} [{os.getpid()}]: {msg}\n")


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

    if os.environ.get("KART_HELPER_LOG"):
        global log_filename
        log_filename = os.path.abspath(os.environ["KART_HELPER_LOG"])

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
            "KART_HELPER_LOG",
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
        _helper_log(f"Bound socket: {socket_filename}")
    except OSError as e:
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

    import sqlalchemy
    from kart.sqlalchemy.gpkg import Db_GPKG
    import pygments.token as token
    from pygments.formatters import TerminalFormatter
    import pygments.styles.default

    while True:
        # The helper will exit if no command received within timeout
        sock.settimeout(timeout)
        try:
            _helper_log(f"socket ready, waiting for messages (timeout={timeout})")
            client, info = sock.accept()
            _helper_log("pre-fork messaged received")
            if os.fork() != 0:
                # parent
                continue
            else:
                # child
                _helper_log("post-fork")

                payload, fds = recv_json_and_fds(client, maxfds=4)
                if not payload or len(fds) != 4:
                    click.echo(
                        "No payload or fds passed from kart_cli_helper: exit(-1)"
                    )
                    sys.exit(-1)

                # as there is a new process the child could drop permissions here or use a security system to set up
                # controls, chroot etc.

                # change to the calling processes working directory
                # TODO - pass as path for windows
                os.fchdir(fds[3])
                _helper_log(f"cwd={os.getcwd()}")

                # set this processes stdin/stdout/stderr to the calling processes passed in fds
                # TODO - have these passed as named pipes paths, will work on windows as well

                # 0,1,2 are the wrong places since they were closed before the helper was attached

                sys.stdin = os.fdopen(fds[0], "r")
                sys.stdout = os.fdopen(fds[1], "w")
                sys.stderr = os.fdopen(fds[2], "w")

                # re-enable SIGCHLD so subprocess handling works
                signal.signal(signal.SIGCHLD, signal.SIG_DFL)

                try:
                    calling_environment = json.loads(payload)
                except (TypeError, ValueError, json.decoder.JSONDecodeError) as e:
                    raise RuntimeError(
                        "kart helper: Unable to read command from kart_cli_helper",
                        e,
                        f"Payload:\n{repr(payload)}",
                    )
                else:
                    try:
                        # Join the process group of the calling process - so that if they get killed, we get killed to.
                        os.setpgid(0, calling_environment["pid"])
                        os.environ["_KART_PGID_SET"] = "1"
                    except Exception as e:
                        # Kart will still work even if this fails: it just means SIGINT Ctrl+C might not work properly.
                        # We'll just log it and hope for the best.
                        _helper_log(f"error joining caller's process group: {e}")
                        pass

                    sys.argv[1:] = calling_environment["argv"][1:]
                    _helper_log(f"cmd={' '.join(calling_environment['argv'])}")
                    os.environ.clear()
                    os.environ.update(
                        {
                            **calling_environment["environ"],
                            **required_environment,
                            "KART_HELPER_PID": str(os.getppid()),
                        }
                    )

                    # setup the semctl() function
                    import ctypes
                    import ctypes.util

                    libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
                    if is_darwin:
                        SETVAL = 8

                        #  union semun {
                        #          int     val;            /* value for SETVAL */
                        #          struct  semid_ds *buf;  /* buffer for IPC_STAT & IPC_SET */
                        #          u_short *array;         /* array for GETALL & SETALL */
                        #  };
                        class C_SEMUN(ctypes.Union):
                            _fields_ = (
                                ("val", ctypes.c_int),
                                ("semid_ds", ctypes.c_void_p),
                                ("array", ctypes.POINTER(ctypes.c_ushort)),
                            )

                    elif is_linux:
                        SETVAL = 16

                        # union semun {
                        #     int              val;    /* Value for SETVAL */
                        #     struct semid_ds *buf;    /* Buffer for IPC_STAT, IPC_SET */
                        #     unsigned short  *array;  /* Array for GETALL, SETALL */
                        #     struct seminfo  *__buf;  /* Buffer for IPC_INFO
                        # };
                        class C_SEMUN(ctypes.Union):
                            _fields_ = (
                                ("val", ctypes.c_int),
                                ("semid_ds", ctypes.c_void_p),
                                ("array", ctypes.POINTER(ctypes.c_ushort)),
                                ("seminfo", ctypes.c_void_p),
                            )

                    # arg (union semun) is a variadic arg. On macOS/arm64 the
                    # calling convention differs between fixed & variadic args
                    # so we _must_ treat it as variadic.
                    libc.semctl.argtypes = (
                        ctypes.c_int,
                        ctypes.c_int,
                        ctypes.c_int,
                    )
                    libc.semctl.restype = ctypes.c_int

                    semid = calling_environment["semid"]
                    SEMNUM = 0

                    _helper_log(f"semid={semid}")
                    try:
                        _helper_log("invoking cli()...")
                        # Don't let helper mode mess up the usage-text, or the shell complete environment variables.
                        prog_name = (
                            "kart"
                            if os.path.basename(sys.argv[0]) == "kart_cli"
                            else None
                        )
                        cli(prog_name=prog_name, complete_var="_KART_COMPLETE")
                    except SystemExit as system_exit:
                        """exit is called in the commands but we ignore as we need to clean up the caller"""
                        # TODO - do we ever see negative exit codes from cli (git etc)?
                        _helper_log(
                            f"SystemExit from cli(): {system_exit.code} semval={1000+system_exit.code}"
                        )
                        if (
                            libc.semctl(
                                semid,
                                SEMNUM,
                                SETVAL,
                                C_SEMUN(val=system_exit.code + 1000),
                            )
                            < 0
                        ):
                            raise RuntimeError(
                                f"Error setting semid {semid}[{SEMNUM}]=1000+{system_exit.code}: "
                                f"{errno.errorcode.get(ctypes.get_errno(), ctypes.get_errno())}"
                            )
                    except Exception:
                        # TODO - should ext-run capture/handle this?
                        _helper_log(
                            f"unhandled exception from cli() semval=1001: {traceback.format_exc()}"
                        )
                        print("kart helper: unhandled exception", file=sys.stderr)
                        traceback.print_exc(file=sys.stderr)
                        if libc.semctl(semid, SEMNUM, SETVAL, C_SEMUN(val=1001)) < 0:
                            raise RuntimeError(
                                f"Error setting semid {semid}[{SEMNUM}]=1001: "
                                f"{errno.errorcode.get(ctypes.get_errno(), ctypes.get_errno())}"
                            )
                    else:
                        _helper_log("return from cli() without SystemExit semval=1000")
                        if libc.semctl(semid, SEMNUM, SETVAL, C_SEMUN(val=1000)) < 0:
                            raise RuntimeError(
                                f"Error setting semid {semid}[{SEMNUM}]=1000: "
                                f"{errno.errorcode.get(ctypes.get_errno(), ctypes.get_errno())}"
                            )

                    try:
                        # send a signal to caller that we are done
                        _helper_log(
                            f"sending SIGALRM to pid {calling_environment['pid']}"
                        )
                        os.kill(calling_environment["pid"], signal.SIGALRM)
                    except ProcessLookupError as e:
                        _helper_log(f"error signalling caller: {e}")
                        pass

                _helper_log("bye(0)")
                sys.exit()

        except socket.timeout:
            _helper_log("socket timeout, bye (0)")
            try:
                os.unlink(socket_filename)
            except FileNotFoundError:
                """already unlinked???"""
            sys.exit()
