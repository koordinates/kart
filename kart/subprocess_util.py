import asyncio
import os
import subprocess
import sys
from asyncio import IncompleteReadError, LimitOverrunError
from asyncio.subprocess import PIPE
from functools import partial

from . import is_windows
from .cli_util import tool_environment


async def read_stream_and_display(stream, display):
    """Read from stream line by line until EOF, display, and capture the lines."""
    output = []
    while True:
        line = await read_universal_line(stream)
        if not line:
            break
        output.append(line)
        display(line)  # assume it doesn't block
    return b"".join(output)


async def read_and_display(cmd, **kwargs):
    """Capture cmd's stdout and stderr while displaying them as they arrive (line by line)."""
    # start process
    process = await asyncio.create_subprocess_exec(
        *cmd, stdout=PIPE, stderr=PIPE, **kwargs
    )

    def display(stream, output):
        stream.buffer.write(output)
        stream.flush()

    # Read child's stdout/stderr concurrently (capture and display)
    try:
        stdout, stderr = await asyncio.gather(
            read_stream_and_display(process.stdout, partial(display, sys.stdout)),
            read_stream_and_display(process.stderr, partial(display, sys.stderr)),
        )
    except Exception:
        process.kill()
        raise
    finally:
        # Wait for the process to exit
        return_code = await process.wait()
    return return_code, stdout, stderr


async def read_universal_line(stream):
    """Read chunk of data from the stream until a newline char '\r' or '\n' is found."""
    separators = b"\r\n"
    try:
        line = await read_until_any_of(stream, separators)
    except IncompleteReadError as e:
        return e.partial
    except LimitOverrunError as e:
        if stream._buffer[e.consumed] in separators:
            del stream._buffer[: e.consumed + 1]
        else:
            stream._buffer.clear()
        stream._maybe_resume_transport()
        raise ValueError(e.args[0])
    return line


async def read_until_any_of(stream, separators=b"\n"):
    """Read data from the stream until any of the separator chars are found."""
    if len(separators) < 1:
        raise ValueError("separators should be at least one-byte string")

    if stream._exception is not None:
        raise stream._exception

    offset = 0

    # Loop until we find `separator` in the buffer, exceed the buffer size,
    # or an EOF has happened.
    while True:
        buflen = len(stream._buffer)

        # Check if we now have enough data in the buffer for `separator` to
        # fit.
        if buflen - offset >= 1:
            isep = min(
                (
                    i
                    for i in (stream._buffer.find(s, offset) for s in separators)
                    if i >= 0
                ),
                default=-1,
            )

            if isep != -1:
                # `separator` is in the buffer. `isep` will be used later to retrieve the data.
                break

            offset = buflen
            if offset > stream._limit:
                raise LimitOverrunError(
                    "Separator is not found, and chunk exceed the limit", offset
                )

        # Complete message (with full separator) may be present in buffer
        # even when EOF flag is set. This may happen when the last chunk
        # adds data which makes separator be found. That's why we check for
        # EOF *ater* inspecting the buffer.
        if stream._eof:
            chunk = bytes(stream._buffer)
            stream._buffer.clear()
            raise IncompleteReadError(chunk, None)

        # _wait_for_data() will resume reading if stream was paused.
        await stream._wait_for_data("readuntil")

    if isep > stream._limit:
        raise LimitOverrunError(
            "Separator is found, but chunk is longer than limit", isep
        )

    chunk = stream._buffer[: isep + 1]
    del stream._buffer[: isep + 1]
    stream._maybe_resume_transport()
    return bytes(chunk)


def subprocess_tee(cmd, **kwargs):
    """
    Run a subprocess and *don't* capture its output - let stdout and stderr display as per usual -
    - but also *do* capture its output so that we can inspect it.
    Returns a tuple of (exit-code, stdout output string, stderr output string).
    """
    if is_windows and not isinstance(
        asyncio.get_event_loop(), asyncio.ProactorEventLoop
    ):
        loop = asyncio.ProactorEventLoop()  # for subprocess' pipes on Windows
        asyncio.set_event_loop(loop)

    return_code, stdout, stderr = asyncio.run(read_and_display(cmd, **kwargs))
    return return_code, stdout, stderr


def run_with_capture(cmd, args, env):
    # In testing, .run must be set to capture_output and so use PIPEs to communicate
    # with the process to run whereas in normal operation the standard streams of
    # this process are passed into subprocess.run.
    # Capturing the output in a PIPE and then writing to sys.stdout is compatible
    # with click.testing which sets sys.stdout and sys.stderr to a custom
    # io wrapper.
    # This io wrapper is not compatible with the stdin= kwarg to .run - in that case
    # it gets treated as a file like object and fails.
    p = subprocess.run([cmd] + args, capture_output=True, encoding="utf-8", env=env)
    sys.stdout.write(p.stdout)
    sys.stdout.flush()
    sys.stderr.write(p.stderr)
    sys.stderr.flush()
    sys.exit(p.returncode)


def run(cmd, args):
    """
    Run a process and wait for it to exit, this is required
    when in helper mode as using execvpe overwrites the process so
    the caller can't be notified when the command is complete.

    The subprocess uses this processes standard streams.

    If called in test then use capture mode rather than passing in 'real' standard
    streams.
    """
    env = tool_environment(os.environ)
    if "_KART_RUN_WITH_CAPTURE" in os.environ:
        run_with_capture(cmd, args, env)
    else:
        p = subprocess.run(
            [cmd] + args,
            encoding="utf-8",
            env=env,
            stdin=sys.stdin,
            stdout=sys.stdout,
            stderr=sys.stderr,
        )
        sys.exit(p.returncode)
