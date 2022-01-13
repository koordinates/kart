from . import is_windows

import asyncio
import sys
from asyncio.subprocess import PIPE


@asyncio.coroutine
def read_stream_and_display(stream, display):
    """Read from stream line by line until EOF, display, and capture the lines."""
    output = []
    while True:
        line = yield from stream.readline()
        if not line:
            break
        output.append(line)
        display(line)  # assume it doesn't block
    return b''.join(output)


@asyncio.coroutine
def read_and_display(cmd, **kwargs):
    """Capture cmd's stdout and stderr while displaying them as they arrive (line by line)."""
    # start process
    process = yield from asyncio.create_subprocess_exec(
        *cmd, stdout=PIPE, stderr=PIPE, **kwargs
    )

    # Read child's stdout/stderr concurrently (capture and display)
    try:
        stdout, stderr = yield from asyncio.gather(
            read_stream_and_display(process.stdout, sys.stdout.buffer.write),
            read_stream_and_display(process.stderr, sys.stderr.buffer.write),
        )
    except Exception:
        process.kill()
        raise
    finally:
        # Wait for the process to exit
        return_code = yield from process.wait()
    return return_code, stdout, stderr


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

    return_code, stdout, stderr = asyncio.get_event_loop().run_until_complete(
        read_and_display(cmd, **kwargs)
    )
    return return_code, stdout, stderr
