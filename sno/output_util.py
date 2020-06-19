import io
import json
import sys

from contextlib import contextmanager
import os
from threading import Thread


JSON_PARAMS = {
    "compact": {},
    "pretty": {"indent": 2, "sort_keys": True},
    "extracompact": {"separators": (',', ':')},
}


def dump_json_output(output, output_path, json_style="pretty"):
    """
    Dumps the output to JSON in the output file.
    """

    fp = resolve_output_path(output_path)

    if json_style == 'pretty' and fp == sys.stdout and fp.isatty():
        # Add syntax highlighting
        from pygments import highlight
        from pygments.lexers import JsonLexer
        from pygments.formatters import TerminalFormatter

        dumped = json.dumps(output, **JSON_PARAMS[json_style])
        highlighted = highlight(dumped.encode(), JsonLexer(), TerminalFormatter())
        fp.write(highlighted)
    else:
        json.dump(output, fp, **JSON_PARAMS[json_style])


def resolve_output_path(output_path):
    """
    Takes a path-ish thing, and returns the appropriate writable file-like object.
    The path-ish thing could be:
      * a pathlib.Path object
      * a file-like object
      * the string '-' or None (both will return sys.stdout)
    """
    if isinstance(output_path, io.IOBase):
        return output_path
    elif (not output_path) or output_path == "-":
        return sys.stdout
    else:
        return output_path.open("w")


class InputMode:
    DEFAULT = 0
    INTERACTIVE = 1
    NO_INPUT = 2


def get_input_mode():
    if sys.stdin.isatty() and sys.stdout.isatty():
        return InputMode.INTERACTIVE
    elif sys.stdin.isatty() and not sys.stdout.isatty():
        return InputMode.NO_INPUT
    elif is_empty_stream(sys.stdin):
        return InputMode.NO_INPUT
    else:
        return InputMode.DEFAULT


def is_empty_stream(stream):
    if stream.seekable():
        pos = stream.tell()
        if stream.read(1) == "":
            return True
        stream.seek(pos)
    return False


@contextmanager
def logpipe(logger, level):
    """
    Context manager.
    Yields a writable file-like object that pipes text to a logger.

    Uses threads to avoid deadlock when this is passed to a subprocess.
    """
    fd_read, fd_write = os.pipe()

    def run():
        with os.fdopen(fd_read) as fo_read:
            for line in iter(fo_read.readline, ''):
                logger.log(level, line.strip('\n'))

    Thread(target=run).start()
    with os.fdopen(fd_write, 'w') as f_write:
        yield f_write
