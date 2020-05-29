import io
import json
import os
import shutil
import sys
import threading
from contextlib import closing, contextmanager
from queue import Queue, Empty

import click
from click._compat import should_strip_ansi

JSON_PARAMS = {
    "compact": {},
    "pretty": {"indent": 2, "sort_keys": True},
    "extracompact": {"separators": (',', ':')},
}


def dump_json_output(output, output_path, json_style="pretty"):
    """
    Dumps the output to JSON in the output file.
    """

    with resolve_output_path(output_path) as fp:
        if json_style == 'pretty' and not should_strip_ansi(fp):
            # Add syntax highlighting
            from pygments import highlight
            from pygments.lexers import JsonLexer
            from pygments.formatters import TerminalFormatter

            dumped = json.dumps(output, **JSON_PARAMS[json_style])
            highlighted = highlight(dumped.encode(), JsonLexer(), TerminalFormatter())
            fp.write(highlighted)
        else:
            json.dump(output, fp, **JSON_PARAMS[json_style])


@contextmanager
def resolve_output_path(output_path, allow_pager=True):
    """
    Context manager.

    Takes a path-ish thing, and yields the appropriate writable file-like object.
    The path-ish thing could be:
      * a pathlib.Path object
      * a file-like object
      * the string '-' or None (both will return sys.stdout)

    If the file is not stdout, it will be closed when exiting the context manager.

    If allow_pager=True (the default) and the file is stdout, this will attempt to use a
    pager to display long output.
    """

    if isinstance(output_path, io.IOBase):
        # Make this contextmanager re-entrant
        yield output_path
    elif (not output_path) or output_path == "-":
        if allow_pager and get_input_mode() == InputMode.INTERACTIVE:
            pager_cmd = (
                os.environ.get('SNO_PAGER') or os.environ.get('PAGER') or DEFAULT_PAGER
            )

            with _push_environment('PAGER', pager_cmd):
                with click.get_pager_file() as pager:
                    yield pager
        else:
            yield sys.stdout
    else:
        with closing(output_path.open("w")) as f:
            yield f


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


def _setenv(k, v):
    if v is None:
        os.environ.pop(k, None)
    else:
        os.environ[k] = v


@contextmanager
def _push_environment(k, v):
    orig = os.environ.get(k)
    _setenv(k, v)
    try:
        yield
    finally:
        _setenv(k, orig)


DEFAULT_PAGER = shutil.which('less')
if DEFAULT_PAGER:
    DEFAULT_PAGER += ' --quit-if-one-screen --no-init -R'
