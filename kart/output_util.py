import datetime
import itertools
import json
import re
import shutil
import sys
import textwrap
import types
from pathlib import Path

import msgspec.json
import pygments
from pygments.lexers import JsonLexer

from .wkt_lexer import WKTLexer

_terminal_formatter = None

JSON_PARAMS = {
    "compact": {},
    "extracompact": {"separators": (",", ":")},
    "pretty": {"indent": 2},
}


class SerializableGenerator(list):
    """Generator that is serializable by JSON"""

    def __init__(self, iterable):
        tmp_body = iter(iterable)
        try:
            self._head = iter([next(tmp_body)])
            self.append(tmp_body)
        except StopIteration:
            self._head = []

    def __iter__(self):
        return itertools.chain(self._head, *self[:1])


def msgspec_json_encode_default(obj):
    """
    Hook to extend the default serialisation of `msgspec.json.dumps()`
    """
    if isinstance(obj, tuple):
        return list(obj)

    if hasattr(obj, "__json__"):
        return obj.__json__()

    raise NotImplementedError


msgspec_json_encoder = msgspec.json.Encoder(enc_hook=msgspec_json_encode_default)


class ExtendedJsonEncoder(json.JSONEncoder):
    """A JSONEncoder that tries calling __json__() if it can't serialise an object another way."""

    def __init__(self, *args, default=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_function = default

    def default(self, obj):
        if self.default_function is not None:
            result = self.default_function(obj)
            if result is not None:
                return result

        if isinstance(obj, types.GeneratorType):
            return SerializableGenerator(obj)

        if isinstance(obj, (datetime.date, datetime.datetime, datetime.time)):
            return obj.isoformat()

        if hasattr(obj, "__json__"):
            return obj.__json__()

        return json.JSONEncoder.default(self, obj)


def get_terminal_formatter():
    global _terminal_formatter
    if _terminal_formatter is None:
        import pygments.token as token
        from pygments.formatters import TerminalFormatter

        # Colours to use for syntax highlighting if printing to terminal.
        # First colour is for light background, second for dark background.
        # Default is light background, pass bg="dark" to TerminalFormatter to use dark background colours.
        _terminal_formatter = TerminalFormatter(
            colorscheme={
                token.Token: ("", ""),
                token.Whitespace: ("gray", "brightblack"),
                token.Keyword: ("magenta", "brightmagenta"),
                token.Name.Tag: ("yellow", "yellow"),
                token.String: ("brightblue", "brightblue"),
                token.Number: ("cyan", "brightcyan"),
                token.Generic.Error: ("brightred", "brightred"),
                token.Error: ("_brightred_", "_brightred_"),
            }
        )
    return _terminal_formatter


def format_json_for_output(output, fp, json_style="pretty"):
    """
    Serializes JSON for writing to the given filelike object.
    Doesn't actually write the JSON, just returns it.

    Adds syntax highlighting if appropriate.
    """
    if json_style == "pretty" and can_output_colour(fp):
        # Add syntax highlighting
        dumped = json.dumps(output, **JSON_PARAMS[json_style])
        return pygments.highlight(
            dumped.encode(), JsonLexer(), get_terminal_formatter()
        )
    else:
        # pygments adds a newline, best we do that here too for consistency
        return json.dumps(output, **JSON_PARAMS[json_style]) + "\n"


def can_output_colour(fp):
    return fp in (sys.stdout, sys.stderr) and fp.isatty()


def format_wkt_for_output(output, fp=None, syntax_highlight=True):
    """
    Formats WKT whitespace for readability.
    Adds syntax highlighting if fp is a terminal and syntax_highlight=True.
    Doesn't print the formatted WKT to fp, just returns it.
    """
    token_iter = WKTLexer().get_tokens(output, pretty_print=True)
    if syntax_highlight and can_output_colour(fp):
        return pygments.format(token_iter, get_terminal_formatter())
    else:
        token_value = (value for token_type, value in token_iter)
        return "".join(token_value)


def write_with_indent(fp, text, indent=""):
    for line in text.splitlines():
        fp.write(f"{indent}{line}\n")


def wrap_text_to_terminal(text, indent=""):
    """
    Wraps block text to the current width of the terminal.

    Optionally adds an indent.

    Respects 'COLUMNS' env var
    """
    lines = []
    term_width = shutil.get_terminal_size().columns
    for line in text.splitlines():
        lines.extend(
            textwrap.wrap(
                line,
                width=term_width - len(indent),
                # textwrap has all the wrong defaults :(
                replace_whitespace=False,
                drop_whitespace=False,
                expand_tabs=False,
                # without this it tends to break URLs up
                break_on_hyphens=False,
            )
            # double-newlines (ie pretty paragraph breaks) get collapsed without this
            or [""]
        )
    return "".join(f"{indent}{line}\n" for line in lines)


def _buffer_json_keys(chunk_generator):
    """
    We can do chunk-by-chunk JSON highlighting, but only if we buffer everything that might be a key, so that:
    {"key": value} can be treated differently to ["value", "value", "value", ...]
    """

    buf = None
    for chunk in chunk_generator:
        if buf is not None:
            yield buf + chunk
            buf = None
        elif re.search(r"""["']\s*$""", chunk):
            buf = chunk
        else:
            yield chunk

    if buf is not None:
        yield buf


def dump_json_output(
    output,
    output_path,
    json_style="pretty",
    encoder_class=ExtendedJsonEncoder,
    encoder_kwargs={},
):
    """
    Dumps the output to JSON in the output file.
    """
    output = _maybe_legacy_style_output(output)

    fp = resolve_output_path(output_path)

    highlit = can_output_colour(fp)
    json_encoder = encoder_class(**JSON_PARAMS[json_style], **encoder_kwargs)
    if highlit:
        json_lexer = JsonLexer()
        for chunk in _buffer_json_keys(json_encoder.iterencode(output)):
            token_generator = (
                (token_type, value)
                for (index, token_type, value) in json_lexer.get_tokens_unprocessed(
                    chunk
                )
            )
            fp.write(pygments.format(token_generator, get_terminal_formatter()))

    else:
        for chunk in json_encoder.iterencode(output):
            fp.write(chunk)
    fp.write("\n")


def _maybe_legacy_style_output(output):
    # If the caller ran "sno status", return output starting with "sno.status/v1"
    # But if they run "kart status", return the unchanged output ie "kart.status/v1".
    import os

    if os.path.basename(sys.argv[0]) != "sno":
        return output
    if (
        isinstance(output, dict)
        and len(output) <= 2
        and all(key.startswith("kart.") for key in output)
    ):
        output = {key.replace("kart.", "sno."): value for key, value in output.items()}
    return output


def resolve_output_path(output_path):
    """
    Takes a path-ish thing, and returns the appropriate writable file-like object.
    The path-ish thing could be:
      * a pathlib.Path object
      * a file-like object
      * the string '-' or None (both will return sys.stdout)
    """
    if (not output_path) or output_path == "-":
        return sys.stdout

    if isinstance(output_path, str):
        output_path = Path(output_path)
    if isinstance(output_path, Path):
        output_path = output_path.expanduser()

    if hasattr(output_path, "write"):
        # filelike object. *usually* this is a io.TextIOWrapper,
        # but in some circumstances it can be something else.
        # e.g. click on windows may wrap it with a colorama.ansitowin32.StreamWrapper.
        return output_path

    else:
        return output_path.open("w", encoding="utf-8")


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
        try:
            pos = stream.tell()
        except OSError:
            # if the stream is a pipe it will return .seekable() as True
            # though it is actually not.
            return False
        if stream.read(1) == "":
            return True
        stream.seek(pos)
    return False
