import io
import json
import sys


JSON_PARAMS = {
    "compact": {},
    "pretty": {"indent": 2},
    "extracompact": {"separators": (',', ':')},
}


class ExtendedJsonEncoder(json.JSONEncoder):
    """A JSONEncoder that tries calling __json__() if it can't serialise an object another way."""

    def default(self, obj):
        try:
            return obj.__json__()
        except AttributeError:
            return json.JSONEncoder.default(self, obj)


def format_json_for_output(output, fp, json_style="pretty"):
    """
    Serializes JSON for writing to the given filelike object.
    Doesn't actually write the JSON, just returns it.

    Adds syntax highlighting if appropriate.
    """
    if json_style == 'pretty' and fp == sys.stdout and fp.isatty():
        # Add syntax highlighting
        from pygments import highlight
        from pygments.lexers import JsonLexer
        from pygments.formatters import TerminalFormatter

        dumped = json.dumps(output, **JSON_PARAMS[json_style])
        return highlight(dumped.encode(), JsonLexer(), TerminalFormatter())
    else:
        # pygments adds a newline, best we do that here too for consistency
        return json.dumps(output, **JSON_PARAMS[json_style]) + '\n'


def dump_json_output(output, output_path, json_style="pretty"):
    """
    Dumps the output to JSON in the output file.
    """
    fp = resolve_output_path(output_path)

    # TODO: reintroduce JSON syntax highlighting
    # highlit = json_style == 'pretty' and fp == sys.stdout and fp.isatty()

    json_encoder = ExtendedJsonEncoder(**JSON_PARAMS[json_style])
    for chunk in json_encoder.iterencode(output):
        fp.write(chunk)
    fp.write('\n')


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
