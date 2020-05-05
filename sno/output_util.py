import io
import json
import sys

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
