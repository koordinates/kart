import json
import shutil
import sys
import textwrap

import pygments
from pygments.lexers import JsonLexer
from pygments.lexer import ExtendedRegexLexer, LexerContext


_terminal_formatter = None

JSON_PARAMS = {
    "compact": {},
    "pretty": {"indent": 2},
    "extracompact": {"separators": (",", ":")},
}


class ExtendedJsonEncoder(json.JSONEncoder):
    """A JSONEncoder that tries calling __json__() if it can't serialise an object another way."""

    def default(self, obj):
        try:
            return obj.__json__()
        except AttributeError:
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
    if json_style == "pretty" and fp == sys.stdout and fp.isatty():
        # Add syntax highlighting
        dumped = json.dumps(output, **JSON_PARAMS[json_style])
        return pygments.highlight(
            dumped.encode(), JsonLexer(), get_terminal_formatter()
        )
    else:
        # pygments adds a newline, best we do that here too for consistency
        return json.dumps(output, **JSON_PARAMS[json_style]) + "\n"


def wrap_text_to_terminal(text, indent=''):
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
            or ['']
        )
    return "".join(f"{indent}{line}\n" for line in lines)


class ExtendedJsonLexer(JsonLexer, ExtendedRegexLexer):
    """
    Inherits patterns from JsonLexer and get_tokens_unprocessed function from ExtendedRegexLexer.
    get_tokens_unprocessed enables the lexer to lex incomplete chunks of json.
    """

    pass


def dump_json_output(output, output_path, json_style="pretty"):
    """
    Dumps the output to JSON in the output file.
    """
    fp = resolve_output_path(output_path)

    highlit = json_style == "pretty" and fp == sys.stdout and fp.isatty()
    json_encoder = ExtendedJsonEncoder(**JSON_PARAMS[json_style])
    if highlit:
        ex_json_lexer = ExtendedJsonLexer()
        # The LexerContext stores the state of the lexer after each call to get_tokens_unprocessed
        lexer_context = LexerContext("", 0)

        for chunk in json_encoder.iterencode(output):
            lexer_context.text = chunk
            lexer_context.pos = 0
            lexer_context.end = len(chunk)
            token_generator = (
                (token_type, value)
                for (index, token_type, value) in ex_json_lexer.get_tokens_unprocessed(
                    context=lexer_context
                )
            )
            fp.write(pygments.format(token_generator, get_terminal_formatter()))

    else:
        for chunk in json_encoder.iterencode(output):
            fp.write(chunk)
    fp.write("\n")


def resolve_output_path(output_path):
    """
    Takes a path-ish thing, and returns the appropriate writable file-like object.
    The path-ish thing could be:
      * a pathlib.Path object
      * a file-like object
      * the string '-' or None (both will return sys.stdout)
    """
    if hasattr(output_path, 'write'):
        # filelike object. *usually* this is a io.TextIOWrapper,
        # but in some circumstances it can be something else.
        # e.g. click on windows may wrap it with a colorama.ansitowin32.StreamWrapper.
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
