from collections import deque
import itertools

from pygments.lexer import RegexLexer, include
from pygments.token import Keyword, Number, String, Punctuation, Whitespace


class WKTLexer(RegexLexer):
    """
    WKTLexer is able to split a string with well-known text format (WKT 1) into tokens.
    The tokens can then be passed to a pygments formatter.
    """

    name = "WKT"

    # integer part of a number
    int_part = r"-?(0|[1-9]\d*)"

    # fractional part of a number
    frac_part = r"\.\d+"

    # exponential part of a number
    exp_part = r"[eE](\+|-)?\d+"

    tokens = {
        "whitespace": [
            (r"\s+", Whitespace),
        ],
        # represents a simple terminal value
        "simplevalue": [
            (
                (
                    "%(int_part)s(%(frac_part)s%(exp_part)s|"
                    "%(exp_part)s|%(frac_part)s)"
                )
                % vars(),
                Number.Float,
            ),
            (int_part, Number.Integer),
            (r'"(""|[^"])*"', String.Double),
        ],
        # the contents of a list separated by commas and ended by a close bracket
        "list": [
            include("whitespace"),
            include("value"),
            (r",", Punctuation),
            (r"(\]|\))", Punctuation, "#pop"),
        ],
        # a list is started by an opening bracket (square or round)
        "liststart": [
            include("whitespace"),
            (r"(\[|\()", Punctuation, "list"),
        ],
        # a keyword
        "keyword": [
            include("whitespace"),
            (r"\w(\w|\d|_)*", Keyword),
        ],
        # values can be either simple values, keywords, or lists
        "value": [
            include("whitespace"),
            include("simplevalue"),
            include("keyword"),
            include("liststart"),
        ],
        # starting state, should start with a value
        "root": [
            include("value"),
        ],
    }

    def get_tokens(self, text, pretty_print=False, **kwargs):
        """
        Return an iterable of (tokentype, value) pairs generated from `text`.
        pretty_print - if True, strips any existing whitespace and adds new whitespace to make it readable.
            Each keyword will be on a new line, with indentation according to the level of nesting.
        """
        token_iter = super().get_tokens(text, **kwargs)
        if not pretty_print:
            yield from token_iter

        # Filter out existing whitespace
        token_iter = filter(lambda tok: tok[0] != Whitespace, token_iter)
        # Pad iterator at each end so that _windowed() works:
        empty_token = (Whitespace, "")
        token_iter = itertools.chain([empty_token], token_iter, [empty_token])

        indent = 0
        for prev_tok, cur_tok, next_tok in _windowed(token_iter, 3):
            if prev_tok[1] == ",":
                if cur_tok[0] == Keyword and next_tok[1] in ("[", "("):
                    indent += 1
                    yield Whitespace, "\n" + "    " * indent
                else:
                    yield Whitespace, " "

            if cur_tok[1] in ("]", ")"):
                indent = max(indent - 1, 0)

            yield cur_tok

        yield Whitespace, "\n"


def _windowed(iterable, size):
    """Yields a sliding window of length size across iterable."""
    iterable = iter(iterable)
    window = deque(itertools.islice(iterable, size), size)
    if len(window) == size:
        yield window
        for elem in iterable:
            window.append(elem)
            yield window
