from collections import deque
import itertools

from pygments.lexer import RegexLexer, include
from pygments.token import Keyword, Number, String, Punctuation, Token, Whitespace


Comma = Punctuation.Comma
OpenBracket = Punctuation.OpenBracket
CloseBracket = Punctuation.CloseBracket


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
            (r",", Comma),
            (r"(\]|\))", CloseBracket, "#pop"),
        ],
        # a list is started by an opening bracket (square or round)
        "liststart": [
            include("whitespace"),
            (r"(\[|\()", OpenBracket, "list"),
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
            if prev_tok[0] is Comma:
                if cur_tok[0] == Keyword and next_tok[0] is OpenBracket:
                    indent += 1
                    yield Whitespace, "\n" + "    " * indent
                else:
                    yield Whitespace, " "

            if cur_tok[0] is CloseBracket:
                indent = max(indent - 1, 0)

            yield cur_tok

        yield Whitespace, "\n"

    def find_pattern(
        self, text, pattern, at_depth=None, extract_strings=False, **kwargs
    ):
        """
        Given a pattern to search for eg ("AUTHORITY", "[", String.Double, ",", String.Double, "]") -
        returns the text that matches the first occurrence of that pattern.
        Whitespace is skipped and is not included in the result.
        """
        token_iter = super().get_tokens(text, **kwargs)
        token_iter = filter(lambda tok: tok[0] != Whitespace, token_iter)

        depth = 0
        result = []
        result_len = 0

        for tokentype, value in token_iter:
            if tokentype is OpenBracket:
                depth += 1
            elif tokentype is CloseBracket:
                depth -= 1
            if at_depth is not None and result_len == 0 and depth != at_depth:
                continue
            if self._matches(pattern[result_len], tokentype, value):
                result.append(value)
                result_len += 1
                if result_len == len(pattern):
                    return (
                        self._extract_strings(pattern, result)
                        if extract_strings
                        else tuple(result)
                    )
            else:
                result.clear()
                result_len = 0

        return None

    def _matches(self, expected, tokentype, value):
        if isinstance(expected, type(Token)):
            return tokentype in expected
        return expected == value

    def _extract_strings(self, pattern, result):
        extracted = []
        for p, r in zip(pattern, result):
            if p in String:
                extracted.append(r[1:-1].replace('""', '"'))
        return tuple(extracted)


def _windowed(iterable, size):
    """Yields a sliding window of length size across iterable."""
    iterable = iter(iterable)
    window = deque(itertools.islice(iterable, size), size)
    if len(window) == size:
        yield window
        for elem in iterable:
            window.append(elem)
            yield window
