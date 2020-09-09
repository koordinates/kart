from pygments.lexer import RegexLexer, include
from pygments.token import *


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
