from enum import Enum
import click


class DiffFormat(str, Enum):
    # Values for the --diff-format option
    FULL = "full"
    NO_DATA_CHANGES = "no-data-changes"
    NONE = "none"
