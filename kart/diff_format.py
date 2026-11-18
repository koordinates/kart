from enum import Enum
import click


class DiffFormat(str, Enum):
    # Values for the --diff-format option
    FULL = "full"
    NO_DATA_CHANGES = "no-data-changes"
    NONE = "none"


class DiffFormatChoice(click.Choice):
    """
    A click.Choice for the DiffFormat enum that matches on the enum *value*
    (eg 'none') rather than the member *name* (eg 'NONE'). Click 8.2+ normalizes
    enum choices by name by default, which would break the documented
    lower-case --diff-format values.
    """

    def normalize_choice(self, choice, ctx):
        if isinstance(choice, Enum):
            choice = choice.value
        return super().normalize_choice(choice, ctx)
