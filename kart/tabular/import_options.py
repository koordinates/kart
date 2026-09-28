"""
The --path-* options that control how features are laid out in the repository.

These are defined here (rather than inline in kart/tabular/import_.py) so that the
`kart import` wrapper command can declare exactly the same options as `kart table-import`
without pulling in the rest of the tabular import machinery.
"""

import click


def path_structure_options(cmd):
    """Adds the --path-encoding / --path-levels / --path-branches options to a command."""
    for option in reversed(_PATH_STRUCTURE_OPTIONS):
        cmd = option(cmd)
    return cmd


_PATH_STRUCTURE_OPTIONS = [
    click.option(
        "--path-encoding",
        type=click.Choice(["hex", "base64"]),
        default=None,
        help=(
            "Which alphabet is used to name the trees that features are stored in. "
            "Defaults to base64. Only affects newly imported datasets."
        ),
    ),
    click.option(
        "--path-levels",
        type=click.INT,
        default=None,
        help=(
            "How many levels of trees features are stored in. Defaults to 4. "
            "Fewer levels means fewer, larger trees - this is generally more efficient for "
            "datasets with randomly distributed primary keys (eg UUIDs) that are not expected "
            "to grow to many millions of features. Only affects newly imported datasets."
        ),
    ),
    click.option(
        "--path-branches",
        type=click.INT,
        default=None,
        help=(
            "How many trees each level of trees branches into. Defaults to 64. "
            "Must be a power of the size of the alphabet given by --path-encoding - "
            "so 16, 256 or 4096 for hex, or 64 or 4096 for base64. "
            "Only affects newly imported datasets."
        ),
    ),
]
