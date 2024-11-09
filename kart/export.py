import click

from kart.completion_shared import repo_path_completer
from kart.cli_util import KartCommand, forward_context_to_command


# Aliases `kart export` to `kart table-export`.
# One day we might support other types of export, and when we do, this will then
# be a general purpose command that delegates to one or other more specific commands.
# See: how `kart import` delegates to one of table-import, point-cloud-import, raster-import.


@click.command(
    "export",
    cls=KartCommand,
    context_settings=dict(ignore_unknown_options=True),
)
@click.pass_context
@click.option(
    "--list-formats",
    is_flag=True,
    help="List available export formats, and then exit",
)
@click.argument(
    "args",
    nargs=-1,
    metavar="DATASET [EXPORT_TYPE:]DESTINATION",
    shell_complete=repo_path_completer,  # type: ignore[call-arg]
)
def export(ctx, args, **kwargs):
    """
    Basic export command - exports a tabular kart dataset at a particular commit.
    Currently only vector / tabular datasets are supported.

    To see help specific to vector / tabular datasets, look up kart table-export.
    """

    from kart.tabular.export import table_export

    forward_context_to_command(ctx, table_export)
