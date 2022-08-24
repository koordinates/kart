import click

from kart.completion import Shells, install_tab_completion
from kart.cli_util import add_help_subcommand, KartGroup


@add_help_subcommand
@click.group(cls=KartGroup)
@click.pass_context
def install(ctx, **kwargs):
    """Install tools and add-ons"""


@install.command()
@click.option(
    "--shell",
    nargs=1,
    type=click.Choice([s.value for s in Shells] + ["auto"]),
    default="auto",
    help="Select a shell to install tab completion for. Defaults to auto for auto-detecting shell.",
)
def tab_completion(shell: str):
    """Install tab completion for the specific or current shell"""
    if shell == "auto":
        shell, path = install_tab_completion()
    else:
        shell, path = install_tab_completion(shell=shell)
    click.secho(f"{shell} completion installed in {path}", fg="green")
    click.echo("Completion will take effect once you restart the terminal")
