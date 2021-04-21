import sys

import click

from . import status
from .cli_util import add_help_subcommand
from .output_util import dump_json_output
from .repo import SnoRepoState


# Changing these items would generally break the repo;
# we disallow that.


@add_help_subcommand
@click.group()
@click.pass_context
def data(ctx, **kwargs):
    """Information about the datasets in a repository."""


@data.command(name="ls")
@click.option(
    "--output-format",
    "-o",
    type=click.Choice(["text", "json"]),
    default="text",
)
@click.argument("refish", required=False, default="HEAD")
@click.pass_context
def data_ls(ctx, output_format, refish):
    """List all of the datasets in the Kart repository"""
    repo = ctx.obj.get_repo(allowed_states=SnoRepoState.ALL_STATES)
    if repo.is_empty:
        ds_paths = []
    else:
        ds_paths = [ds.path for ds in repo.datasets(refish)]

    if output_format == "text":
        if ds_paths:
            for ds_path in ds_paths:
                click.echo(ds_path)
        else:
            ctx.invoke(status.status)

    elif output_format == "json":
        dump_json_output({"kart.data.ls/v1": ds_paths}, sys.stdout)


@data.command(name="version")
@click.option(
    "--output-format",
    "-o",
    type=click.Choice(["text", "json"]),
    default="text",
)
@click.pass_context
def data_version(ctx, output_format):
    """Show the repository structure version"""
    repo = ctx.obj.get_repo(
        allowed_states=SnoRepoState.ALL_STATES, allow_unsupported_versions=True
    )
    version = repo.version
    if output_format == "text":
        click.echo(f"This Kart repo uses Datasets v{version}")
        if version >= 1:
            click.echo(
                f"(See https://github.com/koordinates/sno/blob/master/docs/DATASETS_v{version}.md)"
            )
    elif output_format == "json":
        dump_json_output({"kart.data.version": version}, sys.stdout)
