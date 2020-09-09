import sys

import click

from . import status
from .cli_util import add_help_subcommand
from .output_util import dump_json_output
from .structure import RepositoryStructure
from .repo_files import RepoState


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
@click.pass_context
def data_ls(ctx, output_format):
    """List all of the datasets in the sno repository"""
    repo = ctx.obj.get_repo(allowed_states=RepoState.ALL_STATES)
    if repo.is_empty:
        ds_paths = []
    else:
        rs = RepositoryStructure.lookup(repo, "HEAD")
        ds_paths = [ds.path for ds in rs]

    if output_format == "text":
        if ds_paths:
            for ds_path in ds_paths:
                click.echo(ds_path)
        else:
            ctx.invoke(status.status)

    elif output_format == "json":
        dump_json_output({"sno.data.ls/v1": ds_paths}, sys.stdout)
