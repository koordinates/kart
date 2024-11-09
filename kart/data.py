import sys

import click

from .cli_util import KartGroup, StringFromFile, add_help_subcommand
from .commit import commit_json_to_text, commit_obj_to_json, get_commit_message
from .diff_structs import DatasetDiff, Delta, DeltaDiff, RepoDiff
from .exceptions import NO_TABLE, NotFound
from .output_util import dump_json_output
from .repo import KartRepoState
from .completion_shared import ref_completer

# Changing these items would generally break the repo;
# we disallow that.


@add_help_subcommand
@click.group(cls=KartGroup)
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
@click.option(
    "--with-dataset-types/--without-dataset-types",
    is_flag=True,
    help="When set, outputs the dataset type and version. (This may become the default in a later version of Kart)",
)
@click.argument(
    "refish",
    required=False,
    default="HEAD",
    shell_complete=ref_completer,  # type: ignore[call-arg]
)
@click.pass_context
def data_ls(ctx, output_format, with_dataset_types, refish):
    """List all of the datasets in the Kart repository"""
    repo = ctx.obj.get_repo(allowed_states=KartRepoState.ALL_STATES)
    json_list = [
        {"path": ds.path, "type": ds.DATASET_TYPE, "version": ds.VERSION}
        for ds in repo.datasets(refish)
    ]

    if output_format == "text" and not json_list:
        repo_desc = (
            "Empty repository."
            if repo.head_is_unborn
            else f"The commit at {refish} has no datasets."
        )
        click.echo(f'{repo_desc}\n  (use "kart import" to add some data)')
        return

    if output_format == "text":
        if with_dataset_types:
            for ds_obj in json_list:
                click.echo("{path}\t({type}.v{version})".format(**ds_obj))

        else:
            for ds_obj in json_list:
                click.echo(ds_obj["path"])

    elif output_format == "json":
        if not with_dataset_types:
            json_list = [ds_obj["path"] for ds_obj in json_list]
        version_marker = "v2" if with_dataset_types else "v1"
        dump_json_output({f"kart.data.ls/{version_marker}": json_list}, sys.stdout)


@data.command(name="rm")
@click.option(
    "--message",
    "-m",
    multiple=True,
    help=(
        "Use the given message as the commit message. If multiple `-m` options are given, their values are "
        "concatenated as separate paragraphs."
    ),
    type=StringFromFile(encoding="utf-8"),
)
@click.option(
    "--output-format",
    "-o",
    type=click.Choice(["text", "json"]),
    default="text",
)
@click.argument("datasets", nargs=-1, type=click.UNPROCESSED)
@click.pass_context
def data_rm(ctx, message, output_format, datasets):
    """Delete one or more datasets in the Kart repository, and commit the result"""

    if not datasets:
        raise click.UsageError("Specify a dataset to delete: eg `kart data rm DATASET`")

    repo = ctx.obj.get_repo()
    existing_ds_paths = set(repo.datasets().paths())

    for ds_path in datasets:
        if ds_path not in existing_ds_paths:
            raise NotFound(
                f"Cannot delete dataset at path '{ds_path}' since it does not exist",
                exit_code=NO_TABLE,
            )

    repo.working_copy.check_not_dirty()

    repo_diff = RepoDiff()
    for ds_path in datasets:
        dataset = repo.datasets()[ds_path]
        ds_diff = DatasetDiff()
        ds_diff["meta"] = DeltaDiff.diff_dicts(dataset.meta_items(), {})
        ds_diff["feature"] = dataset.all_features_diff(delta_type=Delta.delete)
        repo_diff[ds_path] = ds_diff

    do_json = output_format == "json"
    if message:
        commit_msg = "\n\n".join([m.strip() for m in message]).strip()
    else:
        commit_msg = get_commit_message(repo, repo_diff, quiet=do_json)

    if not commit_msg:
        raise click.UsageError("Aborting commit due to empty commit message.")

    new_commit = repo.structure().commit_diff(repo_diff, commit_msg)
    repo.working_copy.reset_to_head()

    jdict = commit_obj_to_json(new_commit, repo, repo_diff)
    if do_json:
        dump_json_output(jdict, sys.stdout)
    else:
        click.echo(commit_json_to_text(jdict))

    repo.gc("--auto")


@data.command(name="version", hidden=True)
@click.option(
    "--output-format",
    "-o",
    type=click.Choice(["text", "json"]),
    default="text",
)
@click.pass_context
def data_version(ctx, output_format):
    """
    Show the repository structure version.

    This was more useful when each Kart repositories contained a single type of table dataset at a single version -
    eg table.v1 in one repository, vs table.v2 in another repository.
    Now that Kart repositories can contain more than one dataset type eg table.v3, point-cloud.v1, raster.v1,
    it no longer really conveys anything useful about the Kart repository's "version".
    """
    click.echo(
        "The command `kart data version` is deprecated - use `kart data ls --with-dataset-types` instead.",
        err=True,
    )
    repo = ctx.obj.get_repo(
        allowed_states=KartRepoState.ALL_STATES, allow_unsupported_versions=True
    )
    version = repo.table_dataset_version
    if output_format == "text":
        click.echo(f"This Kart repo uses Datasets v{version}")
        if version >= 1:
            click.echo(
                f"(See https://docs.kartproject.org/en/latest/pages/development/table_v{version}.html"
            )
    elif output_format == "json":
        from .repo import KartConfigKeys

        branding = (
            "kart"
            if KartConfigKeys.KART_REPOSTRUCTURE_VERSION in repo.config
            else "sno"
        )
        dump_json_output(
            {"repostructure.version": version, "localconfig.branding": branding},
            sys.stdout,
        )
