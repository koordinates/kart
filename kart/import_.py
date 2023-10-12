import click

from kart.cli_util import (
    StringFromFile,
    call_and_exit_flag,
    KartCommand,
    find_param,
)
from kart.completion_shared import file_path_completer
from kart.import_sources import from_spec, suggest_specs


def list_import_formats(ctx):
    """List the supported import formats."""
    click.echo(suggest_specs())


# Some options are defined here for since they are supported by this command directly,
# but most are here so that they show up in the help text for this command.
# Apart from causing them to show up in the help text, there is no need to define
# most options here - defining them in the sub-variants that actually support them
# is sufficient.
# Options that are defined here should be defined exactly the same in any of the
# sub-variants that support them, to avoid confusion.


@click.command(
    "import",
    cls=KartCommand,
    context_settings=dict(ignore_unknown_options=True),
)
@click.pass_context
@click.option(
    "--message",
    "-m",
    type=StringFromFile(encoding="utf-8"),
    help="Commit message. By default this is auto-generated.",
)
@call_and_exit_flag(
    "--list-formats",
    callback=list_import_formats,
    help="List available import formats, and then exit",
)
@click.option(
    "--replace-existing",
    is_flag=True,
    help="Replace existing dataset(s) of the same name.",
)
@click.option(
    "--checkout/--no-checkout",
    "do_checkout",
    is_flag=True,
    default=True,
    help=(
        "Whether to check out the dataset once the import is finished. If false, the dataset will be configured as "
        "not being checked out and will never be written to the working copy, until this decision is reversed by "
        "running `kart checkout --dataset=DATASET-PATH`."
    ),
)
@click.option(
    "--dataset-path", "--dataset", "ds_path", help="The dataset's path once imported"
)
@click.argument(
    "args",
    nargs=-1,
    metavar="SOURCE [[SOURCES...] or [DATASETS...]]",
    shell_complete=file_path_completer,
)
def import_(ctx, args, **kwargs):
    """
    Import data into a repository.
    This is a one-size-fits-all command - look up the following commands for more
    specific information, including more options that are not listed here:

    \b
    kart table-import
    kart point-cloud-import
    kart raster-import

    For more information on the supported types of import-sources and how to specify them,
    try `kart import --list-formats`

    There are two different forms of this command, depending on what is being imported.

    \b
    First form:
    $ kart import SOURCE [DATASETS...]

    This form imports one or more datasets that are found in a single import-source.
    An import-source could be a file or a database location. This is the first argument.
    This is sufficient if there is only one dataset to be found inside the import-source.
    If there is more than one, then all subsequent arguments should be the names of
    datasets to be imported that are found inside the import-source.

    For example, to import two datasets called my_points and my_lines from my_data.gpkg:

    $ kart import my_data.gpkg my_points my_lines

    To rename datasets as they are imported, use the suffix :NEW_NAME on the DATASETS -
    for example:

    $ kart import my_data.gpkg my_points:points_renamed my_lines:lines_renamed

    \b
    Second form:
    $ kart import --dataset=DATASET-PATH SOURCE [SOURCES...]

    This form imports one dataset that is found in one or more import-sources.
    The import-source in this case is generally a file that contains one piece
    of the data of a large dataset that has been sharded / tiled into multiple files.
    The dataset specified will be the name (ie, the path) of the dataset once
    imported from all of the sources.

    For example, to import all LAZ files from the my_point_clouds directory:

    $ kart import --dataset=my_points_clouds my_point_clouds/*.laz

    Note that --dataset can be ommitted if a reasonable name can be inferred from the sources.
    """

    args = [a for a in args if not a.startswith("-")]
    if not args:
        click.echo("At least one SOURCE is required for kart import.", err=True)
        raise click.MissingParameter(param=find_param(ctx, "args"))

    for arg in args:
        import_source_type = from_spec(arg, allow_unrecognised=True)
        if import_source_type is not None:
            break

    if import_source_type is None:
        raise click.UsageError(
            "Unrecognised import-source specification.\n"
            f"Try one of the following:\n{suggest_specs()}"
        )

    import_cmd = import_source_type.import_cmd

    subctx = import_cmd.make_context(import_cmd.name, ctx.unparsed_args)
    subctx.obj = ctx.obj
    subctx.forward(import_cmd)
