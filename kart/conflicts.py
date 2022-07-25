import click
from .conflicts_writer import BaseConflictsWriter
from .cli_util import OutputFormatType, parse_output_format, KartCommand
from .crs_util import CoordinateReferenceString
from .repo import KartRepoState


@click.command(cls=KartCommand)
@click.pass_context
@click.option(
    "--output-format",
    "-o",
    type=OutputFormatType(
        output_types=["text", "json", "geojson", "quiet"],
        allow_text_formatstring=False,
    ),
    default="text",
    help="Output format. 'quiet' disables all output and implies --exit-code.",
)
@click.option(
    "--output",
    "output_path",
    help="Output to a specific file/directory instead of stdout.",
    type=click.Path(writable=True, allow_dash=True),
)
@click.option(
    "--exit-code",
    is_flag=True,
    help="Make the program exit with 1 if there are conflicts and 0 means no conflicts.",
)
@click.option(
    "--json-style",
    type=click.Choice(["extracompact", "compact", "pretty"]),
    help="[deprecated] How to format the output. Only used with `-o json` and `-o geojson`",
)
@click.option(
    "--exit-code",
    is_flag=True,
    help="Make the program exit with 1 if there are conflicts and 0 means no conflicts.",
)
@click.option(
    "-s",
    "--summarise",
    "--summarize",
    count=True,
    help="Summarise the conflicts rather than output each one in full. Use -ss for short summary.",
)
@click.option(
    "--flat",
    is_flag=True,
    hidden=True,
    help="Output all conflicts in a flat list, instead of in a hierarchy.",
)
@click.option(
    "--crs",
    type=CoordinateReferenceString(encoding="utf-8"),
    help="Reproject geometries into the given coordinate reference system. Accepts: 'EPSG:<code>'; proj text; OGC WKT; OGC URN; PROJJSON.)",
)
@click.argument("filters", nargs=-1)
def conflicts(
    ctx,
    output_format,
    output_path,
    exit_code,
    json_style,
    summarise,
    flat,
    crs,
    filters,
):
    """
    Lists merge conflicts that need to be resolved before the ongoing merge can be completed.

    To list only particular conflicts, supply one or more FILTERS of the form [DATASET[:PRIMARY_KEY]]
    """
    repo = ctx.obj.get_repo(allowed_states=KartRepoState.MERGING)
    output_type, fmt = parse_output_format(output_format, json_style)

    conflicts_writer_class = BaseConflictsWriter.get_conflicts_writer_class(output_type)
    conflicts_writer = conflicts_writer_class(
        repo,
        filters,
        output_path,
        summarise,
        flat,
        json_style=fmt,
        target_crs=crs,
    )
    conflicts_writer.write_conflicts()

    if exit_code or output_type == "quiet":
        conflicts_writer.exit_with_code()
