import click

from .crs_util import CoordinateReferenceString
from .repo import KartRepoState
from . import diff_estimation


@click.command()
@click.pass_context
@click.option(
    "--output-format",
    "-o",
    type=click.Choice(
        ["text", "json", "geojson", "quiet", "feature-count", "html", "json-lines"]
    ),
    default="text",
    help=(
        "Output format. 'quiet' disables all output and implies --exit-code.\n"
        "'html' attempts to open a browser unless writing to stdout ( --output=- )"
    ),
)
@click.option(
    "--exit-code",
    is_flag=True,
    help="Make the program exit with codes similar to diff(1). That is, it exits with 1 if there were differences and 0 means no differences.",
)
@click.option(
    "--crs",
    type=CoordinateReferenceString(encoding="utf-8"),
    help="Reproject geometries into the given coordinate reference system. Accepts: 'EPSG:<code>'; proj text; OGC WKT; OGC URN; PROJJSON.)",
)
@click.option(
    "--output",
    "output_path",
    help="Output to a specific file/directory instead of stdout.",
    type=click.Path(writable=True, allow_dash=True),
)
@click.option(
    "--json-style",
    type=click.Choice(["extracompact", "compact", "pretty"]),
    default="pretty",
    help="How to format the output. Only used with -o json or -o geojson",
)
@click.option(
    "--only-feature-count",
    default=None,
    type=click.Choice(diff_estimation.ACCURACY_CHOICES),
    help=(
        "Returns only a feature count (the number of features modified in this diff). "
        "If the value is 'exact', the feature count is exact (this may be slow.) "
        "Otherwise, the feature count will be approximated with varying levels of accuracy."
    ),
)
@click.argument("refish", default="HEAD", required=False)
@click.argument("filters", nargs=-1)
def show(
    ctx,
    *,
    output_format,
    crs,
    output_path,
    exit_code,
    json_style,
    only_feature_count,
    refish,
    filters,
):
    """
    Show the given commit, or HEAD
    """
    if ".." in refish:
        raise click.BadParameter(
            f"Can only show a single ref-ish - can't show {refish}", param_hint="refish"
        )

    commit_spec = f"{refish}^?...{refish}"

    if only_feature_count:
        from .diff import feature_count_diff

        return feature_count_diff(
            ctx,
            output_format,
            commit_spec,
            output_path,
            exit_code,
            json_style,
            only_feature_count,
        )

    from .base_diff_writer import BaseDiffWriter

    repo = ctx.obj.get_repo(allowed_states=KartRepoState.ALL_STATES)
    diff_writer_class = BaseDiffWriter.get_diff_writer_class(output_format)
    diff_writer = diff_writer_class(
        repo, commit_spec, filters, output_path, json_style=json_style, target_crs=crs
    )
    diff_writer.include_target_commit_as_header()
    diff_writer.write_diff()

    if exit_code or output_format == "quiet":
        diff_writer.exit_with_code()


@click.command(name="create-patch")
@click.pass_context
@click.option(
    "--json-style",
    type=click.Choice(["extracompact", "compact", "pretty"]),
    default="pretty",
    help="How to format the output",
)
@click.option(
    "--output",
    "output_path",
    help="Output to a specific file/directory instead of stdout.",
    type=click.Path(writable=True, allow_dash=True),
)
# NOTE: this is *required* for now.
# A future version might create patches from working-copy changes.
@click.argument("refish")
def create_patch(ctx, *, refish, json_style, output_path, **kwargs):
    """
    Creates a JSON patch from the given ref.
    The patch can be applied with `kart apply`.
    """
    from .json_diff_writers import PatchWriter

    if ".." in refish:
        raise click.BadParameter(
            f"Can only create-patch for a single ref-ish - can't create-patch for {refish}",
            param_hint="refish",
        )
    commit_spec = f"{refish}^?...{refish}"

    repo = ctx.obj.get_repo(allowed_states=KartRepoState.ALL_STATES)
    diff_writer = PatchWriter(repo, commit_spec, [], output_path, json_style=json_style)
    diff_writer.include_target_commit_as_header()
    diff_writer.write_diff()
