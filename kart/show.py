import click

from kart.completion_shared import ref_completer

from kart import diff_estimation
from kart.cli_util import KartCommand, OutputFormatType
from kart.completion_shared import ref_or_repo_path_completer
from kart.crs_util import CoordinateReferenceString
from kart.diff_format import DiffFormat
from kart.parse_args import PreserveDoubleDash, parse_revisions_and_filters
from kart.repo import KartRepoState


@click.command(cls=PreserveDoubleDash)
@click.pass_context
@click.option(
    "--output-format",
    "-o",
    type=OutputFormatType(
        output_types=[
            "text",
            "json",
            "geojson",
            "quiet",
            "feature-count",
            "html",
            "json-lines",
        ],
        # TODO: minor thing, but this should really be True.
        # `git show --format=%H` works; no particular reason it shouldn't in Kart.
        # (except I didn't get around to implementing it.
        # it might be easier to do after moving `log` implementation in-house, since we'll
        # presumably have some function that interprets the formatstrings.)
        allow_text_formatstring=False,
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
    "--only-feature-count",
    default=None,
    type=click.Choice(diff_estimation.ACCURACY_CHOICES),
    help=(
        "Returns only a feature count (the number of features modified in this diff). "
        "If the value is 'exact', the feature count is exact (this may be slow.) "
        "Otherwise, the feature count will be approximated with varying levels of accuracy."
    ),
)
@click.option(
    "--diff-files",
    is_flag=True,
    help="Show changes to file contents (instead of just showing the object IDs of changed files)",
)
@click.argument(
    "args",
    metavar="[REVISION] [--] [FILTERS]",
    nargs=-1,
    type=click.UNPROCESSED,
    shell_complete=ref_or_repo_path_completer,
)
@click.option(
    "--diff-format",
    type=click.Choice(DiffFormat),
    default=DiffFormat.FULL,
    help="Choose the diff format: \n'full' for full diff, 'none' for viewing commit metadata only, or 'no-data-changes' for metadata and a bool indicating the feature/tile tree changes.",
)
def show(
    ctx,
    *,
    output_format,
    crs,
    output_path,
    exit_code,
    only_feature_count,
    diff_files,
    args,
    diff_format=DiffFormat.FULL,
):
    """
    Shows the given REVISION, or HEAD if none is specified.

    To list only particular changes, supply one or more FILTERS of the form [DATASET[:PRIMARY_KEY]]
    """
    repo = ctx.obj.get_repo(allowed_states=KartRepoState.ALL_STATES)
    options, commits, filters = parse_revisions_and_filters(repo, args)

    if len(commits) > 1:
        raise click.BadParameter(
            f"Can only show a single revision - can't show {', '.join(commits)}"
        )
    commit = commits[0] if commits else "HEAD"
    if ".." in commit:
        raise click.BadParameter(
            f"Can only show a single revision - can't show {commit}"
        )

    commit_spec = f"{commit}^?...{commit}"
    output_type, fmt = output_format

    if only_feature_count:
        from .diff import feature_count_diff

        return feature_count_diff(
            repo,
            output_type,
            commit_spec,
            output_path,
            exit_code,
            fmt,
            only_feature_count,
        )

    from .base_diff_writer import BaseDiffWriter

    diff_writer_class = BaseDiffWriter.get_diff_writer_class(output_type)
    diff_writer = diff_writer_class(
        repo, commit_spec, filters, output_path, json_style=fmt, target_crs=crs
    )
    diff_writer.full_file_diffs(diff_files)
    diff_writer.include_target_commit_as_header()
    diff_writer.write_diff(diff_format=diff_format)

    if exit_code or output_type == "quiet":
        diff_writer.exit_with_code()


@click.command(cls=KartCommand, name="create-patch")
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
@click.option(
    "--patch-type",
    type=click.Choice(["full", "minimal"]),
    default="full",
    help=(
        "Style of patch to produce. 'full' is the default and most applyable, but is quite a verbose patch. "
        "'minimal' creates a much smaller patch by omitting the 'old' version of edits, "
        "but 'minimal' patches are only applyable if the parent commit is present in the target repo."
    ),
)
@click.option(
    "--diff-format",
    type=click.Choice(["none", "full", "no-data-changes"]),
    default="full",
    help="Choose the diff format",
)
# NOTE: this is *required* for now.
# A future version might create patches from working-copy changes.
@click.argument("refish", shell_complete=ref_completer)
def create_patch(
    ctx,
    *,
    refish,
    json_style,
    output_path,
    patch_type,
    diff_format=DiffFormat.FULL,
    **kwargs,
):
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
    diff_writer = PatchWriter(
        repo,
        commit_spec,
        [],
        output_path,
        json_style=json_style,
        patch_type=patch_type,
    )
    diff_writer.full_file_diffs(True)
    diff_writer.include_target_commit_as_header()
    diff_writer.write_diff(diff_format=diff_format)
