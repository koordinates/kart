import logging
import sys

import click

from .crs_util import CoordinateReferenceString
from .diff_structs import RepoDiff, DatasetDiff
from .key_filters import RepoKeyFilter, DatasetKeyFilter
from .output_util import dump_json_output
from .repo import KartRepoState
from . import diff_estimation


L = logging.getLogger("kart.diff")


def get_dataset_diff(
    base_rs, target_rs, working_copy, dataset_path, ds_filter=DatasetKeyFilter.MATCH_ALL
):
    diff = DatasetDiff()

    if base_rs != target_rs:
        # diff += base_rs<>target_rs
        base_ds = base_rs.datasets.get(dataset_path)
        target_ds = target_rs.datasets.get(dataset_path)

        params = {}
        if not base_ds:
            base_ds, target_ds = target_ds, base_ds
            params["reverse"] = True

        diff_cc = base_ds.diff(target_ds, ds_filter=ds_filter, **params)
        L.debug("commit<>commit diff (%s): %s", dataset_path, repr(diff_cc))
        diff += diff_cc

    if working_copy:
        # diff += target_rs<>working_copy
        target_ds = target_rs.datasets.get(dataset_path)
        diff_wc = working_copy.diff_db_to_tree(target_ds, ds_filter=ds_filter)
        L.debug(
            "commit<>working_copy diff (%s): %s",
            dataset_path,
            repr(diff_wc),
        )
        diff += diff_wc

    diff.prune()
    return diff


def get_repo_diff(base_rs, target_rs, repo_key_filter=RepoKeyFilter.MATCH_ALL):
    """Generates a Diff for every dataset in both RepoStructures."""
    base_ds_paths = {ds.path for ds in base_rs.datasets}
    target_ds_paths = {ds.path for ds in target_rs.datasets}
    all_ds_paths = base_ds_paths | target_ds_paths

    if not repo_key_filter.match_all:
        all_ds_paths = all_ds_paths & repo_key_filter.keys()

    result = RepoDiff()
    for ds_path in sorted(all_ds_paths):
        ds_diff = get_dataset_diff(
            base_rs, target_rs, None, ds_path, repo_key_filter[ds_path]
        )
        result[ds_path] = ds_diff

    result.prune()
    return result


def feature_count_diff(
    ctx,
    output_format,
    commit_spec,
    output_path,
    exit_code,
    json_style,
    accuracy,
):
    if output_format not in ("text", "json"):
        raise click.UsageError("--only-feature-count requires text or json output")

    repo = ctx.obj.repo
    from .base_diff_writer import BaseDiffWriter

    base_rs, target_rs, working_copy = BaseDiffWriter._parse_diff_commit_spec(
        repo, commit_spec
    )

    dataset_change_counts = diff_estimation.estimate_diff_feature_counts(
        base_rs, target_rs, working_copy=working_copy, accuracy=accuracy
    )

    if output_format == "text":
        if dataset_change_counts:
            for dataset_name, count in sorted(dataset_change_counts.items()):
                click.secho(f"{dataset_name}:", bold=True)
                click.echo(f"\t{count} features changed")
        else:
            click.echo("0 features changed")
    elif output_format == "json":
        dump_json_output(dataset_change_counts, output_path, json_style=json_style)
    if dataset_change_counts and exit_code:
        sys.exit(1)


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
@click.argument("commit_spec", required=False, nargs=1)
@click.argument("filters", nargs=-1)
def diff(
    ctx,
    output_format,
    crs,
    output_path,
    exit_code,
    json_style,
    only_feature_count,
    commit_spec,
    filters,
):
    """
    Show changes between two commits, or between a commit and the working copy.

    COMMIT_SPEC -

    - if not supplied, the default is HEAD, to diff between HEAD and the working copy.

    - if a single ref is supplied: commit-A - diffs between commit-A and the working copy.

    - if supplied with the form: commit-A...commit-B - diffs between commit-A and commit-B.

    - if supplied with the form: commit-A..commit-B - diffs between (the common ancestor of
    commit-A and commit-B) and (commit-B).

    To list only particular conflicts, supply one or more FILTERS of the form [DATASET[:PRIMARY_KEY]]
    """
    if only_feature_count:
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
    diff_writer.write_diff()

    if exit_code or output_format == "quiet":
        diff_writer.exit_with_code()
