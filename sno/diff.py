import logging
import re
import sys
from pathlib import Path

import click
from osgeo import osr

from .cli_util import StringFromFile
from .diff_output import (  # noqa - used from globals()
    diff_output_text,
    diff_output_json,
    diff_output_geojson,
    diff_output_quiet,
    diff_output_html,
)
from .diff_structs import RepoDiff, DatasetDiff
from .exceptions import (
    InvalidOperation,
    NotFound,
    NO_WORKING_COPY,
    UNCATEGORIZED_ERROR,
)
from .filter_util import build_feature_filter, UNFILTERED
from .geometry import make_crs
from .repo_files import RepoState
from .structure import RepositoryStructure


L = logging.getLogger("sno.diff")


def get_dataset_diff(
    base_rs, target_rs, working_copy, dataset_path, ds_filter=UNFILTERED
):
    diff = DatasetDiff()

    if base_rs != target_rs:
        # diff += base_rs<>target_rs
        base_ds = base_rs.get(dataset_path)
        target_ds = target_rs.get(dataset_path)

        params = {}
        if not base_ds:
            base_ds, target_ds = target_ds, base_ds
            params["reverse"] = True

        diff_cc = base_ds.diff(target_ds, ds_filter=ds_filter, **params)
        L.debug("commit<>commit diff (%s): %s", dataset_path, repr(diff_cc))
        diff += diff_cc

    if working_copy:
        # diff += target_rs<>working_copy
        target_ds = target_rs.get(dataset_path)
        diff_wc = working_copy.diff_db_to_tree(target_ds, ds_filter=ds_filter)
        L.debug(
            "commit<>working_copy diff (%s): %s", dataset_path, repr(diff_wc),
        )
        diff += diff_wc

    diff.prune()
    return diff


def get_repo_diff(base_rs, target_rs, feature_filter=UNFILTERED):
    """Generates a Diff for every dataset in both RepositoryStructures."""
    all_datasets = {ds.path for ds in base_rs} | {ds.path for ds in target_rs}

    if feature_filter is not UNFILTERED:
        all_datasets = all_datasets.intersection(feature_filter.keys())

    result = RepoDiff()
    for dataset in sorted(all_datasets):
        ds_diff = get_dataset_diff(
            base_rs, target_rs, None, dataset, feature_filter[dataset]
        )
        result[dataset] = ds_diff

    result.prune()
    return result


def get_common_ancestor(repo, rs1, rs2):
    for rs in rs1, rs2:
        if not rs.head_commit:
            raise click.UsageError(
                f"The .. operator works on commits, not trees - {rs.id} is a tree. (Perhaps try the ... operator)"
            )
    ancestor_id = repo.merge_base(rs1.id, rs2.id)
    if not ancestor_id:
        raise InvalidOperation(
            "The .. operator tries to find the common ancestor, but no common ancestor was found. Perhaps try the ... operator."
        )
    return RepositoryStructure.lookup(repo, ancestor_id)


def diff_with_writer(
    ctx,
    diff_writer,
    *,
    output_path='-',
    exit_code,
    json_style="pretty",
    commit_spec,
    filters,
    target_crs=None,
):
    """
    Calculates the appropriate diff from the arguments,
    and writes it using the given writer contextmanager.

      ctx: the click context
      diff_writer: One of the `diff_output_*` contextmanager factories.
                   When used as a contextmanager, the diff_writer should yield
                   another callable which accepts (dataset, diff) arguments
                   and writes the output by the time it exits.
      output_path: The output path, or a file-like object, or the string '-' to use stdout.
      exit_code:   If True, the process will exit with code 1 if the diff is non-empty.
      commit_spec: The commit-ref or -refs to diff.
      filters:     Limit the diff to certain datasets or features.
      target_crs:  An osr.SpatialReference object, or None
    """
    from .working_copy import WorkingCopy

    try:
        if isinstance(output_path, str) and output_path != "-":
            output_path = Path(output_path).expanduser()

        repo = ctx.obj.get_repo(allowed_states=RepoState.ALL_STATES)

        # Parse <commit> or <commit>...<commit>
        commit_spec = commit_spec or "HEAD"
        commit_parts = re.split(r"(\.{2,3})", commit_spec)

        if len(commit_parts) == 3:
            # Two commits specified - base and target. We diff base<>target.
            base_rs = RepositoryStructure.lookup(repo, commit_parts[0] or "HEAD")
            target_rs = RepositoryStructure.lookup(repo, commit_parts[2] or "HEAD")
            if commit_parts[1] == "..":
                # A   C    A...C is A<>C
                #  \ /     A..C  is B<>C
                #   B      (git log semantics)
                base_rs = get_common_ancestor(repo, base_rs, target_rs)
            working_copy = None
        else:
            # When one commit is specified, it is base, and we diff base<>working_copy.
            # When no commits are specified, base is HEAD, and we do the same.
            # We diff base<>working_copy by diffing base<>target + target<>working_copy,
            # and target is set to HEAD.
            base_rs = RepositoryStructure.lookup(repo, commit_parts[0])
            target_rs = RepositoryStructure.lookup(repo, "HEAD")
            working_copy = WorkingCopy.get(repo)
            if not working_copy:
                raise NotFound("No working copy", exit_code=NO_WORKING_COPY)
            working_copy.assert_db_tree_match(target_rs.tree)

        # Parse [<dataset>[:pk]...]
        feature_filter = build_feature_filter(filters)

        base_str = base_rs.id
        target_str = "working-copy" if working_copy else target_rs.id
        L.debug('base=%s target=%s', base_str, target_str)

        all_datasets = {ds.path for ds in base_rs} | {ds.path for ds in target_rs}

        if feature_filter is not UNFILTERED:
            all_datasets = all_datasets.intersection(feature_filter.keys())

        dataset_geometry_transforms = {}
        if target_crs is not None:
            for dataset_path in all_datasets:
                dataset = base_rs.get(dataset_path) or target_rs.get(dataset_path)
                crs_wkt = dataset.crs_wkt
                if crs_wkt is not None:
                    src_crs = make_crs(crs_wkt)
                    try:
                        transform = osr.CoordinateTransformation(src_crs, target_crs)
                    except RuntimeError as e:
                        raise InvalidOperation(
                            f"Can't reproject dataset {dataset_path!r} into target CRS: {e}"
                        )

                    dataset_geometry_transforms[dataset_path] = transform

        writer_params = {
            "repo": repo,
            "base": base_rs,
            "target": target_rs,
            "output_path": output_path,
            "dataset_count": len(all_datasets),
            "json_style": json_style,
            "dataset_geometry_transforms": dataset_geometry_transforms,
        }

        L.debug(
            "base_rs %s == target_rs %s: %s",
            repr(base_rs),
            repr(target_rs),
            base_rs == target_rs,
        )

        num_changes = 0
        with diff_writer(**writer_params) as w:
            for dataset_path in all_datasets:
                diff = get_dataset_diff(
                    base_rs,
                    target_rs,
                    working_copy,
                    dataset_path,
                    feature_filter[dataset_path],
                )
                dataset = base_rs.get(dataset_path) or target_rs.get(dataset_path)
                num_changes += len(diff)
                L.debug("overall diff (%s): %s", dataset_path, repr(diff))
                w(dataset, diff)

    except click.ClickException as e:
        L.debug("Caught ClickException: %s", e)
        if exit_code and e.exit_code == 1:
            e.exit_code = UNCATEGORIZED_ERROR
        raise
    except Exception as e:
        L.debug("Caught non-ClickException: %s", e)
        if exit_code:
            click.secho(f"Error: {e}", fg="red", file=sys.stderr)
            raise SystemExit(UNCATEGORIZED_ERROR) from e
        else:
            raise
    else:
        if exit_code and num_changes:
            sys.exit(1)


class CoordinateReferenceString(StringFromFile):
    def convert(self, value, param, ctx):
        value = super().convert(value, param, ctx)

        try:
            return make_crs(value)
        except RuntimeError as e:
            self.fail(
                f"Invalid or unknown coordinate reference system: {value!r} ({e})"
            )


@click.command()
@click.pass_context
@click.option(
    "--output-format",
    "-o",
    type=click.Choice(["text", "json", "geojson", "quiet", "html"]),
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
@click.argument("commit_spec", required=False, nargs=1)
@click.argument("filters", nargs=-1)
def diff(
    ctx, output_format, crs, output_path, exit_code, json_style, commit_spec, filters
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

    diff_writer = globals()[f"diff_output_{output_format}"]
    if output_format == "quiet":
        exit_code = True

    return diff_with_writer(
        ctx,
        diff_writer,
        output_path=output_path,
        exit_code=exit_code,
        json_style=json_style,
        commit_spec=commit_spec,
        filters=filters,
        target_crs=crs,
    )
