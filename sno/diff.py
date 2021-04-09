import logging
import re
import statistics
import subprocess
import sys
import time
from pathlib import Path

import click

from .crs_util import CoordinateReferenceString
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
from .output_util import dump_json_output
from .repo import SnoRepoState


L = logging.getLogger("sno.diff")


def get_dataset_diff(
    base_rs, target_rs, working_copy, dataset_path, ds_filter=UNFILTERED
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


def get_repo_diff(base_rs, target_rs, feature_filter=UNFILTERED):
    """Generates a Diff for every dataset in both RepoStructures."""
    base_ds_paths = {ds.path for ds in base_rs.datasets}
    target_ds_paths = {ds.path for ds in target_rs.datasets}
    all_ds_paths = base_ds_paths | target_ds_paths

    if feature_filter is not UNFILTERED:
        all_ds_paths = all_ds_paths.intersection(feature_filter.keys())

    result = RepoDiff()
    for ds_path in sorted(all_ds_paths):
        ds_diff = get_dataset_diff(
            base_rs, target_rs, None, ds_path, feature_filter[ds_path]
        )
        result[ds_path] = ds_diff

    result.prune()
    return result


def get_common_ancestor(repo, rs1, rs2):
    for rs in rs1, rs2:
        if not rs.commit:
            raise click.UsageError(
                f"The .. operator works on commits, not trees - {rs.id} is a tree. (Perhaps try the ... operator)"
            )
    ancestor_id = repo.merge_base(rs1.id, rs2.id)
    if not ancestor_id:
        raise InvalidOperation(
            "The .. operator tries to find the common ancestor, but no common ancestor was found. Perhaps try the ... operator."
        )
    return repo.structure(ancestor_id)


def _parse_diff_commit_spec(repo, commit_spec):
    # Parse <commit> or <commit>...<commit>
    commit_spec = commit_spec or "HEAD"
    commit_parts = re.split(r"(\.{2,3})", commit_spec)

    if len(commit_parts) == 3:
        # Two commits specified - base and target. We diff base<>target.
        base_rs = repo.structure(commit_parts[0] or "HEAD")
        target_rs = repo.structure(commit_parts[2] or "HEAD")
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
        base_rs = repo.structure(commit_parts[0])
        target_rs = repo.structure("HEAD")
        working_copy = repo.working_copy
        if not working_copy:
            raise NotFound("No working copy", exit_code=NO_WORKING_COPY)
        working_copy.assert_db_tree_match(target_rs.tree)
    return base_rs, target_rs, working_copy


def diff_with_writer(
    ctx,
    diff_writer,
    *,
    output_path="-",
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
    try:
        if isinstance(output_path, str) and output_path != "-":
            output_path = Path(output_path).expanduser()

        repo = ctx.obj.get_repo(allowed_states=SnoRepoState.ALL_STATES)

        base_rs, target_rs, working_copy = _parse_diff_commit_spec(repo, commit_spec)

        # Parse [<dataset>[:pk]...]
        feature_filter = build_feature_filter(filters)

        base_str = base_rs.id
        target_str = "working-copy" if working_copy else target_rs.id
        L.debug("base=%s target=%s", base_str, target_str)

        base_ds_paths = {ds.path for ds in base_rs.datasets}
        target_ds_paths = {ds.path for ds in target_rs.datasets}
        all_ds_paths = base_ds_paths | target_ds_paths

        if feature_filter is not UNFILTERED:
            all_ds_paths = all_ds_paths.intersection(feature_filter.keys())

        dataset_geometry_transforms = {}
        if target_crs is not None:
            for ds_path in all_ds_paths:
                ds = base_rs.datasets.get(ds_path) or target_rs.datasets.get(ds_path)
                transform = ds.get_geometry_transform(target_crs)
                if transform is not None:
                    dataset_geometry_transforms[ds_path] = transform

        writer_params = {
            "repo": repo,
            "base": base_rs,
            "target": target_rs,
            "output_path": output_path,
            "dataset_count": len(all_ds_paths),
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
            for ds_path in all_ds_paths:
                diff = get_dataset_diff(
                    base_rs,
                    target_rs,
                    working_copy,
                    ds_path,
                    feature_filter[ds_path],
                )
                ds = base_rs.datasets.get(ds_path) or target_rs.datasets.get(ds_path)
                num_changes += len(diff)
                L.debug("overall diff (%s): %s", ds_path, repr(diff))
                w(ds, diff)

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


FEATURE_SUBTREES_PER_TREE = 256
FEATURE_TREE_NESTING = 2
MAX_TREES = FEATURE_SUBTREES_PER_TREE ** FEATURE_TREE_NESTING


def _feature_count_sample_trees(rev_spec, feature_path, num_trees):
    num_full_subtrees = num_trees // 256
    paths = [f"{feature_path}{n:02x}" for n in range(num_full_subtrees)]
    paths.extend(
        [
            f"{feature_path}{num_full_subtrees:02x}/{n:02x}"
            for n in range(num_trees % 256)
        ]
    )

    p = subprocess.Popen(
        [
            "git",
            "diff",
            "--name-only",
            "--no-renames",
            rev_spec,
            "--",
            *paths,
        ],
        stdout=subprocess.PIPE,
        encoding="utf-8",
    )
    tree_samples = {}
    for line in p.stdout:
        # path/to/dataset/.sno-dataset/feature/ab/cd/abcdef123
        # --> ab/cd
        root, tree, subtree, basename = line.rsplit("/", 3)
        k = f"{tree}/{subtree}"
        tree_samples.setdefault(k, 0)
        tree_samples[k] += 1
    p.wait()
    r = list(tree_samples.values())
    r.extend([0] * (num_trees - len(r)))
    return r


Z_SCORES = {
    0.50: 0.0,
    0.60: 0.26,
    0.70: 0.53,
    0.75: 0.68,
    0.80: 0.85,
    0.85: 1.04,
    0.90: 1.29,
    0.95: 1.65,
    0.99: 2.33,
}


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
    base_rs, target_rs, working_copy = _parse_diff_commit_spec(repo, commit_spec)
    if working_copy:
        raise NotImplementedError(
            "--only-feature-count isn't supported for working-copy diffs yet"
        )

    if base_rs == target_rs:
        return {}

    base_ds_paths = {ds.path for ds in base_rs.datasets}
    target_ds_paths = {ds.path for ds in target_rs.datasets}
    all_ds_paths = base_ds_paths | target_ds_paths
    rev_spec = f"{base_rs.tree.id}..{target_rs.tree.id}"

    dataset_change_counts = {}
    for dataset_path in all_ds_paths:
        base_ds = base_rs.datasets.get(dataset_path)
        target_ds = target_rs.datasets.get(dataset_path)

        if not base_ds:
            base_ds, target_ds = target_ds, base_ds
        elif target_ds:
            if base_ds.feature_tree == target_ds.feature_tree:
                continue

        # Come up with a list of trees to diff.
        # TODO: decouple this stuff from dataset2 a bit (?)
        feature_path = f"{base_ds.path}/{base_ds.FEATURE_PATH}"
        if accuracy == "exact":
            ds_total = sum(
                _feature_count_sample_trees(rev_spec, feature_path, MAX_TREES)
            )
        else:
            if accuracy == "veryfast":
                # only ever sample two trees
                sample_size = 2
                required_confidence = 0.00001
                z_score = 0.0
            else:
                if accuracy == "fast":
                    sample_size = 2
                    required_confidence = 0.60
                elif accuracy == "medium":
                    sample_size = 8
                    required_confidence = 0.80
                elif accuracy == "good":
                    sample_size = 16
                    required_confidence = 0.95
                z_score = Z_SCORES[required_confidence]

            confidence_interval = (-1, -1)
            sample_mean = 0
            while sample_size <= MAX_TREES and (
                sample_mean < confidence_interval[0]
                or sample_mean > confidence_interval[1]
            ):
                L.debug(f"sampling %d trees for dataset %s", sample_size, dataset_path)
                t1 = time.monotonic()
                samples = _feature_count_sample_trees(
                    rev_spec, feature_path, sample_size
                )
                sample_mean = statistics.mean(samples)
                sample_stdev = statistics.stdev(samples)

                t2 = time.monotonic()
                if accuracy == "veryfast":
                    # even if no features were found in the two trees, call it done.
                    # this will be Good Enough if all you need to know is something like
                    # "is the diff size probably less than 100K features?"
                    break
                if sample_mean == 0:
                    # no features were encountered in the sample.
                    # this is likely a very small diff.
                    # let's just sample a lot more trees.
                    new_sample_size = sample_size * 1024
                    if new_sample_size > MAX_TREES:
                        L.debug(
                            "sampled %s trees in %.3fs, found 0 features; stopping",
                            sample_size,
                            t2 - t1,
                        )
                    else:
                        L.debug(
                            "sampled %s trees in %.3fs, found 0 features; increased sample size to %d",
                            sample_size,
                            t2 - t1,
                            new_sample_size,
                        )
                    sample_size = new_sample_size
                    continue

                # try and get within 10% of the real mean.
                margin_of_error = 0.10 * sample_mean
                required_sample_size = min(
                    MAX_TREES, (z_score * sample_stdev / margin_of_error) ** 2
                )
                L.debug(
                    "sampled %s trees in %.3fs (ƛ=%.3f, s=%.3f). required: %.1f (margin: %.1f; confidence: %d%%)",
                    sample_size,
                    t2 - t1,
                    sample_mean,
                    sample_stdev,
                    required_sample_size,
                    margin_of_error * MAX_TREES,
                    required_confidence * 100,
                )
                if sample_size >= required_sample_size:
                    break

                if sample_size == MAX_TREES:
                    break
                while sample_size < required_sample_size:
                    sample_size *= 2
                sample_size = min(MAX_TREES, sample_size)
            ds_total = int(round(sample_mean * MAX_TREES))

        if ds_total:
            dataset_change_counts[dataset_path] = ds_total

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
    type=click.Choice(["text", "json", "geojson", "quiet", "feature-count", "html"]),
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
    type=click.Choice(["veryfast", "fast", "medium", "good", "exact"]),
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
