import os
import sys

import click
import pygit2

from .base_diff_writer import BaseDiffWriter
from .key_filters import RepoKeyFilter
from .conflicts_writer import BaseConflictsWriter
from .crs_util import make_crs
from .exceptions import CrsError, GeometryError
from .geometry import geometry_from_string
from .merge_util import MergeContext, merge_status_to_text
from .output_util import dump_json_output
from .repo import KartRepoState
from .spatial_filter import SpatialFilter
from kart.cli_util import KartCommand


class StatusDiffWriter(BaseDiffWriter):
    """
    Counts inserts, updates, and deletes for each part of each dataset.
    Updates where the old value of the feature is outside the spatial don't count towards updates - instead, they are
    considered to be primaryKeyConflicts (that is, the user is probably accidentally reusing existing primary keys
    of the features outside the filter that they can't see.)
    """

    def __init__(self, repo):
        super().__init__(repo)

        if not self.spatial_filter.match_all:
            self.record_spatial_filter_stats = True
            self.spatial_filter_pk_conflicts = RepoKeyFilter()
        else:
            self.record_spatial_filter_stats = False
            self.spatial_filter_pk_conflicts = None

    def get_type_counts(self):
        """
        Gets a summary of changes - broken down first by dataset, then by dataset-part, then by changetype
        - one of "insert", "update", "delete" or unusually, "primaryKeyConflict".
        """
        repo_type_counts = {}

        for ds_path in self.all_ds_paths:
            ds_diff = self.get_dataset_diff(ds_path)
            ds_type_counts = ds_diff.type_counts()
            if not ds_type_counts:
                continue

            repo_type_counts[ds_path] = ds_type_counts
            feature_type_counts = ds_type_counts.get("feature")

            if (
                self.record_spatial_filter_stats
                and feature_type_counts
                and feature_type_counts.get("updates")
            ):
                self.record_spatial_filter_stats_for_dataset(ds_path, ds_diff)
                pk_conflicts = self.spatial_filter_pk_conflicts.recursive_get(
                    [ds_path, "feature"]
                )
                if pk_conflicts:
                    pk_conflicts_count = len(pk_conflicts)
                    feature_type_counts["primaryKeyConflicts"] = pk_conflicts_count
                    feature_type_counts["updates"] -= pk_conflicts_count
                    if not feature_type_counts["updates"]:
                        del feature_type_counts["updates"]

        return repo_type_counts


@click.command(cls=KartCommand)
@click.pass_context
@click.option(
    "--output-format",
    "-o",
    type=click.Choice(["text", "json"]),
    default="text",
)
def status(ctx, output_format):
    """Show the working copy status"""
    repo = ctx.obj.get_repo(allowed_states=KartRepoState.ALL_STATES)
    jdict = get_branch_status_json(repo)
    jdict["spatialFilter"] = SpatialFilter.load_repo_config(repo)
    if output_format == "json":
        jdict["spatialFilter"] = spatial_filter_status_to_json(jdict["spatialFilter"])

    if repo.state == KartRepoState.MERGING:
        merge_context = MergeContext.read_from_repo(repo)
        jdict["merging"] = merge_context.as_json()
        conflicts_writer_class = BaseConflictsWriter.get_conflicts_writer_class(
            output_format
        )
        conflicts_writer = conflicts_writer_class(repo, summarise=2)
        jdict["conflicts"] = conflicts_writer.list_conflicts()
        jdict["state"] = "merging"
    else:
        jdict["workingCopy"] = get_working_copy_status_json(repo)

    if output_format == "json":
        dump_json_output({"kart.status/v1": jdict}, sys.stdout)
    else:
        click.echo(status_to_text(jdict))


def get_branch_status_json(repo):
    output = {"commit": None, "abbrevCommit": None, "branch": None, "upstream": None}

    commit = repo.head_commit
    if commit:
        output["commit"] = commit.id.hex
        output["abbrevCommit"] = commit.short_id

    output["branch"] = repo.head_branch_shorthand
    if not repo.head_is_unborn and not repo.head_is_detached:
        branch = repo.branches[repo.head_branch_shorthand]
        upstream = branch.upstream

        if upstream:
            upstream_head = upstream.peel(pygit2.Commit)
            n_ahead, n_behind = repo.ahead_behind(commit.id, upstream_head.id)
            output["upstream"] = {
                "branch": upstream.shorthand,
                "ahead": n_ahead,
                "behind": n_behind,
            }
    return output


def get_working_copy_status_json(repo):
    if repo.is_bare:
        return None

    # TODO: this JSON needs to be updated now that the WC has more than one part.
    table_wc = repo.working_copy.tabular
    table_wc_path = table_wc.clean_location if table_wc else None

    result = {"path": table_wc_path, "changes": get_diff_status_json(repo)}

    # If we're not doing experimental point clouds, keep the JSON how it was in Kart 0.11 and earlier...
    if not os.environ.get("X_KART_POINT_CLOUDS"):
        # Don't show any WC status at all if there's no "path" for the tabular part.
        if result["path"] is None:
            return None
        # If there are no changes, show changes null rather than an empty dict.
        if not result["changes"]:
            result["changes"] = None

    return result


def get_diff_status_json(repo):
    """
    Returns a structured count of all the inserts, updates, and deletes (and primaryKeyConflicts) for meta items
    or  features in each dataset.
    """
    if not repo.working_copy.exists():
        return {}

    status_diff_writer = StatusDiffWriter(repo)
    return status_diff_writer.get_type_counts()


def status_to_text(jdict):
    status_list = [branch_status_to_text(jdict)]
    is_spatial_filter = bool(jdict["spatialFilter"])
    is_empty = not jdict["commit"]
    is_merging = jdict.get("state", None) == KartRepoState.MERGING.value

    if is_spatial_filter:
        status_list.append(spatial_filter_status_to_text(jdict["spatialFilter"]))

    if is_merging:
        status_list.append(merge_status_to_text(jdict, fresh=False))

    if not is_merging and not is_empty:
        status_list.append(working_copy_status_to_text(jdict["workingCopy"]))

    return "\n\n".join(status_list)


def branch_status_to_text(jdict):
    commit = jdict["abbrevCommit"]
    if not commit:
        return 'Empty repository.\n  (use "kart import" to add some data)'
    branch = jdict["branch"]
    if not branch:
        return f"{click.style('HEAD detached at', fg='red')} {commit}"
    output = f"On branch {branch}"

    upstream = jdict["upstream"]
    if upstream:
        output = "\n".join([output, upstream_status_to_text(upstream)])
    return output


def upstream_status_to_text(jdict):
    upstream_branch = jdict["branch"]
    n_ahead = jdict["ahead"]
    n_behind = jdict["behind"]

    if n_ahead == n_behind == 0:
        return f"Your branch is up to date with '{upstream_branch}'."
    elif n_ahead > 0 and n_behind > 0:
        return (
            f"Your branch and '{upstream_branch}' have diverged,\n"
            f"and have {n_ahead} and {n_behind} different commits each, respectively.\n"
            '  (use "kart pull" to merge the remote branch into yours)'
        )
    elif n_ahead > 0:
        return (
            f"Your branch is ahead of '{upstream_branch}' by {n_ahead} {_pc(n_ahead)}.\n"
            '  (use "kart push" to publish your local commits)'
        )
    elif n_behind > 0:
        return (
            f"Your branch is behind '{upstream_branch}' by {n_behind} {_pc(n_behind)}, "
            "and can be fast-forwarded.\n"
            '  (use "kart pull" to update your local branch)'
        )


def spatial_filter_status_to_json(jdict):
    # We always try to return hexwkb geometries for JSON output, regardless of how the geometry is stored.
    # Apart from that, the data we have is already dumpable as JSON.
    if jdict is None or "geometry" not in jdict:
        return jdict

    result = jdict.copy()

    ctx = "spatial filter"
    if "reference" in jdict:
        ctx += f" at reference {jdict['reference']} "

    try:
        geometry = geometry_from_string(jdict["geometry"], context=ctx)
        assert geometry is not None
        result["geometry"] = geometry.to_hex_wkb()
    except GeometryError:
        click.echo("Repo config contains unparseable spatial filter", err=True)

    return result


def spatial_filter_status_to_text(jdict):
    from osgeo import osr

    spatial_filter_desc = "spatial filter"
    if "reference" in jdict:
        spatial_filter_desc += f" at reference {jdict['reference']} "

    ctx = spatial_filter_desc
    try:
        geometry = geometry_from_string(jdict["geometry"], context=ctx)
    except GeometryError:
        return "Repo config contains unparseable spatial filter"

    try:
        crs = make_crs(jdict["crs"], context=ctx)
    except CrsError:
        return "Repo config contains spatial filter with invalid CRS"

    try:
        transform = osr.CoordinateTransformation(crs, make_crs("EPSG:4326"))
        geom_ogr = geometry.to_ogr()
        geom_ogr.Transform(transform)
        w, e, s, n = geom_ogr.GetEnvelope()
        envelope = f"[{w:.3f}, {s:.3f}, {e:.3f}, {n:.3f}]"

        return f"A {spatial_filter_desc} is active, limiting repo to a specific region inside {envelope}"

    except RuntimeError:
        return "Repo config contains unworkable spatial filter - can't reproject spatial filter into EPSG:4326"


def working_copy_status_to_text(jdict):
    if jdict is None:
        return 'No working copy\n  (use "kart checkout" to create a working copy)\n'

    if not jdict["changes"]:
        return "Nothing to commit, working copy clean"

    return (
        "Changes in working copy:\n"
        '  (use "kart commit" to commit)\n'
        '  (use "kart restore" to discard changes)\n\n'
        + diff_status_to_text(jdict["changes"])
    )


def diff_status_to_text(jdict):
    change_types = (
        ("inserts", "inserts"),
        ("updates", "updates"),
        ("deletes", "deletes"),
        ("primaryKeyConflicts", "primary key conflicts"),
    )

    message = []
    for dataset_path, dataset_changes in jdict.items():
        message.append(f"  {dataset_path}:")
        for dataset_part in ("meta", "feature", "tile"):
            if dataset_part not in dataset_changes:
                continue
            message.append(f"    {dataset_part}:")
            dataset_part_changes = dataset_changes[dataset_part]
            for json_type, change_type in change_types:
                if json_type not in dataset_part_changes:
                    continue
                change_type_count = dataset_part_changes[json_type]
                message.append(f"      {change_type_count} {change_type}")

    return "\n".join(message)


def feature_change_message(message, feature_changes, key):
    n = feature_changes.get(key)
    label = f"    {key}:"
    col_width = 15
    if n:
        message.append(f"{label: <{col_width}}{n} {_pf(n)}")


def get_branch_status_message(repo):
    return branch_status_to_text(get_branch_status_json(repo))


def get_diff_status_message(diff):
    """Given a diff.Diff, return a status message describing it."""
    return diff_status_to_text(diff.type_counts())


def _pf(count):
    """Simple pluraliser for feature/features"""
    if count == 1:
        return "feature"
    else:
        return "features"


def _pc(count):
    """Simple pluraliser for commit/commits"""
    if count == 1:
        return "commit"
    else:
        return "commits"
