import copy
import json
import os
from pathlib import Path

import click
import pygit2

from kart.completion_shared import conflict_completer

from kart.cli_util import MutexOption, KartCommand
from kart.exceptions import NO_CONFLICT, InvalidOperation, NotFound, NotYetImplemented
from kart.lfs_util import pointer_file_bytes_to_dict, get_local_path_from_lfs_hash
from kart.geometry import geojson_to_gpkg_geom
from kart.merge_util import (
    MergeContext,
    MergedIndex,
    RichConflict,
    WorkingCopyMerger,
    rich_conflicts,
)
from kart.point_cloud.tilename_util import set_tile_extension
from kart.reflink_util import try_reflink
from kart.repo import KartRepoState
from kart.key_filters import RepoKeyFilter


def ungeojson_feature(feature, dataset):
    """Given a geojson feature belonging to dataset, returns the feature as a dict containing a gpkg geometry."""
    result = copy.deepcopy(feature["properties"])
    if dataset.geom_column_name:
        result[dataset.geom_column_name] = geojson_to_gpkg_geom(feature["geometry"])
    return result


def ungeojson_file(file_path, dataset):
    """
    Given a file containing multiple geojson features belonging to dataset,
    returns the features as dicts containing gpkg geometries.
    """
    features = json.load(file_path.open())["features"]
    return [ungeojson_feature(f, dataset) for f in features]


def write_feature_to_dataset_entry(feature, dataset, repo):
    """
    Adds the given feature to the given dataset by writing a blob to the Kart repo.
    Returns the IndexEntry that refers to that blob - this IndexEntry still needs
    to be written to the repo to complete the write.
    """
    feature_path, feature_data = dataset.encode_feature(feature)
    blob_id = repo.create_blob(feature_data)
    return pygit2.IndexEntry(feature_path, blob_id, pygit2.GIT_FILEMODE_BLOB)


def load_dataset(rich_conflict):
    """
    This loads the dataset as merged-so-far. We use this to serialise feature resolves, since
    they will need to be serialised in a way that is consistent with however the dataset it merged -
    ie, if we decide to accept their new schema, there's no point serialising features with our schema instead.
    """
    # TODO - we need to keep MERGED_TREE up to date with any schema.json resolves, and we need to force the user to
    # resolve meta conflicts before they resolve feature conflicts.
    sample_ds = rich_conflict.any_true_version.dataset
    return sample_ds.repo.datasets("MERGED_TREE")[sample_ds.path]


def load_file_resolve(rich_conflict, file_path):
    """Loads a feature from the given file in order to use it as a conflict resolution."""
    single_path = not rich_conflict.has_multiple_paths
    dataset_part = rich_conflict.decoded_path[1]
    if not single_path or dataset_part not in ("feature", "tile"):
        raise NotYetImplemented(
            "Sorry, only feature or tile conflicts can currently be resolved using --with-file"
        )

    dataset_part = rich_conflict.decoded_path[1]
    if dataset_part == "feature":
        return _load_file_resolve_for_feature(rich_conflict, file_path)
    elif dataset_part == "tile":
        return _load_file_resolve_for_tile(rich_conflict, file_path)
    else:
        raise RuntimeError()


def _load_file_resolve_for_feature(rich_conflict, file_path):
    dataset = load_dataset(rich_conflict)
    return [
        write_feature_to_dataset_entry(f, dataset, dataset.repo)
        for f in ungeojson_file(file_path, dataset)
    ]


def _load_file_resolve_for_tile(rich_conflict, file_path):
    from kart.lfs_util import get_local_path_from_lfs_hash, dict_to_pointer_file_bytes

    tilename = rich_conflict.decoded_path[2]
    dataset = load_dataset(rich_conflict)
    repo = dataset.repo
    rel_tile_path = os.path.relpath(file_path.resolve(), repo.workdir_path.resolve())
    tile_summary = dataset.extract_tile_metadata_from_filesystem_path(file_path)["tile"]
    if not dataset.is_tile_compatible(
        tile_summary, dataset.tile_metadata["format.json"]
    ):
        # TODO: maybe support type-conversion during resolves like we do during commits.
        raise InvalidOperation(
            f"The tile at {rel_tile_path} does not match the dataset's format"
        )

    path_in_lfs_cache = get_local_path_from_lfs_hash(repo, tile_summary["oid"])
    if not path_in_lfs_cache.is_file():
        path_in_lfs_cache.parents[0].mkdir(parents=True, exist_ok=True)
        try_reflink(file_path, path_in_lfs_cache)
    pointer_data = dict_to_pointer_file_bytes(tile_summary)
    blob_path = dataset.tilename_to_blob_path(tilename)
    blob_id = repo.create_blob(pointer_data)
    return [pygit2.IndexEntry(blob_path, blob_id, pygit2.GIT_FILEMODE_BLOB)]


def load_workingcopy_resolve(rich_conflict):
    """Loads a feature from the working copy in order to use it as a conflict resolution."""
    single_path = not rich_conflict.has_multiple_paths
    dataset_part = rich_conflict.decoded_path[1]
    if not single_path or dataset_part not in ("feature", "tile"):
        raise NotYetImplemented(
            "Sorry, only feature or tile conflicts can currently be resolved using --with=workingcopy"
        )

    dataset_part = rich_conflict.decoded_path[1]
    if dataset_part == "feature":
        return _load_workingcopy_resolve_for_feature(rich_conflict)
    elif dataset_part == "tile":
        return _load_workingcopy_resolve_for_tile(rich_conflict)
    else:
        raise RuntimeError()


def _load_workingcopy_resolve_for_feature(rich_conflict):
    dataset = load_dataset(rich_conflict)
    repo = dataset.repo
    table_wc = repo.working_copy.tabular
    pk = rich_conflict.decoded_path[2]
    feature = (
        table_wc.get_feature(dataset, pk, allow_schema_diff=False) if table_wc else None
    )
    if feature is None:
        raise NotFound(
            f"No feature found at {rich_conflict.label} - to resolve a conflict by deleting the feature, use --with=delete"
        )
    feature_path, feature_data = dataset.encode_feature(feature)
    blob_id = repo.create_blob(feature_data)
    return [pygit2.IndexEntry(feature_path, blob_id, pygit2.GIT_FILEMODE_BLOB)]


def _load_workingcopy_resolve_for_tile(rich_conflict):
    from kart.point_cloud.tilename_util import get_tile_path_pattern

    dataset = load_dataset(rich_conflict)
    repo = dataset.repo
    workdir = repo.working_copy.workdir
    tilename = rich_conflict.decoded_path[2]
    matching_files = []
    if workdir:
        # Get a glob that roughly matches the tiles we are looking for.
        matching_files = list((workdir.path / dataset.path).glob(f"**/{tilename}.*"))
        # Narrow it down more exactly using get_tile_path_pattern which allows for a few different extensions.
        filename_pattern = get_tile_path_pattern(tilename)
        matching_files = [
            p for p in matching_files if filename_pattern.fullmatch(p.name)
        ]

    if not matching_files:
        raise NotFound(
            f"No tile found at {rich_conflict.label} - to resolve a conflict by deleting the tile, use --with=delete"
        )
    if len(matching_files) > 1:
        click.echo(
            "Found multiple files in the working copy that could be intended as the resolution:",
            err=True,
        )
        for file in matching_files:
            click.echo(
                os.path.relpath(file.resolve(), repo.workdir_path.resolve()), err=True
            )
        raise InvalidOperation("Couldn't resolve conflict using working copy")
    return _load_file_resolve_for_tile(rich_conflict, matching_files[0])


def find_single_conflict_to_resolve(merged_index, merge_context, conflict_labels):
    """
    Given a single conflict label that the user wants to resolve - eg mydataset:feature:1 -
    loads the unresolved conflict from the merge index.
    Raises an error if there is no such conflict, or it is already resolved, or cannot yet be resolved,
    or if the given label(s) match more than one conflict.
    """
    if len(conflict_labels) == 0:
        raise click.UsageError("Missing argument: CONFLICT_LABEL")

    if len(conflict_labels) > 1:
        raise NotYetImplemented(
            "Sorry, resolving multiple conflicts at once is not yet supported (except when using --renumber)",
            exit_code=NO_CONFLICT,
        )

    conflict_label = conflict_labels[0]

    result = None
    label_parts = conflict_label.split(":")
    ds_path = label_parts[0]
    is_meta = len(label_parts) >= 2 and label_parts[1] == "meta"
    for key, conflict3 in merged_index.conflicts.items():
        rich_conflict = RichConflict((key, conflict3), merge_context)
        if key in merged_index.resolves:
            if rich_conflict.label == conflict_label:
                raise InvalidOperation(
                    f"Conflict at {conflict_label} is already resolved"
                )
            continue

        if rich_conflict.label == conflict_label:
            result = rich_conflict
        elif (
            (not is_meta)
            and rich_conflict.decoded_path[0] == ds_path
            and rich_conflict.decoded_path[1] == "meta"
        ):
            raise InvalidOperation(
                f"There are still unresolved meta-item conflicts for dataset {ds_path}. These need to be resolved first."
            )

    if result is None:
        if find_multiple_conflicts_to_resolve(
            merged_index, merge_context, [conflict_label]
        ):
            raise NotYetImplemented(
                "Sorry, resolving multiple conflicts at once is not yet supported (except when using --renumber)",
                exit_code=NO_CONFLICT,
            )
        else:
            raise NotFound(
                f"No conflict found at {conflict_label}", exit_code=NO_CONFLICT
            )

    return result


def find_multiple_conflicts_to_resolve(merged_index, merge_context, user_key_filters):
    """
    Given filters that match the conflicts the user wants to resolve - eg mydataset:feature -
    returns a list of all matching unresolved conflicts as RichConflicts, from the merge index.
    Returns an empty list if this doesn't match any unresolved conflicts.
    """
    repo_key_filter = RepoKeyFilter.build_from_user_patterns(user_key_filters)
    conflicts = rich_conflicts(
        merged_index.unresolved_conflicts.items(),
        merge_context,
    )
    conflicts = [c for c in conflicts if c.matches_filter(repo_key_filter)]
    return conflicts


def update_workingcopy_with_resolve(
    repo, merged_index, merge_context, rich_conflict, res
):
    ds_part = rich_conflict.decoded_path[1]
    if ds_part == "meta":
        # If a meta conflict has been resolved, we update the merged_tree and then reset the WC to it.
        working_copy_merger = WorkingCopyMerger(repo, merge_context)
        # The merged_tree is used mostly for updating the working copy, but is also used for
        # serialising feature resolves, so we write it even if there's no WC.
        merged_tree = working_copy_merger.write_merged_tree(merged_index)
        if repo.working_copy.exists():
            working_copy_merger.update_working_copy(merged_index, merged_tree)

    elif ds_part == "feature":
        wc = repo.working_copy.tabular
        if wc is None:
            return
        dataset = load_dataset(rich_conflict)
        features = [dataset.get_feature(path=r.path, data=repo[r.id]) for r in res]
        with wc.session() as sess:
            wc.delete_features(sess, rich_conflict.as_key_filter())
            if features:
                sess.execute(wc.insert_or_replace_into_dataset_cmd(dataset), features)

    elif ds_part == "tile":
        workdir = repo.working_copy.workdir
        if workdir is None:
            return
        dataset = load_dataset(rich_conflict)
        workdir.delete_tiles_for_dataset(
            dataset,
            rich_conflict.as_key_filter()[dataset.path],
            including_conflict_versions=True,
        )
        for r in res:
            tilename = dataset.tilename_from_path(r.path)
            pointer_dict = pointer_file_bytes_to_dict(repo[r.id])
            lfs_path = get_local_path_from_lfs_hash(repo, pointer_dict["oid"])
            filename = set_tile_extension(tilename, tile_format=pointer_dict)
            workdir_path = workdir.path / dataset.path / filename
            if workdir_path.is_file():
                workdir_path.unlink()
            try_reflink(lfs_path, workdir_path)


def resolve_conflicts_with_renumber(repo, renumber, conflict_labels):
    """
    Resolve one or more insert/insert conflicts by keeping one version unchanged,
    and renumbering the primary key value of the other version.
    Only works for feature conflicts with integer primary keys.

    repo - the kart repo
    renumber - one of "ours" or "theirs"
    conflict_labels - filter or specify the exact the conflicts to resolve.
        If not set, renumbers all possible insert/insert conflicts.
    """
    assert renumber in ("ours", "theirs")

    merged_index = MergedIndex.read_from_repo(repo)
    merge_context = MergeContext.read_from_repo(repo)

    matching_conflicts = find_renumber_conflicts_to_resolve(
        merged_index, merge_context, conflict_labels
    )
    ds_path_to_conflicts = {}
    for conflict in matching_conflicts:
        ds_path = conflict.any_true_version.dataset_path
        ds_path_to_conflicts.setdefault(ds_path, []).append(conflict)

    resolve_paths = [
        entry.path for resolve in merged_index.resolves.values() for entry in resolve
    ]

    structure = repo.structure()
    decoded_resolves = [structure.decode_path(p) for p in resolve_paths]
    ours_datasets = repo.datasets(merge_context.versions.ours.commit_id)
    theirs_datasets = repo.datasets(merge_context.versions.theirs.commit_id)
    merged_datasets = repo.datasets("MERGED_TREE")

    resolved_conflicts = 0
    ds_path_to_features = {}

    # Renumber either ours or theirs, write the resolves to the merge index.
    for ds_path, conflicts in ds_path_to_conflicts.items():
        next_unassigned = max(
            ours_datasets[ds_path].find_start_of_unassigned_range(),
            theirs_datasets[ds_path].find_start_of_unassigned_range(),
        )
        dataset = merged_datasets[ds_path]

        for decoded_path in decoded_resolves:
            if decoded_path[0] == ds_path and decoded_path[1] == "feature":
                next_unassigned = max(next_unassigned, decoded_path[2] + 1)

        ds_features = ds_path_to_features.setdefault(ds_path, [])

        for conflict in conflicts:
            if renumber == "ours":
                keep_version = conflict.versions.theirs
                renumber_version = conflict.versions.ours
            elif renumber == "theirs":
                keep_version = conflict.versions.ours
                renumber_version = conflict.versions.theirs
            else:
                raise RuntimeError()

            keep_feature = keep_version.feature
            renumber_feature = renumber_version.feature
            renumber_feature[dataset.primary_key] = next_unassigned
            next_unassigned += 1

            res = [
                write_feature_to_dataset_entry(f, dataset, dataset.repo)
                for f in (keep_feature, renumber_feature)
            ]

            merged_index.add_resolve(conflict.key, res)
            resolved_conflicts += 1
            ds_features.append(keep_feature)
            ds_features.append(renumber_feature)

    merged_index.write_to_repo(repo)

    # Update the working copy to contain the resolves.
    wc = repo.working_copy.tabular
    if wc is not None:
        with wc.session() as sess:
            for ds_path, features in ds_path_to_features.items():
                if not features:
                    continue
                dataset = merged_datasets[ds_path]
                sess.execute(wc.insert_or_replace_into_dataset_cmd(dataset), features)

    unresolved_conflicts = len(merged_index.unresolved_conflicts)
    click.echo(
        f"Resolved {_pc(resolved_conflicts)}. {_pc(unresolved_conflicts)} to go."
    )
    if unresolved_conflicts == 0:
        click.echo("Use `kart merge --continue` to complete the merge")


def find_renumber_conflicts_to_resolve(merged_index, merge_context, conflict_labels):
    """
    Given one or more conflict labels that the user wants to resolve by renumbering - eg mydataset:feature -
    loads the matching conflicts from the merge index.
    Helps the user find renumberable conflicts to some extent (ie, only matches insert/insert conflicts).
    Helps the user more if no conflict_labels are supplied (ie, only matches feature conflicts with integer PKs).
    Raises an error if there are no matching conflicts or if some matching conflicts cannot be renumbered.
    """
    conflicts = find_multiple_conflicts_to_resolve(
        merged_index, merge_context, conflict_labels
    )

    # Failed to find any matching conflicts - maybe already resolved?
    if not conflicts:
        if len(conflict_labels) == 1:
            conflict_label = conflict_labels[0]
            matching_resolved_conflicts = rich_conflicts(
                merged_index.resolved_conflicts, merge_context
            )
            if any(c.label == conflict_label for c in matching_resolved_conflicts):
                raise InvalidOperation(
                    f"Conflict at {conflict_label} is already resolved"
                )
            else:
                raise NotFound(
                    f"No matching conflict(s) found at {conflict_label}",
                    exit_code=NO_CONFLICT,
                )
        else:
            raise NotFound("No matching conflict(s) found", exit_code=NO_CONFLICT)

    if conflict_labels:
        non_feature_conflicts = [c for c in conflicts if c.decoded_path[1] != "feature"]
        if non_feature_conflicts:
            desc = "\n".join(_summary_of_conflict_labels(non_feature_conflicts))
            raise InvalidOperation(
                f"The --renumber option only works for feature conflicts - it cannot resolve the following conflicts:\n{desc}"
            )
    else:
        # If the user runs this with no filters, we help them match the renumberable conflicts.
        conflicts = [c for c in conflicts if c.decoded_path[1] == "feature"]

    # Check for unresolved meta-conflicts and non-renumberable primary keys:
    merged_datasets = merge_context.repo.datasets("MERGED_TREE")
    affected_ds_paths = set(c.any_true_version.dataset_path for c in conflicts)
    for ds_path in affected_ds_paths:
        if find_multiple_conflicts_to_resolve(
            merged_index, merge_context, [f"{ds_path}:meta"]
        ):
            raise InvalidOperation(
                f"There are still unresolved meta-item conflicts for dataset {ds_path}. These need to be resolved first."
            )

        dataset = merged_datasets[ds_path]
        if [c.data_type for c in dataset.schema.pk_columns] != ["integer"]:
            if conflict_labels:
                raise InvalidOperation(
                    f"Dataset {ds_path} does not have an integer primary key, and so conflicts cannot be automatically renumbered."
                )
            else:
                # If the user runs this with no filters, we help them match the renumberable conflicts.
                conflicts = [c for c in conflicts if c.decoded_path[0] != ds_path]

    # We don't have a way to specify only insert/insert conflicts using filters, but --renumber only works for insert/insert conflicts.
    # So, we just implicitly filter out the other types of feature conflict.
    conflicts = [
        c
        for c in conflicts
        if c.versions.ancestor is None
        and c.versions.ours is not None
        and c.versions.theirs is not None
    ]

    if not conflicts:
        raise NotFound(
            "The --renumber option only works for unresolved insert/insert conficts with integer primary-keys. "
            "There are no matching conflicts that can be renumbered.",
            exit_code=NO_CONFLICT,
        )

    return conflicts


def _summary_of_conflict_labels(conflicts):
    if len(conflicts) > 15:
        return [c.label for c in conflicts[:10]] + ["..."]
    else:
        return [c.label for c in conflicts]


def _fix_conflict_label(conflict_label):
    """
    Due to the way conflict labels are often displayed with ":ancestor" etc on the end,
    a user could easily have an extra ":" on the end by accident.
    """
    rstripped = conflict_label.rstrip(":")
    if rstripped and len(rstripped) == len(conflict_label) - 1:
        return rstripped
    return conflict_label


@click.command(cls=KartCommand)
@click.pass_context
@click.option(
    "--with",
    "with_version",
    type=click.Choice(["ancestor", "ours", "theirs", "delete", "workingcopy"]),
    help=(
        "Resolve the conflict with any of the following - \n"
        ' - "ancestor", "ours", or "theirs" - the versions which already exist in these commits'
        ' - "workingcopy" - the version currently found inside the working copy'
        ' - "delete" - the conflict is resolved by simply removing it'
    ),
    cls=MutexOption,
    exclusive_with=["file_path", "renumber"],
)
@click.option(
    "--with-file",
    "file_path",
    type=click.Path(exists=True, dir_okay=False),
    help="Resolve the conflict by accepting the version(s) supplied in the given file.",
    cls=MutexOption,
    exclusive_with=["with_version", "renumber"],
)
@click.option(
    "--renumber",
    type=click.Choice(["ours", "theirs"]),
    help='Resolve one or more insert/insert conflicts by keeping both versions ("ours" and "theirs")'
    "but assigning one of the two versions a new primary key value so it doesn't conflict with the other",
    cls=MutexOption,
    exclusive_with=["with_version", "file_path"],
)
@click.argument(
    "conflict_labels",
    nargs=-1,
    metavar="[CONFLICT_LABELS]",
    shell_complete=conflict_completer,
)
def resolve(ctx, with_version, file_path, renumber, conflict_labels):
    """Resolve a merge conflict, using one of the conflicting versions, or with a user-supplied resolution."""

    repo = ctx.obj.get_repo(allowed_states=KartRepoState.MERGING)

    if not (with_version or file_path or renumber):
        raise click.UsageError(
            "Choose a resolution using --with or --with-file or --renumber"
        )

    conflict_labels = [_fix_conflict_label(c) for c in conflict_labels]

    if renumber:
        resolve_conflicts_with_renumber(repo, renumber, conflict_labels)
        return

    merged_index = MergedIndex.read_from_repo(repo)
    merge_context = MergeContext.read_from_repo(repo)

    rich_conflict = find_single_conflict_to_resolve(
        merged_index, merge_context, conflict_labels
    )

    if file_path:
        res = load_file_resolve(rich_conflict, Path(file_path))
    elif with_version == "workingcopy":
        res = load_workingcopy_resolve(rich_conflict)
    elif with_version == "delete":
        res = []
    else:
        assert with_version in ("ancestor", "ours", "theirs")
        version = getattr(rich_conflict.versions, with_version)
        if version is None:
            click.echo(
                f'Version "{with_version}" does not exist - resolving conflict by deleting.'
            )
            res = []
        else:
            res = [version.entry]

    merged_index.add_resolve(rich_conflict.key, res)
    merged_index.write_to_repo(repo)
    update_workingcopy_with_resolve(
        repo, merged_index, merge_context, rich_conflict, res
    )

    unresolved_conflicts = len(merged_index.unresolved_conflicts)
    click.echo(f"Resolved 1 conflict. {_pc(unresolved_conflicts)} to go.")
    if unresolved_conflicts == 0:
        click.echo("Use `kart merge --continue` to complete the merge")


def _pc(count):
    """Simple pluraliser for conflict/conflicts"""
    if count == 1:
        return "1 conflict"
    else:
        return f"{count} conflicts"
