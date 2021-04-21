import copy
import json
from pathlib import Path

import click
import pygit2

from .cli_util import MutexOption
from .exceptions import InvalidOperation, NotFound, NotYetImplemented, NO_CONFLICT
from .geometry import geojson_to_gpkg_geom
from .merge_util import MergeIndex, MergeContext, RichConflict
from .repo import SnoRepoState


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
    features = json.load(Path(file_path).open())["features"]
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


def load_geojson_resolve(file_path, dataset, repo):
    """
    Given a file that contains 0 or more geojson features, and a dataset,
    returns pygit2.IndexEntrys containing those features when added to that dataset.
    """
    return [
        write_feature_to_dataset_entry(f, dataset, repo)
        for f in ungeojson_file(file_path, dataset)
    ]


def ensure_geojson_resolve_supported(rich_conflict):
    """
    Ensures that the given conflict can be resolved by a resolution provided in a geojson file.
    This is true so long as the conflict only involves a single table
    - otherwise we won't know which table should contain the resolution -
    and the conflict only involves features, since we can't load metadata from geojson.
    """
    single_table = "," not in rich_conflict.decoded_path[0]
    only_features = rich_conflict.decoded_path[1] == "feature"
    if not single_table or not only_features:
        raise NotYetImplemented(
            "Sorry, only feature conflicts can currently be resolved using --with-file"
        )


@click.command()
@click.pass_context
@click.option(
    "--with",
    "with_version",
    type=click.Choice(["ancestor", "ours", "theirs", "delete"]),
    help=(
        "Resolve the conflict with any of the existing versions - "
        '"ancestor", "ours", or "theirs" - or with "delete" which resolves the conflict by deleting it.'
    ),
    cls=MutexOption,
    exclusive_with=["file_path"],
)
@click.option(
    "--with-file",
    "file_path",
    type=click.Path(exists=True, dir_okay=False),
    help="Resolve the conflict by accepting the version(s) supplied in the given file.",
    cls=MutexOption,
    exclusive_with=["with_version"],
)
@click.argument("conflict_label", default=None, required=True)
def resolve(ctx, with_version, file_path, conflict_label):
    """Resolve a merge conflict. So far only supports resolving to any of the three existing versions."""

    repo = ctx.obj.get_repo(allowed_states=SnoRepoState.MERGING)
    if not (with_version or file_path):
        raise click.UsageError("Choose a resolution using --with or --with-file")

    merge_index = MergeIndex.read_from_repo(repo)
    merge_context = MergeContext.read_from_repo(repo)

    if conflict_label.endswith(":"):
        # Due to the way conflict labels are often displayed with ":ancestor" etc on the end,
        # a user could easily have an extra ":" on the end by accident.
        conflict_label = conflict_label[:-1]

    for key, conflict3 in merge_index.conflicts.items():
        rich_conflict = RichConflict(conflict3, merge_context)
        if rich_conflict.label == conflict_label:
            if key in merge_index.resolves:
                raise InvalidOperation(
                    f"Conflict at {conflict_label} is already resolved"
                )

            if file_path:
                ensure_geojson_resolve_supported(rich_conflict)
                # Use any version of the dataset to serialise the feature.
                # TODO: This will need more work when schema changes are supported.
                dataset = rich_conflict.any_true_version.dataset
                res = load_geojson_resolve(file_path, dataset, repo)

            elif with_version == "delete":
                res = []
            else:
                assert with_version in ("ancestor", "ours", "theirs")
                res = [getattr(conflict3, with_version)]
                if res == [None]:
                    click.echo(
                        f'Version "{with_version}" does not exist - resolving conflict by deleting.'
                    )
                    res = []

            merge_index.add_resolve(key, res)
            merge_index.write_to_repo(repo)
            click.echo(
                f"Resolved 1 conflict. {len(merge_index.unresolved_conflicts)} conflicts to go."
            )
            ctx.exit(0)

    raise NotFound(f"No conflict found at {conflict_label}", NO_CONFLICT)
