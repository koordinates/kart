import subprocess
from datetime import datetime
from pathlib import Path

import click
import pygit2

from sno.core import walk_tree
from sno.dataset1 import Dataset1
from sno.dataset2 import Dataset2
from sno.dataset2_gpkg import gpkg_to_v2_schema


@click.command()
@click.argument("source", type=click.Path(exists=True, file_okay=False), required=True)
@click.argument("dest", type=click.Path(exists=False, writable=True), required=True)
def upgrade(source, dest):
    """
    Upgrade a v0.2/v0.3/v0.4 Sno repository to Sno v0.5
    """
    source = Path(source)
    dest = Path(dest)

    if dest.exists():
        raise click.BadParameter(f"'{dest}': already exists", param_hint="DEST")

    source_repo = pygit2.Repository(str(source))
    if not source_repo or not source_repo.is_bare:
        raise click.BadParameter(
            f"'{source}': not an existing repository", param_hint="SOURCE"
        )

    # action!
    click.secho(f"Initialising {dest} ...", bold=True)
    dest.mkdir()
    dest_repo = pygit2.init_repository(str(dest), bare=True)

    # walk _all_ references
    source_walker = source_repo.walk(
        source_repo.head.target, pygit2.GIT_SORT_TOPOLOGICAL | pygit2.GIT_SORT_REVERSE
    )
    for ref in source_repo.listall_reference_objects():
        source_walker.push(ref.resolve().target)

    commit_map = {}

    click.secho("\nWriting new commits ...", bold=True)
    for i, source_commit in enumerate(source_walker):
        dest_parents = []
        for parent_id in source_commit.parent_ids:
            try:
                dest_parents.append(commit_map[parent_id.hex])
            except KeyError:
                raise ValueError(
                    f"Commit {i} ({source_commit.id}): Haven't seen parent ({parent_id})"
                )

        _upgrade_commit(i, source_commit, dest_parents, dest_repo, commit_map)

    click.echo(f"{i+1} commits processed.")

    click.secho("\nUpdating references ...", bold=True)
    for ref in source_repo.listall_reference_objects():
        if ref.type == pygit2.GIT_REF_OID:
            # real references
            target = commit_map[ref.target.hex]
            dest_repo.references.create(ref.name, target, True)  # overwrite
            click.echo(f"  {ref.name} ({ref.target.hex[:8]} → {target[:8]})")

    for ref in source_repo.listall_reference_objects():
        if ref.type == pygit2.GIT_REF_SYMBOLIC:
            dest_repo.references.create(ref.name, ref.target)
            click.echo(f"  {ref.name} → {ref.target}")

    click.secho("\nCompacting repository ...", bold=True)
    subprocess.check_call(["git", "-C", str(dest), "gc"])

    click.secho("\nUpgrade complete", fg="green", bold=True)


def _upgrade_commit(i, source_commit, dest_parents, dest_repo, commit_map):
    source_tree = source_commit.peel(pygit2.Tree)
    dest_tree, dataset_count, feature_count = _upgrade_tree(source_tree, dest_repo)
    dest_commit = dest_repo.create_commit(
        "HEAD",
        source_commit.author,
        source_commit.committer,
        source_commit.message,
        dest_tree,
        dest_parents,
        # source_commit.message_encoding,
    )
    commit_map[source_commit.hex] = dest_commit.hex

    commit_time = datetime.fromtimestamp(source_commit.commit_time)
    click.echo(
        f"  {i}: {source_commit.hex[:8]} → {dest_commit.hex[:8]} ({commit_time}; {source_commit.committer.name}; {dataset_count} datasets; {feature_count} rows)"
    )


def _upgrade_tree(source_tree, dest_repo):
    dest_index = pygit2.Index()
    dataset_count = 0
    feature_count = 0
    for top_tree, top_path, subtree_names, blob_names in walk_tree(
        source_tree, topdown=True
    ):
        if subtree_names == [".sno-table"]:
            dataset_count += 1
            source_dataset = Dataset1(top_tree, top_path)
            feature_count += _update_dataset(source_dataset, dest_index, dest_repo)
            # No need to walk into subtrees.
            subtree_names.clear()

    dest_tree = dest_index.write_tree(dest_repo)
    return dest_tree, dataset_count, feature_count


def _update_dataset(source_dataset, dest_index, dest_repo):
    assert source_dataset.version == "1.0", source_dataset.version
    path = source_dataset.path

    sqlite_table_info = source_dataset.get_meta_item("sqlite_table_info")
    gpkg_geometry_columns = source_dataset.get_meta_item("gpkg_geometry_columns")
    schema = gpkg_to_v2_schema(sqlite_table_info, gpkg_geometry_columns, id_salt=path)
    _write_to_index(
        dest_index, dest_repo, path, Dataset2.VERSION_PATH, Dataset2.VERSION_IMPORT,
    )
    _write_to_index(dest_index, dest_repo, path, *Dataset2.encode_schema(schema))
    _write_to_index(dest_index, dest_repo, path, *Dataset2.encode_legend(schema.legend))

    feature_count = 0
    for _, feature in source_dataset.features():
        feature_count += 1
        _write_to_index(
            dest_index, dest_repo, path, *Dataset2.encode_feature(feature, schema)
        )
    return feature_count


def _write_to_index(dest_index, dest_repo, dataset_path, path, data):
    path = f"{dataset_path}/{path}"
    blob_id = dest_repo.create_blob(data)
    dest_index.add(pygit2.IndexEntry(path, blob_id, pygit2.GIT_FILEMODE_BLOB))
