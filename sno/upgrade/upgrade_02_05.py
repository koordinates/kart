import functools
import subprocess
from datetime import datetime
from pathlib import Path

import click
import pygit2

from sno.core import walk_tree
from sno.dataset1 import Dataset1
from sno.dataset2_gpkg import gpkg_to_v2_schema
from sno.fast_import import fast_import_tables


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


def _raw_commit_time(commit):
    offset = commit.commit_time_offset
    hours, minutes = divmod(abs(offset), 60)
    sign = "+" if offset >= 0 else "-"
    return f"{commit.commit_time} {sign}{hours:02}{minutes:02}"


def _upgrade_commit(i, source_commit, dest_parents, dest_repo, commit_map):
    source_tree = source_commit.peel(pygit2.Tree)
    sources = {d.path: ImportV1Dataset(d) for d in _find_datasets(source_tree)}
    dataset_count = len(sources)
    feature_count = sum(s.row_count for s in sources.values())

    s = source_commit
    commit_time = _raw_commit_time(s)
    header = (
        "commit refs/heads/master\n"
        f"author {s.author.name} <{s.author.email}> {commit_time}\n"
        f"committer {s.committer.name} <{s.committer.email}> {commit_time}\n"
        f"data {len(s.message.encode('utf8'))}\n{s.message}\n"
    )
    header += "".join(f"merge {p}\n" for p in dest_parents)

    fast_import_tables(
        dest_repo, sources, incremental=False, quiet=True, header=header, version="2.0",
    )

    dest_commit = dest_repo.head.peel(pygit2.Commit)
    commit_map[source_commit.hex] = dest_commit.hex

    commit_time = datetime.fromtimestamp(source_commit.commit_time)
    click.echo(
        f"  {i}: {source_commit.hex[:8]} → {dest_commit.hex[:8]} ({commit_time}; {source_commit.committer.name}; {dataset_count} datasets; {feature_count} rows)"
    )


def _find_datasets(source_tree):
    for top_tree, top_path, subtree_names, blob_names in walk_tree(
        source_tree, topdown=True
    ):
        if subtree_names == [".sno-table"]:
            yield Dataset1(top_tree, top_path)
            # No need to walk into the subtree of this dataset.
            subtree_names.clear()


class ImportV1Dataset:
    def __init__(self, dataset):
        assert dataset.version == "1.0"
        self.dataset = dataset
        self.path = self.dataset.path
        self.table = self.path
        self.source = "v1-sno-repo"

    @functools.lru_cache(maxsize=1)
    def get_v2_schema(self):
        sqlite_table_info = self.dataset.get_meta_item("sqlite_table_info")
        gpkg_geometry_columns = self.dataset.get_meta_item("gpkg_geometry_columns")
        return gpkg_to_v2_schema(
            sqlite_table_info, gpkg_geometry_columns, id_salt=self.path
        )

    def get_meta_item(self, key):
        if key == "title":
            return self.dataset.get_meta_item("gpkg_contents")["identifier"]
        elif key == "description":
            return self.dataset.get_meta_item("gpkg_contents")["description"]
        else:
            return self.dataset.get_meta_item(key)

    def iter_features(self):
        for _, feature in self.dataset.features():
            yield feature

    @property
    def row_count(self):
        return self.dataset.feature_count()

    def __str__(self):
        return self.path

    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass
