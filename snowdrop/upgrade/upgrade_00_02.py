#!/usr/bin/env python3

import json
import re
import subprocess
from datetime import datetime
from pathlib import Path

import click
import pygit2

from snowdrop.core import walk_tree
from snowdrop.structure import Dataset02


@click.command()
@click.argument('source', type=click.Path(exists=True, file_okay=False), required=True)
@click.argument('dest', type=click.Path(exists=False, writable=True), required=True)
@click.argument('layer', required=True)
def upgrade(source, dest, layer):
    """
    Upgrade a v0.0/v0.1 Snowdrop repository to Sno v0.2
    """
    source = Path(source)
    dest = Path(dest)

    if dest.exists():
        raise click.BadParameter(f"'{dest}': already exists", param_hint="DEST")

    source_repo = pygit2.Repository(str(source))
    if not source_repo or not source_repo.is_bare:
        raise click.BadParameter(f"'{source}': not an existing repository", param_hint="SOURCE")

    try:
        source_tree = (source_repo.head.peel(pygit2.Tree) / layer).obj
    except KeyError:
        raise click.BadParameter(f"'{layer}' not found in source repository", param_hint="SOURCE")

    try:
        version_data = json.loads((source_tree / 'meta' / 'version').obj.data)
        version = tuple([int(v) for v in version_data['version'].split('.')])
    except Exception:
        raise click.BadParameter("Error getting source repository version", param_hint="SOURCE")

    if version >= (0, 2):
        raise click.BadParameter(f"Expecting version <0.2, got {version_data['version']}", param_hint="SOURCE")

    # action!
    click.secho(f"Initialising {dest} ...", bold=True)
    dest.mkdir()
    dest_repo = pygit2.init_repository(str(dest), bare=True)

    # walk _all_ references
    source_walker = source_repo.walk(
        source_repo.head.target,
        pygit2.GIT_SORT_TOPOLOGICAL | pygit2.GIT_SORT_REVERSE
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
                raise ValueError(f"Commit {i} ({source_commit.id}): Haven't seen parent ({parent_id})")

        source_tree = (source_commit.peel(pygit2.Tree) / layer).obj

        sqlite_table_info = json.loads((source_tree / 'meta' / 'sqlite_table_info').obj.data.decode('utf8'))
        field_cid_map = {r['name']: r['cid'] for r in sqlite_table_info}

        gpkg_geometry_columns = json.loads((source_tree / 'meta' / 'gpkg_geometry_columns').obj.data.decode('utf8'))
        geom_field = gpkg_geometry_columns['column_name'] if gpkg_geometry_columns else None

        pk_field = None
        for field in sqlite_table_info:
            if field["pk"]:
                pk_field = field["name"]
                break
        else:
            if sqlite_table_info[0]["type"] == "INTEGER":
                pk_field = sqlite_table_info[0]['name']
            else:
                raise ValueError("No primary key field found")

        if i == 0:
            click.echo(f"  {layer}: Geometry={geom_field} PrimaryKey={pk_field}")

        dataset = Dataset02(None, layer)
        version = json.dumps({"version": dataset.VERSION_IMPORT}).encode('utf8')

        feature_count = 0

        index = pygit2.Index()
        for top_tree, top_path, subtree_names, blob_names in walk_tree(source_tree):
            if top_path == 'meta':
                # copy meta across as-is
                for blob_name in blob_names:
                    if blob_name == 'version':
                        # except version which we update
                        dest_blob = dest_repo.create_blob(version)

                    else:
                        source_blob = (top_tree / blob_name).obj
                        dest_blob = dest_repo.create_blob(source_blob.data)

                    index.add(pygit2.IndexEntry(
                        f'{layer}/.sno-table/{top_path}/{blob_name}',
                        dest_blob,
                        pygit2.GIT_FILEMODE_BLOB
                    ))

            elif re.match(r'^features/[a-f0-9]{4}/([a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12})$', top_path):
                # feature path
                source_feature_dict = {}
                for attr in blob_names:
                    source_blob = (top_tree / attr).obj
                    if attr == geom_field:
                        source_feature_dict[attr] = source_blob.data
                    else:
                        source_feature_dict[attr] = json.loads(source_blob.data.decode('utf8'))

                dataset.write_feature(
                    source_feature_dict,
                    dest_repo,
                    index,
                    field_cid_map=field_cid_map,
                    geom_cols=[geom_field],
                    primary_key=pk_field,
                )
                feature_count += 1

            elif top_path == '' or re.match(r'^features(/[a-f0-9]{4})?$', top_path):
                pass
            else:
                raise ValueError(f"Unexpected path: '{top_path}'")

        dest_tree = index.write_tree(dest_repo)
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
        click.echo(f"  {i}: {source_commit.hex[:8]} → {dest_commit.hex[:8]} ({commit_time}; {source_commit.committer.name}; {feature_count} rows)")

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

    click.secho("\nUpgrade complete", fg='green', bold=True)
