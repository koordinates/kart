import json
import os
import subprocess
import time
import uuid

import click
import pygit2

from . import gpkg


@click.command("import-gpkg")
@click.pass_context
@click.argument("geopackage", type=click.Path(exists=True, dir_okay=False))
@click.argument("table", required=False)
@click.option("--list-tables", is_flag=True)
def import_gpkg(ctx, geopackage, table, list_tables):
    """ Import a GeoPackage to a new repository """
    db = gpkg.db(geopackage)
    dbcur = db.cursor()

    sql = "SELECT 1 FROM sqlite_master WHERE type='table' AND name='gpkg_contents';"
    if not dbcur.execute(sql).fetchone():
        raise click.BadParameter(f"'{geopackage}' doesn't appear to be a valid GeoPackage", param_hint="geopackage")

    if list_tables:
        # print a list of the GeoPackage tables
        click.secho(f"GeoPackage tables in '{geopackage}':", bold=True)
        # support GDAL aspatial extension pre-GeoPackage 1.2 before GPKG supported attributes
        sql = """
            SELECT table_name, data_type, identifier
            FROM gpkg_contents
            WHERE data_type IN ('features', 'attributes', 'aspatial')
            ORDER BY table_name;
        """
        for table_name, data_type, identifier in dbcur.execute(sql):
            click.echo(f"{table_name}  -  {identifier}")
        return

    if not table:
        raise click.BadParameter('Missing argument', param_hint='table')

    sql = """
        SELECT 1
        FROM gpkg_contents
        WHERE
            table_name=?
            AND data_type IN ('features', 'attributes');"""
    if not dbcur.execute(sql, (table,)).fetchone():
        raise click.BadParameter(f"Feature/Attributes table '{table}' not found in gpkg_contents", param_hint="table")

    click.echo(f"Importing {geopackage} ...")

    repo_dir = ctx.obj["repo_dir"]
    if os.path.exists(repo_dir):
        repo = pygit2.Repository(repo_dir)
        assert repo.is_bare, "Not a valid repository"

        if not repo.is_empty:
            raise click.ClickException(
                "Looks like you already have commits in this repository"
            )
    else:
        if not repo_dir.endswith(".snow"):
            raise click.BadParameter(
                "Path should end in .snow", param_hint="--repo"
            )
        repo = pygit2.init_repository(repo_dir, bare=True)

    with db:

        index = pygit2.Index()
        print("Writing meta bits...")
        for name, value in gpkg.get_meta_info(db, layer=table):
            blob_id = repo.create_blob(value.encode("utf8"))
            entry = pygit2.IndexEntry(
                f"{table}/meta/{name}", blob_id, pygit2.GIT_FILEMODE_BLOB
            )
            index.add(entry)

        dbcur.execute(f"SELECT COUNT(*) FROM {gpkg.ident(table)};")
        row_count = dbcur.fetchone()[0]
        print(f"Found {row_count} features in {table}")

        # iterate features
        t0 = time.time()
        dbcur.execute(f"SELECT * FROM {gpkg.ident(table)};")
        t1 = time.time()

        # Repo Structure
        # layer-name/
        #   meta/
        #     version
        #     schema
        #     geometry
        #   features/
        #     {uuid[:4]}/
        #       {uuid}/
        #         {field} => value
        #         ...
        #       ...
        #     ...

        print(f"Query ran in {t1-t0:.1f}s")
        for i, row in enumerate(dbcur):
            feature_id = str(uuid.uuid4())

            for field in row.keys():
                object_path = f"{table}/features/{feature_id[:4]}/{feature_id}/{field}"

                value = row[field]
                if not isinstance(value, bytes):  # blob
                    value = json.dumps(value).encode("utf8")

                blob_id = repo.create_blob(value)
                entry = pygit2.IndexEntry(
                    object_path, blob_id, pygit2.GIT_FILEMODE_BLOB
                )
                index.add(entry)
            # print(feature_id, object_path, field, value, entry)

            if i and i % 500 == 0:
                print(f"  {i+1} features... @{time.time()-t1:.1f}s")

        t2 = time.time()

        print(f"Added {i+1} Features to index in {t2-t1:.1f}s")
        print(f"Overall rate: {((i+1)/(t2-t0)):.0f} features/s)")

        print("Writing tree...")
        tree_id = index.write_tree(repo)
        del index
        t3 = time.time()
        print(f"Tree sha: {tree_id} (in {(t3-t2):.0f}s)")

        print("Committing...")
        user = repo.default_signature
        commit = repo.create_commit(
            "refs/heads/master",
            user,
            user,
            f"Import from {os.path.split(geopackage)[1]}",
            tree_id,
            [],
        )
        t4 = time.time()
        print(f"Commit: {commit} (in {(t4-t3):.0f}s)")

        print(f"Garbage-collecting...")
        subprocess.check_call(["git", "-C", repo_dir, "gc"])
        t5 = time.time()
        print(f"GC completed in {(t5-t4):.1f}s")
