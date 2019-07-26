import json
import os
import sqlite3
import subprocess
import sys
import time
import uuid
from pathlib import Path

import click
import pygit2

from . import gpkg, checkout


@click.command("import-gpkg", hidden=True)
@click.pass_context
@click.argument("geopackage", type=click.Path(exists=True, dir_okay=False))
@click.argument("table", required=False)
@click.option("--list-tables", is_flag=True)
def import_gpkg(ctx, geopackage, table, list_tables):
    """
    Import a GeoPackage to a new repository (deprecated; use 'init')
    """

    click.secho('"import-gpkg" is deprecated and will be removed in future, use "init" instead', fg='yellow')

    directory = ctx.obj["repo_dir"] or os.curdir

    import_from = ["GPKG", geopackage, None]
    if table and not list_tables:
        import_from[2] = table

    if not list_tables and directory:
        Path(directory).mkdir(exist_ok=True)

    ctx.invoke(init, directory=directory, import_from=tuple(import_from), do_checkout=False)


class ImportPath(click.Path):
    def __init__(self, prefixes=("GPKG",), suffix_required=False, **kwargs):
        params = {
            "exists": True,
            "file_okay": True,
            "dir_okay": False,
            "writable": False,
            "readable": True,
            "resolve_path": False,
            "allow_dash": False,
            "path_type": None,
        }
        params.update(kwargs)

        super().__init__(**params)

        self.prefixes = prefixes
        self.suffix_required = suffix_required

    def convert(self, value, param, ctx):
        if ':' not in value:
            self.fail(f'expecting a prefix (eg. "GPKG:my.gpkg")')
        prefix, value = value.split(':', 1)

        if ':' not in value:
            if self.suffix_required:
                self.fail(f'expecting a suffix (eg. "GPKG:my.gpkg:mytable")')
            else:
                suffix = None
        else:
            value, suffix = value.rsplit(':', 1)

        prefix = prefix.upper()
        if prefix not in self.prefixes:
            self.fail(f'invalid prefix: "{prefix}" (choose from {", ".join(self.prefixes)})')

        path = super().convert(value, param, ctx)
        return (prefix, path, suffix)


class ImportGPKG:
    def __init__(self, path, table=None):
        self.path = path
        self.table = table

    def check(self):
        db = gpkg.db(self.path)
        dbcur = db.cursor()

        sql = "SELECT 1 FROM sqlite_master WHERE type='table' AND name='gpkg_contents';"
        try:
            if not dbcur.execute(sql).fetchone():
                raise ValueError("gpkg_contents table doesn't exist")
        except (ValueError, sqlite3.DatabaseError) as e:
            raise ValueError(f"'{self.path}' doesn't appear to be a valid GeoPackage") from e

        if self.table:
            sql = """
                SELECT 1
                FROM gpkg_contents
                WHERE
                    table_name=?
                    AND data_type IN ('features', 'attributes', 'aspatial');"""
            if not dbcur.execute(sql, (self.table,)).fetchone():
                raise ValueError(f"Feature/Attributes table '{self.table}' not found in gpkg_contents")

    def list_tables(self):
        db = gpkg.db(self.path)
        dbcur = db.cursor()

        # support GDAL aspatial extension pre-GeoPackage 1.2 before GPKG supported attributes
        sql = """
            SELECT table_name, data_type, identifier
            FROM gpkg_contents
            WHERE data_type IN ('features', 'attributes', 'aspatial')
            ORDER BY table_name;
        """
        tables = {}
        for table_name, data_type, identifier in dbcur.execute(sql):
            tables[table_name] = f"{table_name}  -  {identifier}"
        return tables

    def load(self, repo):
        table = self.table
        geopackage = self.path

        click.echo(f"Importing GeoPackage: {geopackage}:{table} ...")

        db = gpkg.db(self.path)
        dbcur = db.cursor()

        with db:
            index = pygit2.Index()
            click.echo("Writing meta bits...")
            for name, value in gpkg.get_meta_info(db, layer=table):
                blob_id = repo.create_blob(value.encode("utf8"))
                entry = pygit2.IndexEntry(
                    f"{table}/meta/{name}", blob_id, pygit2.GIT_FILEMODE_BLOB
                )
                index.add(entry)

            dbcur.execute(f"SELECT COUNT(*) FROM {gpkg.ident(table)};")
            row_count = dbcur.fetchone()[0]
            click.echo(f"Found {row_count} features in {table}")

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

            click.echo(f"Query ran in {t1-t0:.1f}s")
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
                # click.echo(feature_id, object_path, field, value, entry)

                if i and i % 500 == 0:
                    click.echo(f"  {i+1} features... @{time.time()-t1:.1f}s")

            t2 = time.time()

            click.echo(f"Added {i+1} Features to index in {t2-t1:.1f}s")
            click.echo(f"Overall rate: {((i+1)/(t2-t0)):.0f} features/s)")

            click.echo("Writing tree...")
            tree_id = index.write_tree(repo)
            del index
            t3 = time.time()
            click.echo(f"Tree sha: {tree_id} (in {(t3-t2):.0f}s)")

            click.echo("Committing...")
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
            click.echo(f"Commit: {commit} (in {(t4-t3):.0f}s)")

            click.echo(f"Garbage-collecting...")
            subprocess.check_call(["git", "-C", repo.path, "gc"])
            t5 = time.time()
            click.echo(f"GC completed in {(t5-t4):.1f}s")


@click.command()
@click.pass_context
@click.option("--import", "import_from", type=ImportPath(), help='Import from data: "FORMAT:PATH:TABLE" eg. "GPKG:my.gpkg:my_table"')
@click.option("--checkout/--no-checkout", "do_checkout", is_flag=True, default=True, help="Whether to checkout a working copy in the repository")
@click.argument("directory", type=click.Path(exists=True, writable=True, file_okay=False), required=False, default=os.curdir)
def init(ctx, import_from, do_checkout, directory):
    """
    Initialise a new repository and optionally import data

    DIRECTORY must be empty, and be named with a '.snow' suffix. Defaults to the current directory.

    To show available tables in the import data, use
    $ snowdrop init --import=GPKG:my.gpkg
    """

    if import_from:
        import_prefix, import_path, import_table = import_from
        importer_klass = globals()[f"Import{import_prefix}"]
        importer = importer_klass(
            path=import_path,
            table=import_table
        )

        try:
            importer.check()
        except ValueError as e:
            raise click.BadParameter(str(e), param_hint="import_from") from e

        if not import_table:
            available_tables = importer.list_tables()

            # print a list of the GeoPackage tables
            click.secho(f"GeoPackage tables in '{Path(import_path).name}':", bold=True)
            for t_label in available_tables.values():
                click.echo(t_label)

            if sys.stdin.isatty():
                t_choices = click.Choice(choices=available_tables.keys())
                import_table = click.prompt('Please select a table to import', type=t_choices, show_choices=False)
            else:
                click.secho(f'\nSpecify a table to import from via "{import_prefix}:{import_path}:MYTABLE"', fg='yellow')
                ctx.exit(1)
    else:
        # TODO
        raise click.ClickException("Creating an empty repository isn't supported yet. Use 'snowdrop init --import'")

    repo_dir = Path(directory).resolve()

    if repo_dir.suffix != '.snow':
        raise click.BadParameter(
            "name should end in .snow", param_hint="directory"
        )
    if any(repo_dir.iterdir()):
        raise click.BadParameter(
            f'"{repo_dir}" isn\'t empty', param_hint="directory"
        )

    # Create the repository
    repo = pygit2.init_repository(str(repo_dir), bare=True)

    if import_from:
        importer.load(repo)

        if do_checkout:
            # Checkout a working copy
            wc_path = repo_dir / f"{repo_dir.stem}.gpkg"

            click.echo(f'Checkout {import_table} to {wc_path} as GPKG ...')

            checkout.checkout_new(
                repo=repo,
                working_copy=wc_path.name,
                layer=import_table,
                commit=repo.head.peel(pygit2.Commit),
                fmt="GPKG"
            )
