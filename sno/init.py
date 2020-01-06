import functools
import hashlib
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

import click
import pygit2

from . import gpkg, checkout, structure
from .core import check_git_user


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

    check_git_user(repo=None)

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

        # resolve GPKG:~/foo.gpkg and GPKG:~me/foo.gpkg
        # usually this is handled by the shell, but the GPKG: prefix prevents that
        value = os.path.expanduser(value)

        # pass to Click's normal path resolving & validation
        path = super().convert(value, param, ctx)
        return (prefix, path, suffix)


class ImportGPKG:
    """ GeoPackage Import Source """
    def __init__(self, source, table=None):
        self.source = source
        self.table = table
        self.db = gpkg.db(self.source)

    def __str__(self):
        s = f"GeoPackage: {self.source}"
        if self.table:
            s += f":{self.table}"
        return s

    def check(self):
        dbcur = self.db.cursor()

        sql = "SELECT 1 FROM sqlite_master WHERE type='table' AND name='gpkg_contents';"
        try:
            if not dbcur.execute(sql).fetchone():
                raise ValueError("gpkg_contents table doesn't exist")
        except (ValueError, sqlite3.DatabaseError) as e:
            raise ValueError(f"'{self.source}' doesn't appear to be a valid GeoPackage") from e

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
        db = gpkg.db(self.source)
        dbcur = db.cursor()

        # support GDAL aspatial extension pre-GeoPackage 1.2 before GPKG supported attributes
        sql = """
            SELECT table_name, identifier
            FROM gpkg_contents
            WHERE data_type IN ('features', 'attributes', 'aspatial')
            ORDER BY table_name;
        """
        tables = {}
        for table_name, identifier in dbcur.execute(sql):
            tables[table_name] = f"{table_name}  -  {identifier}"
        return tables

    def __enter__(self):
        if not self.table:
            raise ValueError("No table specified")

        self.db.execute("BEGIN")
        self.dbcur = self.db.cursor()
        return self

    def __exit__(self, *exc):
        del self.dbcur
        self.db.execute("ROLLBACK")

    @property
    @functools.lru_cache(maxsize=1)
    def row_count(self):
        self.dbcur.execute(f"SELECT COUNT(*) FROM {gpkg.ident(self.table)};")
        return self.dbcur.fetchone()[0]

    @property
    @functools.lru_cache(maxsize=1)
    def primary_key(self):
        return gpkg.pk(self.db, self.table)

    @property
    @functools.lru_cache(maxsize=1)
    def geom_cols(self):
        return gpkg.geom_cols(self.db, self.table)

    @property
    @functools.lru_cache(maxsize=1)
    def field_cid_map(self):
        q = self.db.execute(f"PRAGMA table_info({gpkg.ident(self.table)});")
        return {r['name']: r['cid'] for r in q}

    def iter_features_sorted(self, pk_callback, limit=None):
        tbl_hash = hashlib.md5(self.table.encode('utf8')).hexdigest()
        tbl_name = f"_sno_{tbl_hash}"
        func_name = f"_sno_sk_{tbl_hash}"

        self.db.create_function(func_name, 1, pk_callback)

        t0 = time.monotonic()
        self.dbcur.execute(f"""
            CREATE TEMPORARY TABLE {tbl_name} (
                sort TEXT PRIMARY KEY,
                link INTEGER
            ) WITHOUT ROWID
        """)

        sql = f"""
            INSERT INTO {tbl_name} (sort, link)
            SELECT
                {func_name}({gpkg.ident(self.primary_key)}),
                {gpkg.ident(self.primary_key)}
            FROM {gpkg.ident(self.table)}
        """
        if limit is not None:
            sql += f" LIMIT {limit:d}"
        self.dbcur.execute(sql)
        t1 = time.monotonic()
        click.echo(f"Build link/sort mapping table in {t1-t0:0.1f}s")
        self.dbcur.execute(f"""
            CREATE INDEX temp.{tbl_hash}_idxm ON {tbl_name}(sort,link);
        """)
        t2 = time.monotonic()
        click.echo(f"Build pk/sort mapping index in {t2-t1:0.1f}s")

        # Print the Query Plan
        # self.dbcur.execute(f"""
        #     EXPLAIN QUERY PLAN
        #     SELECT {gpkg.ident(self.table)}.*
        #     FROM {tbl_name}
        #         INNER JOIN {gpkg.ident(self.table)} ON ({tbl_name}.link={gpkg.ident(self.table)}.{gpkg.ident(self.primary_key)})
        #     ORDER BY {tbl_name}.sort;
        # """)
        # print("\n".join("\t".join(str(f) for f in r) for r in self.dbcur.fetchall()))

        self.dbcur.execute(f"""
            SELECT {gpkg.ident(self.table)}.*
            FROM {tbl_name}
                INNER JOIN {gpkg.ident(self.table)} ON ({tbl_name}.link={gpkg.ident(self.table)}.{gpkg.ident(self.primary_key)})
            ORDER BY {tbl_name}.sort;
        """)
        click.echo(f"Ran SELECT query in {time.monotonic()-t2:0.1f}s")
        return self.dbcur

    def iter_features(self):
        self.dbcur.execute(f"SELECT * FROM {gpkg.ident(self.table)};")
        return self.dbcur

    def build_meta_info(self, repo_version):
        return gpkg.get_meta_info(self.db, layer=self.table, repo_version=repo_version)


@click.command('import')
@click.pass_context
@click.argument("source", type=ImportPath())
@click.argument("directory", type=click.Path(file_okay=False, exists=False, resolve_path=True), required=False)
@click.option("--list", "do_list", is_flag=True, help='List all tables present in the source path')
@click.option(
    "--version",
    type=click.Choice(structure.DatasetStructure.version_numbers()),
    default=structure.DatasetStructure.version_numbers()[0],
    hidden=True
)
@click.option("--method", hidden=True)
def import_table(ctx, source, directory, do_list, version, method):
    """
    Import data into a repository.

    SOURCE: Import from dataset: "FORMAT:PATH[:TABLE]" eg. "GPKG:my.gpkg:my_table"
    DIRECTORY: where to import the table to

    $ sno import GPKG:my.gpkg:my_table layers/the_table

    To show available tables in the import data, use
    $ sno import --list GPKG:my.gpkg
    """
    repo_dir = ctx.obj["repo_dir"] or "."
    repo = pygit2.Repository(repo_dir)
    if not repo or not repo.is_bare:
        raise click.BadParameter("Not an existing repository", param_hint="--repo")

    check_git_user(repo)

    source_prefix, source_path, source_table = source
    source_klass = globals()[f"Import{source_prefix}"]
    source_loader = source_klass(
        source=source_path,
        table=source_table,
    )

    try:
        source_loader.check()
    except ValueError as e:
        raise click.BadParameter(str(e), param_hint="source") from e

    if do_list or not source_table:
        available_tables = source_loader.list_tables()

        # print a list of the GeoPackage tables
        click.secho(f"GeoPackage tables in '{Path(source_path).name}':", bold=True)
        for t_label in available_tables.values():
            click.echo(t_label)

        if do_list:
            return

        if sys.stdout.isatty():
            t_choices = click.Choice(choices=available_tables.keys())
            t_default = next(iter(available_tables)) if len(available_tables) == 1 else None
            source_table = click.prompt('\nSelect a table to import', type=t_choices, show_choices=False, default=t_default)

            # re-init & re-check
            source_loader = source_klass(
                source=source_path,
                table=source_table,
            )
            source_loader.check()

    if not source_table:
        click.secho(f'\nSpecify a table to import from via "{source_prefix}:{source_path}:MYTABLE"', fg='yellow')
        ctx.exit(1)

    if directory:
        directory = os.path.relpath(directory, os.path.abspath(repo_dir))
        if not directory:
            raise click.BadParameter("Invalid import directory", param_hint="directory")
    else:
        directory = source_table

    importer = structure.DatasetStructure.importer(directory, version=version)
    params = json.loads(os.environ.get("SNO_IMPORT_OPTIONS", None) or "{}")
    if params:
        click.echo(f"Import parameters: {params}")
    if method == "slow":
        importer.import_table(repo, source_loader, **params)
    else:
        importer.fast_import_table(repo, source_loader, **params)


@click.command()
@click.pass_context
@click.option("--import", "import_from", type=ImportPath(), help='Import from data: "FORMAT:PATH:TABLE" eg. "GPKG:my.gpkg:my_table"')
@click.option("--checkout/--no-checkout", "do_checkout", is_flag=True, default=True, help="Whether to checkout a working copy in the repository")
@click.argument("directory", type=click.Path(writable=True, file_okay=False), required=False)
def init(ctx, import_from, do_checkout, directory):
    """
    Initialise a new repository and optionally import data

    DIRECTORY must be empty, and should be named with a '.sno' suffix. Defaults to the current directory.

    To show available tables in the import data, use
    $ sno init --import=GPKG:my.gpkg
    """

    if import_from:
        check_git_user(repo=None)

        import_prefix, import_path, import_table = import_from
        source_klass = globals()[f"Import{import_prefix}"]
        source_loader = source_klass(
            source=import_path,
            table=import_table
        )

        try:
            source_loader.check()
        except ValueError as e:
            raise click.BadParameter(str(e), param_hint="import_from") from e

        if not import_table:
            available_tables = source_loader.list_tables()

            # print a list of the GeoPackage tables
            click.secho(f"GeoPackage tables in '{Path(import_path).name}':", bold=True)
            for t_label in available_tables.values():
                click.echo(t_label)

            if sys.stdout.isatty():
                t_choices = click.Choice(choices=available_tables.keys())
                import_table = click.prompt('Please select a table to import', type=t_choices, show_choices=False)

                # re-init & re-check
                source_loader = source_klass(
                    source=import_path,
                    table=import_table,
                )
                source_loader.check()
            else:
                click.secho(f'\nSpecify a table to import from via "{import_prefix}:{import_path}:MYTABLE"', fg='yellow')
                ctx.exit(1)

    if directory is None:
        directory = os.curdir
    elif not Path(directory).exists():
        Path(directory).mkdir(parents=True)

    repo_dir = Path(directory).resolve()
    if any(repo_dir.iterdir()):
        raise click.BadParameter(
            f'"{repo_dir}" isn\'t empty', param_hint="directory"
        )

    # Create the repository
    repo = pygit2.init_repository(str(repo_dir), bare=True)

    if import_from:
        importer = structure.DatasetStructure.importer(import_table)
        importer.import_table(repo, source_loader)

        if do_checkout:
            # Checkout a working copy
            wc_path = repo_dir / f"{repo_dir.stem}.gpkg"

            click.echo(f'Checkout {import_table} to {wc_path} as GPKG ...')

            checkout.checkout_new(
                repo_structure=structure.RepositoryStructure(repo),
                path=wc_path.name,
                commit=repo.head.peel(pygit2.Commit),
            )
    else:
        click.echo(f"Created an empty repository at {repo_dir} — import some data with `sno import`")
