import functools
import hashlib
import json
import os
import sys
import time
from pathlib import Path

import apsw
import click
import pygit2

from sno import is_windows
from . import gpkg, checkout, structure
from .core import check_git_user
from .cli_util import do_json_option
from .exceptions import (
    InvalidOperation,
    NotFound,
    INVALID_ARGUMENT,
    NO_IMPORT_SOURCE,
    NO_TABLE,
)
from .output_util import dump_json_output


@click.command("import-gpkg", hidden=True)
@click.pass_context
@click.argument("geopackage", type=click.Path(exists=True, dir_okay=False))
@click.argument("table", required=False)
@click.option("--list-tables", is_flag=True)
def import_gpkg(ctx, geopackage, table, list_tables):
    """
    Import a GeoPackage to a new repository (deprecated; use 'init')
    """

    click.secho(
        '"import-gpkg" is deprecated and will be removed in future, use "init" instead',
        fg="yellow",
    )

    repo_path = ctx.obj.repo_path

    check_git_user(repo=None)

    import_from = ["GPKG", geopackage, None]
    if table and not list_tables:
        import_from[2] = table

    if not list_tables and repo_path:
        repo_path.mkdir(exist_ok=True)

    ctx.invoke(
        init,
        directory=str(repo_path),
        import_from=tuple(import_from),
        do_checkout=False,
    )


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
        if ":" not in value:
            self.fail(f'expecting a prefix (eg. "GPKG:my.gpkg")')
        prefix, value = value.split(":", 1)

        # need to deal with "GPKG:D:\foo\bar.gpkg:table"
        search_from = 2 if is_windows else 0

        if ":" not in value[search_from:]:
            if self.suffix_required:
                self.fail(f'expecting a suffix (eg. "GPKG:my.gpkg:mytable")')
            else:
                suffix = None
        else:
            value, suffix = value.rsplit(":", 1)

        prefix = prefix.upper()
        if prefix not in self.prefixes:
            self.fail(
                f'invalid prefix: "{prefix}" (choose from {", ".join(self.prefixes)})',
                exit_code=INVALID_ARGUMENT,
            )

        # resolve GPKG:~/foo.gpkg and GPKG:~me/foo.gpkg
        # usually this is handled by the shell, but the GPKG: prefix prevents that
        value = os.path.expanduser(value)

        # pass to Click's normal path resolving & validation
        path = super().convert(value, param, ctx)
        return (prefix, path, suffix)

    def fail(self, message, param=None, ctx=None, exit_code=NO_IMPORT_SOURCE):
        raise NotFound(message, exit_code=exit_code)


class ImportGPKG:
    """ GeoPackage Import Source """

    prefix = "GPKG"

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
        except (ValueError, apsw.SQLError) as e:
            raise NotFound(
                f"'{self.source}' doesn't appear to be a valid GeoPackage",
                exit_code=NO_IMPORT_SOURCE,
            ) from e

        if self.table:
            sql = """
                SELECT 1
                FROM gpkg_contents
                WHERE
                    table_name=?
                    AND data_type IN ('features', 'attributes', 'aspatial');"""
            if not dbcur.execute(sql, (self.table,)).fetchone():
                raise NotFound(
                    f"Feature/Attributes table '{self.table}' not found in gpkg_contents",
                    exit_code=NO_TABLE,
                )

    def get_table_list(self):
        db = gpkg.db(self.source)
        dbcur = db.cursor()

        # support GDAL aspatial extension pre-GeoPackage 1.2 before GPKG supported attributes
        sql = """
            SELECT table_name, identifier
            FROM gpkg_contents
            WHERE data_type IN ('features', 'attributes', 'aspatial')
            ORDER BY table_name;
        """
        table_list = {}
        for table_name, identifier in dbcur.execute(sql):
            table_list[table_name] = identifier
        return table_list

    def print_table_list(self, do_json=False):
        table_list = self.get_table_list()
        if do_json:
            dump_json_output({"sno.tables/v1": table_list}, sys.stdout)
        else:
            click.secho(f"GeoPackage tables in '{Path(self.source).name}':", bold=True)
            for table_name, identifier in table_list.items():
                click.echo(f"{table_name}  -  {identifier}")
        return table_list

    def prompt_for_table(self, prompt):
        table_list = self.print_table_list()

        if not sys.stdout.isatty():
            click.secho(
                f'\n{prompt} via "{self.prefix}:{self.source}:MYTABLE"', fg="yellow",
            )
            sys.exit(1)

        t_choices = click.Choice(choices=table_list.keys())
        t_default = next(iter(table_list)) if len(table_list) == 1 else None
        return click.prompt(
            f"\n{prompt}", type=t_choices, show_choices=False, default=t_default,
        )

    def __enter__(self):
        if not self.table:
            raise ValueError("No table specified")

        dbcur = self.db.cursor()
        dbcur.execute("BEGIN")
        return self

    def __exit__(self, *exc):
        self.db.cursor().execute("ROLLBACK")

    @property
    @functools.lru_cache(maxsize=1)
    def row_count(self):
        dbcur = self.db.cursor()
        dbcur.execute(f"SELECT COUNT(*) FROM {gpkg.ident(self.table)};")
        return dbcur.fetchone()[0]

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
        dbcur = self.db.cursor()
        q = dbcur.execute(f"PRAGMA table_info({gpkg.ident(self.table)});")
        return {r["name"]: r["cid"] for r in q}

    def iter_features_sorted(self, pk_callback, limit=None):
        tbl_hash = hashlib.md5(self.table.encode("utf8")).hexdigest()
        tbl_name = f"_sno_{tbl_hash}"
        func_name = f"_sno_sk_{tbl_hash}"

        self.db.create_function(func_name, 1, pk_callback)
        dbcur = self.db.cursor()

        t0 = time.monotonic()
        dbcur.execute(
            f"""
            CREATE TEMPORARY TABLE {tbl_name} (
                sort TEXT PRIMARY KEY,
                link INTEGER
            ) WITHOUT ROWID
        """
        )

        sql = f"""
            INSERT INTO {tbl_name} (sort, link)
            SELECT
                {func_name}({gpkg.ident(self.primary_key)}),
                {gpkg.ident(self.primary_key)}
            FROM {gpkg.ident(self.table)}
        """
        if limit is not None:
            sql += f" LIMIT {limit:d}"
        dbcur.execute(sql)
        t1 = time.monotonic()
        click.echo(f"Build link/sort mapping table in {t1-t0:0.1f}s")
        dbcur.execute(
            f"""
            CREATE INDEX temp.{tbl_hash}_idxm ON {tbl_name}(sort,link);
        """
        )
        t2 = time.monotonic()
        click.echo(f"Build pk/sort mapping index in {t2-t1:0.1f}s")

        # Print the Query Plan
        # dbcur.execute(f"""
        #     EXPLAIN QUERY PLAN
        #     SELECT {gpkg.ident(self.table)}.*
        #     FROM {tbl_name}
        #         INNER JOIN {gpkg.ident(self.table)} ON ({tbl_name}.link={gpkg.ident(self.table)}.{gpkg.ident(self.primary_key)})
        #     ORDER BY {tbl_name}.sort;
        # """)
        # print("\n".join("\t".join(str(f) for f in r) for r in dbcur.fetchall()))

        dbcur.execute(
            f"""
            SELECT {gpkg.ident(self.table)}.*
            FROM {tbl_name}
                INNER JOIN {gpkg.ident(self.table)} ON ({tbl_name}.link={gpkg.ident(self.table)}.{gpkg.ident(self.primary_key)})
            ORDER BY {tbl_name}.sort;
        """
        )
        click.echo(f"Ran SELECT query in {time.monotonic()-t2:0.1f}s")
        return dbcur

    def iter_features(self):
        dbcur = self.db.cursor()
        dbcur.execute(f"SELECT * FROM {gpkg.ident(self.table)};")
        return dbcur

    def build_meta_info(self, repo_version):
        return gpkg.get_meta_info(self.db, layer=self.table, repo_version=repo_version)


@click.command("import")
@click.pass_context
@click.argument("source", type=ImportPath())
@click.argument(
    "directory",
    type=click.Path(file_okay=False, exists=False, resolve_path=True),
    required=False,
)
@click.option(
    "--list", "do_list", is_flag=True, help="List all tables present in the source path"
)
@click.option(
    "--version",
    type=click.Choice(structure.DatasetStructure.version_numbers()),
    default=structure.DatasetStructure.version_numbers()[0],
    hidden=True,
)
@do_json_option
def import_table(ctx, source, directory, do_list, do_json, version):
    """
    Import data into a repository.

    SOURCE: Import from dataset: "FORMAT:PATH[:TABLE]" eg. "GPKG:my.gpkg:my_table"
    DIRECTORY: where to import the table to

    $ sno import GPKG:my.gpkg:my_table layers/the_table

    To show available tables in the import data, use
    $ sno import --list GPKG:my.gpkg
    """

    if do_json and not do_list:
        raise click.UsageError(
            "Illegal usage: 'sno import --json' only supports --list"
        )

    use_repo_ctx = not do_list
    if use_repo_ctx:
        repo_path = ctx.obj.repo_path
        repo = ctx.obj.repo
        check_git_user(repo)

    source_prefix, source_path, source_table = source
    source_klass = globals()[f"Import{source_prefix}"]
    source_loader = source_klass(source=source_path, table=source_table,)

    try:
        source_loader.check()
    except NotFound as e:
        e.param_hint = "source"
        raise

    if do_list:
        source_loader.print_table_list(do_json=do_json)
        return

    if not source_table:
        source_table = source_loader.prompt_for_table("Select a table to import")
        # re-init & re-check
        source_loader = source_klass(source=source_path, table=source_table,)
        source_loader.check()

    if directory:
        directory = os.path.relpath(directory, os.path.abspath(repo_path))
        if not directory:
            raise click.BadParameter("Invalid import directory", param_hint="directory")
        if is_windows:
            directory = directory.replace("\\", "/")  # git paths use / as a delimiter
    else:
        directory = source_table

    importer = structure.DatasetStructure.importer(directory, version=version)
    params = json.loads(os.environ.get("SNO_IMPORT_OPTIONS", None) or "{}")
    if params:
        click.echo(f"Import parameters: {params}")
    importer.fast_import_table(repo, source_loader, **params)

    rs = structure.RepositoryStructure(repo)
    if rs.working_copy:
        # Update working copy with new dataset
        dataset = rs[directory]
        rs.working_copy.write_full(rs.head_commit, dataset)


@click.command()
@click.pass_context
@click.option(
    "--import",
    "import_from",
    type=ImportPath(),
    help='Import from data: "FORMAT:PATH:TABLE" eg. "GPKG:my.gpkg:my_table"',
)
@click.option(
    "--checkout/--no-checkout",
    "do_checkout",
    is_flag=True,
    default=True,
    help="Whether to checkout a working copy in the repository",
)
@click.argument(
    "directory", type=click.Path(writable=True, file_okay=False), required=False
)
@click.option(
    "--version",
    type=click.Choice(structure.DatasetStructure.version_numbers()),
    default=structure.DatasetStructure.version_numbers()[0],
    hidden=True,
)
def init(ctx, import_from, do_checkout, directory, version):
    """
    Initialise a new repository and optionally import data

    DIRECTORY must be empty. Defaults to the current directory.

    To show available tables in the import data, use
    $ sno init --import=GPKG:my.gpkg
    """

    if import_from:
        check_git_user(repo=None)

        import_prefix, import_path, import_table = import_from
        source_klass = globals()[f"Import{import_prefix}"]
        source_loader = source_klass(source=import_path, table=import_table)

        try:
            source_loader.check()
        except NotFound as e:
            e.param_hint = "import_from"
            raise

        if not import_table:
            import_table = source_loader.prompt_for_table("Select a table to import")
            # re-init & re-check
            source_loader = source_klass(source=import_path, table=import_table,)
            source_loader.check()

    if directory is None:
        directory = os.curdir
    elif not Path(directory).exists():
        Path(directory).mkdir(parents=True)

    repo_path = Path(directory).resolve()
    if any(repo_path.iterdir()):
        raise InvalidOperation(f'"{repo_path}" isn\'t empty', param_hint="directory")

    # Create the repository
    repo = pygit2.init_repository(str(repo_path), bare=True)

    if import_from:
        importer = structure.DatasetStructure.importer(import_table, version=version)
        params = json.loads(os.environ.get("SNO_IMPORT_OPTIONS", None) or "{}")
        if params:
            click.echo(f"Import parameters: {params}")
        importer.fast_import_table(repo, source_loader, **params)

        if do_checkout:
            # Checkout a working copy
            wc_path = repo_path / f"{repo_path.stem}.gpkg"

            click.echo(f"Checkout {import_table} to {wc_path} as GPKG ...")

            checkout.checkout_new(
                repo_structure=structure.RepositoryStructure(repo),
                path=wc_path.name,
                commit=repo.head.peel(pygit2.Commit),
            )
    else:
        click.echo(
            f"Created an empty repository at {repo_path} — import some data with `sno import`"
        )
