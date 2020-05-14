import functools
import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path

import click
import pygit2
from osgeo import gdal, ogr

from sno import is_windows
from . import gpkg, checkout, structure
from .core import check_git_user
from .cli_util import do_json_option
from .exceptions import (
    InvalidOperation,
    NotFound,
    NO_IMPORT_SOURCE,
    NO_TABLE,
)
from .ogr_util import adapt_value_noop, get_type_value_adapter
from .output_util import dump_json_output, get_input_mode, InputMode
from .utils import ungenerator


# This defines what formats are allowed, as well as mapping
# sno prefixes onto an OGR format shortname.
FORMAT_TO_OGR_MAP = {
    'GPKG': 'GPKG',
    'SHP': 'ESRI Shapefile',
    'TAB': 'MapInfo File',
}
# The set of format prefixes where a local path is expected
# (as opposed to a URL / something else)
LOCAL_PATH_FORMATS = set(FORMAT_TO_OGR_MAP.keys()) - {'PG'}


class OgrImporter:
    """
    Imports from an OGR source, currently from a whitelist of formats.
    """

    @classmethod
    def adapt_source_for_ogr(cls, source):
        # Accept Path objects
        ogr_source = str(source)
        # Optionally, accept driver-prefixed paths like 'GPKG:'
        allowed_formats = sorted(FORMAT_TO_OGR_MAP.keys())
        m = re.match(
            rf'^({"|".join(FORMAT_TO_OGR_MAP.keys())}):(.+)$', ogr_source, re.I
        )
        prefix = None
        if m:
            prefix, ogr_source = m.groups()
            prefix = prefix.upper()
            allowed_formats = [prefix]

            if prefix in LOCAL_PATH_FORMATS:
                # resolve GPKG:~/foo.gpkg and GPKG:~me/foo.gpkg
                # usually this is handled by the shell, but the GPKG: prefix prevents that
                ogr_source = os.path.expanduser(ogr_source)

            if prefix in ('CSV', 'PG'):
                # OGR actually handles these prefixes itself...
                ogr_source = f'{prefix}:{ogr_source}'

        if prefix in LOCAL_PATH_FORMATS:
            if not os.path.exists(ogr_source):
                raise NotFound(
                    f"Couldn't find {ogr_source!r}", exit_code=NO_IMPORT_SOURCE
                )
        return ogr_source, allowed_formats

    @classmethod
    def open(cls, source, table=None):
        ogr_source, allowed_formats = cls.adapt_source_for_ogr(source)
        allowed_ogr_drivers = [FORMAT_TO_OGR_MAP[x] for x in allowed_formats]
        try:
            ds = gdal.OpenEx(
                ogr_source,
                gdal.OF_VECTOR | gdal.OF_VERBOSE_ERROR | gdal.OF_READONLY,
                allowed_drivers=allowed_ogr_drivers,
            )
        except RuntimeError as e:
            raise NotFound(
                f"{ogr_source!r} doesn't appear to be valid "
                f"(tried formats: {','.join(allowed_formats)})",
                exit_code=NO_IMPORT_SOURCE,
            ) from e

        try:
            klass = globals()[f'Import{ds.GetDriver().ShortName}']
        except KeyError:
            klass = OgrImporter

        return klass(ds, table, source=source, ogr_source=ogr_source,)

    @classmethod
    def quote_ident_part(cls, part):
        """
        SQL92 conformant identifier quoting, for use with OGR-dialect SQL
        (and most other dialects)
        """
        part = part.replace('"', '""')
        return '"%s"' % part

    @classmethod
    def quote_ident(cls, *parts):
        """
        Quotes an identifier with double-quotes for use in SQL queries.

            >>> quote_ident('mytable')
            '"mytable"'
        """
        if not parts:
            raise ValueError("at least one part required")
        return '.'.join([cls.quote_ident_part(p) for p in parts])

    def __init__(self, ogr_ds, table=None, *, source, ogr_source):
        self.ds = ogr_ds
        self.driver = self.ds.GetDriver()
        self.table = table
        self.source = source
        self.ogr_source = ogr_source

    @property
    @functools.lru_cache(maxsize=1)
    def ogrlayer(self):
        return self.ds.GetLayerByName(self.table)

    def get_tables(self):
        """
        Returns a dict of OGRLayer objects keyed by layer name
        """
        layers = {}
        for i in range(self.ds.GetLayerCount()):
            layer = self.ds.GetLayerByIndex(i)
            layers[layer.GetName()] = layer
        return layers

    def print_table_list(self, do_json=False):
        names = {}
        for table_name, ogrlayer in self.get_tables().items():
            try:
                pretty_name = ogrlayer.GetMetadata_Dict()['IDENTIFIER']
            except KeyError:
                pretty_name = table_name
            names[table_name] = pretty_name
        if do_json:
            dump_json_output({"sno.tables/v1": names}, sys.stdout)
        else:
            click.secho(f"Tables found:", bold=True)
            for table_name, pretty_name in names.items():
                click.echo(f"  {table_name} - {pretty_name}")
        return names

    def prompt_for_table(self, prompt):
        table_list = list(self.print_table_list().keys())

        if not sys.stdout.isatty():
            click.secho(
                f'\n{prompt} via `--table MYTABLE`', fg="yellow",
            )
            sys.exit(1)

        t_choices = click.Choice(choices=table_list)
        t_default = table_list[0] if len(table_list) == 1 else None
        return click.prompt(
            f"\n{prompt}", type=t_choices, show_choices=False, default=t_default,
        )

    def __str__(self):
        s = str(self.source)
        if self.table:
            s += f":{self.table}"
        return s

    def check_table(self, prompt=False):
        if not self.table:
            if prompt:
                # this re-inits and re-checks with the new table
                self.table = self.prompt_for_table("Select a table to import")
            else:
                raise NotFound(
                    "No table specified", exit_code=NO_TABLE, param_hint="--table"
                )

        if self.table not in self.get_tables():
            raise NotFound(
                f"Table '{self.table}' not found",
                exit_code=NO_TABLE,
                param_hint="--table",
            )

    def __enter__(self):
        self.check_table()

        if self.ds.TestCapability(ogr.ODsCTransactions):
            self.ds.StartTransaction()
        return self

    def __exit__(self, *exc):
        if self.ds.TestCapability(ogr.ODsCTransactions):
            self.ds.RollbackTransaction()

    @property
    @functools.lru_cache(maxsize=1)
    def row_count(self):
        # note: choose FAST method if possible ( i recall them being different with MITAB especially)
        return self.ogrlayer.GetFeatureCount()

    @property
    @functools.lru_cache(maxsize=1)
    def primary_key(self):
        return self.ogrlayer.GetFIDColumn()

    @property
    @functools.lru_cache(maxsize=1)
    def geom_cols(self):
        ld = self.ogrlayer.GetLayerDefn()
        cols = []
        for i in range(ld.GetGeomFieldCount()):
            cols.append(ld.GetGeomFieldDefn(i).GetName())
        return cols

    @property
    @functools.lru_cache(maxsize=1)
    @ungenerator(dict)
    def field_cid_map(self):
        ld = self.ogrlayer.GetLayerDefn()

        yield self.primary_key, 0
        start = 1

        gc = self.ogrlayer.GetGeometryColumn()
        if gc:
            yield gc, 1
            start += 1

        for i in range(ld.GetFieldCount()):
            field = ld.GetFieldDefn(i)
            name = field.GetName()
            yield name, i + start

    @property
    @ungenerator(dict)
    def field_adapter_map(self):
        ld = self.ogrlayer.GetLayerDefn()

        yield self.primary_key, adapt_value_noop

        gc = self.ogrlayer.GetGeometryColumn() or self.geom_cols[0]
        if gc:
            yield gc, adapt_value_noop

        for i in range(ld.GetFieldCount()):
            field = ld.GetFieldDefn(i)
            name = field.GetName()
            yield name, get_type_value_adapter(field.GetType())

    @ungenerator(dict)
    def _ogr_feature_to_dict(self, ogr_feature):
        yield self.primary_key, ogr_feature.GetFID()
        for name, adapter in self.field_adapter_map.items():
            if name == self.primary_key:
                yield name, ogr_feature.GetFID()
            elif name in self.geom_cols:
                yield (
                    name,
                    gpkg.ogr_to_gpkg_geom(ogr_feature.GetGeometryRef()),
                )
            else:
                value = ogr_feature.GetField(name)
                yield name, adapter(value)

    def _iter_ogr_features(self):
        l = self.ogrlayer
        l.ResetReading()
        while True:
            f = l.GetNextFeature()
            if f is None:
                # end of iter
                l.ResetReading()
                return
            # Turn an OGRFeature into a name:value dict
            yield f

    def iter_features(self):
        for ogr_feature in self._iter_ogr_features():
            yield self._ogr_feature_to_dict(ogr_feature)

    def build_meta_info(self, repo_version):
        # TODO: what _is_ this, do we need it?
        return []


class ImportGPKG(OgrImporter):
    @classmethod
    def quote_ident_part(cls, part):
        """
        SQLite-conformant identifier quoting
        """
        return gpkg.ident(part)

    def iter_features(self):
        """
        Overrides the super implementation for performance reasons
        (it turns out that OGR feature iterators for GPKG are quite slow!)
        """
        db = gpkg.db(self.ogr_source)
        dbcur = db.cursor()
        dbcur.execute(f"SELECT * FROM {self.quote_ident(self.table)};")
        return dbcur

    def build_meta_info(self, repo_version):
        db = gpkg.db(self.ogr_source)
        return gpkg.get_meta_info(db, layer=self.table, repo_version=repo_version)


def list_import_formats(ctx, param, value):
    """
    List the supported import formats
    """
    if not value or ctx.resilient_parsing:
        return
    names = set()
    for prefix, ogr_driver_name in FORMAT_TO_OGR_MAP.items():
        d = gdal.GetDriverByName(ogr_driver_name)
        if d:
            m = d.GetMetadata()
            # only vector formats which can read things.
            if m.get('DCAP_VECTOR') == 'YES' and m.get('DCAP_OPEN') == 'YES':
                names.add(prefix)
    for n in sorted(names):
        click.echo(n)
    ctx.exit()


@click.command("import")
@click.pass_context
@click.argument("source")
@click.argument(
    "directory",
    type=click.Path(file_okay=False, exists=False, resolve_path=True),
    required=False,
)
@click.option(
    "--table",
    "-t",
    help="Which table to import. If not specified, this will be selected interactively",
)
@click.option(
    "--list", "do_list", is_flag=True, help="List all tables present in the source path"
)
@click.option(
    "--list-formats",
    is_flag=True,
    help="List available import formats, and then exit",
    # https://click.palletsprojects.com/en/7.x/options/#callbacks-and-eager-options
    is_eager=True,
    expose_value=False,
    callback=list_import_formats,
)
@click.option(
    "--version",
    type=click.Choice(structure.DatasetStructure.version_numbers()),
    default=structure.DatasetStructure.version_numbers()[0],
    hidden=True,
)
@do_json_option
def import_table(ctx, source, directory, table, do_list, do_json, version):
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

    source_loader = OgrImporter.open(source, table)

    if do_list:
        source_loader.print_table_list(do_json=do_json)
        return

    source_loader.check_table(prompt=True)

    if directory:
        directory = os.path.relpath(directory, os.path.abspath(repo_path))
        if not directory:
            raise click.BadParameter("Invalid import directory", param_hint="directory")
        if is_windows:
            directory = directory.replace("\\", "/")  # git paths use / as a delimiter
    else:
        directory = source_loader.table

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
    help='Import from data: "FORMAT:PATH" eg. "GPKG:my.gpkg"',
)
@click.option(
    "--table",
    "-t",
    help="Which table to import. If not specified, this will be selected interactively",
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
def init(ctx, import_from, table, do_checkout, directory, version):
    """
    Initialise a new repository and optionally import data

    DIRECTORY must be empty. Defaults to the current directory.

    To show available tables in the import data, use
    $ sno init --import=GPKG:my.gpkg
    """
    if import_from:
        check_git_user(repo=None)

        source_loader = OgrImporter.open(import_from, table)
        source_loader.check_table(prompt=True)

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
        importer = structure.DatasetStructure.importer(
            source_loader.table, version=version
        )
        params = json.loads(os.environ.get("SNO_IMPORT_OPTIONS", None) or "{}")
        if params:
            click.echo(f"Import parameters: {params}")
        importer.fast_import_table(repo, source_loader, **params)

        if do_checkout:
            # Checkout a working copy
            wc_path = repo_path / f"{repo_path.stem}.gpkg"

            click.echo(f"Checkout {source_loader.table} to {wc_path} as GPKG ...")

            checkout.checkout_new(
                repo_structure=structure.RepositoryStructure(repo),
                path=wc_path.name,
                commit=repo.head.peel(pygit2.Commit),
            )
    else:
        click.echo(
            f"Created an empty repository at {repo_path} — import some data with `sno import`"
        )
