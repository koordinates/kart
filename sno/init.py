import functools
import os
import re
import sys
from pathlib import Path
from urllib.parse import parse_qsl, unquote, urlsplit

import click
import pygit2
from osgeo import gdal, ogr

from sno import is_windows
from . import gpkg, checkout, structure
from .core import check_git_user
from .cli_util import call_and_exit_flag, MutexOption
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
    # https://github.com/koordinates/sno/issues/86
    # 'TAB': 'MapInfo File',
    'PG': 'PostgreSQL',
}
# The set of format prefixes where a local path is expected
# (as opposed to a URL / something else)
LOCAL_PATH_FORMATS = set(FORMAT_TO_OGR_MAP.keys()) - {'PG'}


class OgrImporter:
    """
    Imports from an OGR source, currently from a whitelist of formats.
    """

    OGR_TYPE_TO_SQLITE_TYPE = {
        # NOTE: we don't handle OGR's *List (array) fields at all.
        # If you write them to GPKG using OGR, you end up with TEXT.
        # We also don't handle  ogr's "Time" fields, because they end up as TEXT in GPKG,
        # which we can't roundtrip. Tackle when we get someone actually using those types...
        'Integer': 'MEDIUMINT',
        'Integer64': 'INTEGER',
        'Real': 'FLOAT',
        'String': 'TEXT',
        'Binary': 'BLOB',
        'Date': 'DATE',
        'DateTime': 'DATETIME',
    }
    OGR_SUBTYPE_TO_SQLITE_TYPE = {ogr.OFSTBoolean: 'BOOLEAN', ogr.OFSTInt16: 'SMALLINT'}

    OGR_TYPE_TO_V2_SCHEMA_TYPE = {
        'Integer': ('integer', {"size": 32}),
        'Integer64': ('integer', {"size": 64}),
        'Real': ('float', {}),
        'String': ('text', {}),
        'Binary': ('blob', {}),
        'Date': ('date', {}),
        'DateTime': ('datetime', {}),
    }
    OGR_SUBTYPE_TO_V2_SCHEMA_TYPE = {
        ogr.OFSTBoolean: ('boolean', {}),
        ogr.OFSTInt16: ('integer', {"size": 16}),
    }

    @classmethod
    def _all_subclasses(cls):
        for sub in cls.__subclasses__():
            yield sub
            yield from sub._all_subclasses()

    @classmethod
    def adapt_source_for_ogr(cls, source):
        # Accept Path objects
        ogr_source = str(source)
        # Optionally, accept driver-prefixed paths like 'GPKG:'
        allowed_formats = sorted(FORMAT_TO_OGR_MAP.keys())
        m = re.match(
            rf'^(OGR|{"|".join(FORMAT_TO_OGR_MAP.keys())}):(.+)$', ogr_source, re.I
        )
        prefix = None
        if m:
            prefix, ogr_source = m.groups()
            prefix = prefix.upper()
            if prefix == 'OGR':
                # Don't specify a driver; let OGR just do whatever it can do.
                # We don't 'support' this, but it will probably work fine for some datasources.
                allowed_formats = None
            else:
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
        else:
            # see if any subclasses have a handler for this.
            for subclass in cls._all_subclasses():
                if 'handle_source_string' in subclass.__dict__:
                    retval = subclass.handle_source_string(ogr_source)
                    if retval is not None:
                        ogr_source, allowed_formats = retval
                        break

        return ogr_source, allowed_formats

    @classmethod
    def _ogr_open(cls, ogr_source, **open_kwargs):
        return gdal.OpenEx(
            ogr_source,
            gdal.OF_VECTOR | gdal.OF_VERBOSE_ERROR | gdal.OF_READONLY,
            **open_kwargs,
        )

    @classmethod
    def open(cls, source, table=None, primary_key=None):
        ogr_source, allowed_formats = cls.adapt_source_for_ogr(source)
        if allowed_formats is None:
            # let OGR use any driver it's been compiled with.
            open_kwargs = {}
        else:
            open_kwargs = {
                'allowed_drivers': [FORMAT_TO_OGR_MAP[x] for x in allowed_formats]
            }
        try:
            ds = cls._ogr_open(ogr_source, **open_kwargs)
        except RuntimeError as e:
            raise NotFound(
                f"{ogr_source!r} doesn't appear to be valid "
                f"(tried formats: {','.join(allowed_formats) if allowed_formats else '(all)'})",
                exit_code=NO_IMPORT_SOURCE,
            ) from e

        try:
            klass = globals()[f'Import{ds.GetDriver().ShortName}']
        except KeyError:
            klass = cls
        else:
            # Reopen ds to give subclasses a chance to specify open options.
            ds = klass._ogr_open(ogr_source, **open_kwargs)

        return klass(
            ds, table, source=source, ogr_source=ogr_source, primary_key=primary_key
        )

    @classmethod
    def quote_ident_part(cls, part):
        """
        SQL92 conformant identifier quoting, for use with OGR-dialect SQL
        (and most other dialects)
        """
        part = part.replace('"', '""')
        return f'"{part}"'

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

    def __init__(self, ogr_ds, table=None, *, source, ogr_source, primary_key=None):
        self.ds = ogr_ds
        self.driver = self.ds.GetDriver()
        self.table = table
        self.source = source
        self.ogr_source = ogr_source
        self._primary_key = self._check_primary_key_option(primary_key)

    def clone_for_table(self, table, primary_key=None):
        return self.__class__(
            self.ds,
            table=table,
            source=self.source,
            ogr_source=self.ogr_source,
            primary_key=primary_key or self._primary_key,
        )

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
        table_list = list(self.get_tables().keys())

        if len(table_list) == 1:
            return table_list[0]
        else:
            self.print_table_list()
            if get_input_mode() == InputMode.NO_INPUT:
                raise NotFound("No table specified", exit_code=NO_TABLE)
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

    def check_table(self, table_name):
        if table_name not in self.get_tables():
            raise NotFound(
                f"Table '{table_name}' not found", exit_code=NO_TABLE,
            )

    def __enter__(self):
        self.check_table(self.table)

        if self.ds.TestCapability(ogr.ODsCTransactions):
            self.ds.StartTransaction()
        return self

    def __exit__(self, *exc):
        if self.ds.TestCapability(ogr.ODsCTransactions):
            self.ds.RollbackTransaction()

    @property
    @functools.lru_cache(maxsize=1)
    def row_count(self):
        return self.ogrlayer.GetFeatureCount(force=False)

    @property
    @functools.lru_cache(maxsize=1)
    def primary_key(self):
        # NOTE: for many OGR drivers, FID column is always 'FID'.
        # For some drivers (databases), OGR will instead use the primary key
        # of the given table, BUT only if it is an integer.
        # For tables with non-integer PKS, ogrlayer.GetFIDColumn() returns ''.
        # In that case, we would have no choice but to get the PK name outside of OGR.
        # For that reason we don't use ogrlayer.GetFIDColumn() here,
        # and instead we have to implement custom PK behaviour in driver-specific subclasses.
        return self._primary_key or 'FID'

    @property
    @functools.lru_cache(maxsize=1)
    def geom_cols(self):
        ld = self.ogrlayer.GetLayerDefn()
        cols = []
        num_fields = ld.GetGeomFieldCount()
        if num_fields == 0:
            # aspatial dataset
            return []
        elif num_fields == 1:
            # Some OGR drivers don't support named geometry fields;
            # the dataset either has a geometry or doesn't.
            # In situations where there _is_ a field, it doesn't necessarily have a name.
            # So here we pick 'geom' as the default name.
            return [ld.GetGeomFieldDefn(0).GetName() or 'geom']
        for i in range(num_fields):
            # Where there are multiple geom fields, they have names
            cols.append(ld.GetGeomFieldDefn(i).GetName())
        return cols

    @property
    def is_spatial(self):
        return bool(self.geom_cols)

    def _check_primary_key_option(self, primary_key_name):
        if primary_key_name is None:
            return None
        ld = self.ogrlayer.GetLayerDefn()

        for i in range(ld.GetFieldCount()):
            field = ld.GetFieldDefn(i)
            if primary_key_name == field.GetName():
                return primary_key_name
        else:
            raise InvalidOperation(
                f"'{primary_key_name}' was not found in the dataset",
                param_hint='--primary-key',
            )

    @ungenerator(dict)
    def _field_cid_map(self):
        ld = self.ogrlayer.GetLayerDefn()

        yield self.primary_key, 0
        start = 1

        # OGR
        if self.geom_cols:
            gc = self.ogrlayer.GetGeometryColumn() or self.geom_cols[0]
            yield gc, 1
            start += 1

        # The FID field may or may not be in this list, depending on the OGR driver.
        # either way, the @ungenerator(dict) removes dupes...
        for i in range(ld.GetFieldCount()):
            field = ld.GetFieldDefn(i)
            name = field.GetName()
            yield name, i + start

    @property
    @functools.lru_cache(maxsize=1)
    def field_cid_map(self):
        return self._field_cid_map()

    @property
    @functools.lru_cache(maxsize=1)
    @ungenerator(dict)
    def field_adapter_map(self):
        ld = self.ogrlayer.GetLayerDefn()

        yield self.primary_key, adapt_value_noop

        gc = self.ogrlayer.GetGeometryColumn()
        if self.geom_cols and not gc:
            gc = self.geom_cols[0]
        if gc:
            yield gc, adapt_value_noop

        for i in range(ld.GetFieldCount()):
            field = ld.GetFieldDefn(i)
            name = field.GetName()
            yield name, get_type_value_adapter(field.GetType())

    def _get_primary_key_value(self, ogr_feature, name):
        return ogr_feature.GetFID()

    @ungenerator(dict)
    def _ogr_feature_to_dict(self, ogr_feature):
        for name, adapter in self.field_adapter_map.items():
            if name in self.geom_cols:
                yield (
                    name,
                    gpkg.ogr_to_gpkg_geom(ogr_feature.GetGeometryRef()),
                )
            elif name == self.primary_key:
                yield name, self._get_primary_key_value(ogr_feature, name)
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

    def _get_meta_srid(self):
        srs = self.ogrlayer.GetSpatialRef()
        if srs is None:
            return 0
        srs.AutoIdentifyEPSG()
        if srs.IsProjected():
            return int(srs.GetAuthorityCode("PROJCS"))
        elif srs.IsGeographic():
            return int(srs.GetAuthorityCode("GEOGCS"))
        else:
            # TODO: another type of SRS? Need examples.
            raise ValueError(
                "Unknown SRS type; please create an issue with details "
                "( https://github.com/koordinates/sno/issues/new )"
            )

    def get_meta_contents(self):
        ogr_metadata = self.ogrlayer.GetMetadata()
        return {
            'table_name': self.table,
            'data_type': 'features' if self.is_spatial else 'attributes',
            'identifier': ogr_metadata.get('IDENTIFIER') or '',
            'description': ogr_metadata.get('DESCRIPTION') or '',
            'srs_id': self._get_meta_srid(),
        }

    def _get_meta_geometry_type(self):
        # remove Z/M components
        ogr_geom_type = ogr.GT_Flatten(self.ogrlayer.GetGeomType())
        if ogr_geom_type == ogr.wkbUnknown:
            return 'GEOMETRY'
        return (
            # normalise the geometry type names the way the GPKG spec likes it:
            # http://www.geopackage.org/spec/#geometry_types
            ogr.GeometryTypeToName(ogr_geom_type)
            # 'Line String' --> 'LineString'
            .replace(' ', '')
            # --> 'LINESTRING'
            .upper()
        )

    def get_meta_geometry_columns(self):
        if not self.is_spatial:
            return None

        ogr_geom_type = self.ogrlayer.GetGeomType()

        return {
            "table_name": self.table,
            "column_name": self.ogrlayer.GetGeometryColumn() or self.geom_cols[0],
            "geometry_type_name": self._get_meta_geometry_type(),
            "srs_id": self._get_meta_srid(),
            "z": int(ogr.GT_HasZ(ogr_geom_type)),
            "m": int(ogr.GT_HasM(ogr_geom_type)),
        }

    def get_geometry_v2_column_schema(self):
        from .dataset2 import ColumnSchema

        if not self.is_spatial:
            return None

        name = self.ogrlayer.GetGeometryColumn() or self.geom_cols[0]
        geometry_type = self._get_meta_geometry_type()
        ogr_geom_type = self.ogrlayer.GetGeomType()
        z = "Z" if ogr.GT_HasZ(ogr_geom_type) else ""
        m = "M" if ogr.GT_HasM(ogr_geom_type) else ""
        extra_type_info = {
            "geometryType": f"{geometry_type} {z}{m}".strip(),
            "geometrySRS": f"EPSG:{self._get_meta_srid()}",
        }

        return ColumnSchema(
            ColumnSchema.new_id(), name, "geometry", None, **extra_type_info
        )

    def _ogr_type_to_sqlite_type(self, fd):
        subtype = fd.GetSubType()
        if subtype == ogr.OFSTNone:
            type_name = self.OGR_TYPE_TO_SQLITE_TYPE[fd.GetTypeName()]
        else:
            type_name = self.OGR_SUBTYPE_TO_SQLITE_TYPE[subtype]

        if type_name in ('TEXT', 'BLOB'):
            width = fd.GetWidth()
            if width:
                type_name += f'({width})'
        return type_name

    def _field_to_v2_column_schema(self, fd):
        from .dataset2 import ColumnSchema

        ogr_type = fd.GetTypeName()
        ogr_subtype = fd.GetSubType()
        if ogr_subtype == ogr.OFSTNone:
            data_type, extra_type_info = self.OGR_TYPE_TO_V2_SCHEMA_TYPE[ogr_type]
        else:
            data_type, extra_type_info = self.OGR_SUBTYPE_TO_V2_SCHEMA_TYPE[ogr_subtype]

        extra_type_info = extra_type_info.copy()

        if data_type in ('TEXT', 'BLOB'):
            width = fd.GetWidth()
            if width:
                extra_type_info["length"] = width

        return ColumnSchema(
            ColumnSchema.new_id(), fd.GetName(), data_type, None, **extra_type_info
        )

    @functools.lru_cache(maxsize=1)
    def get_v2_schema(self):
        from .dataset2 import Schema, ColumnSchema

        ld = self.ogrlayer.GetLayerDefn()
        pk_column = ColumnSchema(ColumnSchema.new_id(), self.primary_key, "integer", 0)
        geometry_column = self.get_geometry_v2_column_schema()
        special_columns = (
            [pk_column, geometry_column] if geometry_column else [pk_column]
        )

        other_columns = [
            self._field_to_v2_column_schema(ld.GetFieldDefn(i))
            for i in range(ld.GetFieldCount())
        ]

        return Schema(special_columns + other_columns)

    @ungenerator(list)
    def get_meta_table_info(self):
        ld = self.ogrlayer.GetLayerDefn()
        for name, cid in self.field_cid_map.items():
            default = None
            field_index = ld.GetFieldIndex(name)
            if field_index < 0:
                # some datasources don't have FID and geometry fields in the fields list
                if name == self.primary_key:
                    nullable = False
                    type_name = 'INTEGER'
                else:
                    nullable = True
                    type_name = self._get_meta_geometry_type()
            else:
                fd = ld.GetFieldDefn(field_index)
                type_name = self._ogr_type_to_sqlite_type(fd)
                nullable = fd.IsNullable()
                default = fd.GetDefault()

            yield {
                "cid": cid,
                "name": name,
                "type": type_name,
                "notnull": int(not nullable),
                "dflt_value": default,
                "pk": int(name == self.primary_key),
            }

    @ungenerator(list)
    def get_meta_spatial_ref_sys(self):
        srs = self.ogrlayer.GetSpatialRef()
        srid = self._get_meta_srid()
        yield {
            'srs_name': srs.GetName() if srs else 'Unknown CRS',
            'srs_id': srid,
            'organization': 'EPSG',
            'organization_coordsys_id': srid,
            'definition': srs.ExportToWkt() if srs else '',
            'description': None,
        }

    def build_meta_info(self):
        """
        Imitates the ImportGPKG implementation, and we just use the gpkg field/table names
        for compatibility, because there's no particular need to change it...
        Keep both implementations in sync!
        """
        yield "gpkg_contents", self.get_meta_contents()
        yield "gpkg_geometry_columns", self.get_meta_geometry_columns()
        yield "sqlite_table_info", self.get_meta_table_info()
        yield "gpkg_spatial_ref_sys", self.get_meta_spatial_ref_sys()
        # TODO: The GPKG impl of this method reads internal XML metadata
        # (gpkg_metadata_reference, gpkg_metadata)
        # The OGR impl should probably read and store XML metadata from nearby files.


class ImportGPKG(OgrImporter):
    @classmethod
    def quote_ident_part(cls, part):
        """
        SQLite-conformant identifier quoting
        """
        return gpkg.ident(part)

    @property
    @functools.lru_cache(maxsize=1)
    def primary_key(self):
        if self._primary_key:
            return self._primary_key

        db = gpkg.db(self.ogr_source)
        return gpkg.pk(db, self.table)

    def iter_features(self):
        """
        Overrides the super implementation for performance reasons
        (it turns out that OGR feature iterators for GPKG are quite slow!)
        """
        db = gpkg.db(self.ogr_source)
        dbcur = db.cursor()
        dbcur.execute(f"SELECT * FROM {self.quote_ident(self.table)};")
        yield from dbcur

    def build_meta_info(self):
        """
        Returns metadata from the gpkg_* tables about this GPKG.
        Keep this in sync with the OgrImporter implementation
        """
        db = gpkg.db(self.ogr_source)
        yield from gpkg.get_meta_info(db, layer=self.table)


class ImportPostgreSQL(OgrImporter):
    @classmethod
    def postgres_url_to_ogr_conn_str(cls, url):
        """
        Takes a URL ('postgresql://..')
        and turns it into a key/value connection string, prefixed by 'PG:' for OGR.

        libpq actually handles URIs fine, but OGR doesn't :(
        So to import via OGR we have to convert them.

        https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING

        ^ These docs say these URLs can contain multiple hostnames or ports,
        but we don't handle that.
        """

        url = urlsplit(url)
        scheme = url.scheme.lower()
        if scheme not in ('postgres', 'postgresql'):
            raise ValueError("Bad scheme")

        # Start with everything from the querystring.
        params = dict(parse_qsl(url.query))

        # Each of these fields can come from the main part of the URL,
        # OR can come from the querystring.
        # If both are specified, the querystring has precedence.
        # So in 'postgresql://host1/?host=host2', the resultant host is 'host2'
        if url.username:
            params.setdefault('user', url.username)
        if url.password:
            params.setdefault('password', url.password)
        if url.hostname:
            params.setdefault('host', unquote(url.hostname))
        if url.port:
            params.setdefault('port', url.port)
        dbname = (url.path or '/')[1:]
        if dbname:
            params.setdefault('dbname', dbname)

        conn_str = ' '.join(sorted(f'{k}={v}' for (k, v) in params.items()))
        return f'PG:{conn_str}'

    @classmethod
    def handle_source_string(cls, source):
        if '://' not in source:
            return None
        try:
            return cls.postgres_url_to_ogr_conn_str(source), ['PG']
        except ValueError:
            return None

    @classmethod
    def _ogr_open(cls, ogr_source, **open_kwargs):
        open_options = open_kwargs.setdefault('open_options', [])
        # don't only list tables listed in geometry_columns
        open_options.append('LIST_ALL_TABLES=YES')
        return super()._ogr_open(ogr_source, **open_kwargs)

    def psycopg2_conn(self):
        import psycopg2

        conn_str = self.source
        if conn_str.startswith('OGR:'):
            conn_str = conn_str[4:]
        if conn_str.startswith('PG:'):
            conn_str = conn_str[3:]
        # this will either be a URL or a key=value conn str
        return psycopg2.connect(conn_str)

    def _get_primary_key_value(self, ogr_feature, name):
        try:
            return ogr_feature.GetField(name)
        except KeyError:
            # OGR uses integer PKs as the 'FID', but then *doesn't*
            # expose them as fields.
            # In that case we have to call GetFID()
            return ogr_feature.GetFID()

    @property
    @functools.lru_cache(maxsize=1)
    def primary_key(self):
        if self._primary_key:
            return self._primary_key
        conn = self.psycopg2_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.attname
                FROM   pg_index i
                JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                     AND a.attnum = ANY(i.indkey)
                WHERE  i.indrelid = %s::regclass
                AND    i.indisprimary;
                """,
                [self.table],
            )
            rows = cur.fetchall()
            # TODO: handle multi-column PKs. Ignoring for now.
            assert len(rows) == 1
            return rows[0][0]


def list_import_formats(ctx, param, value):
    """
    List the supported import formats
    """
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


@click.command("import")
@click.pass_context
@click.argument("source")
@click.argument(
    "tables", nargs=-1,
)
@click.option(
    "--all-tables",
    "-a",
    help="Import all tables from the source.",
    is_flag=True,
    cls=MutexOption,
    exclusive_with=["do_list", "tables"],
)
@click.option(
    "--message", "-m", help="Commit message. By default this is auto-generated.",
)
@click.option(
    "--list",
    "do_list",
    is_flag=True,
    help="List all tables present in the source path",
    cls=MutexOption,
    exclusive_with=["all_tables", "tables"],
)
@click.option(
    "--version",
    type=click.Choice(structure.DatasetStructure.version_numbers()),
    default=structure.DatasetStructure.version_numbers()[0],
    hidden=True,
)
@call_and_exit_flag(
    "--list-formats",
    callback=list_import_formats,
    help="List available import formats, and then exit",
)
@click.option(
    "--output-format", "-o", type=click.Choice(["text", "json"]), default="text",
)
@click.option(
    "--primary-key",
    help="Which field to use as the primary key. Must be unique. Auto-detected when possible.",
)
def import_table(
    ctx,
    all_tables,
    message,
    do_list,
    output_format,
    version,
    primary_key,
    source,
    tables,
):
    """
    Import data into a repository.

    $ sno import SOURCE [TABLE_SPEC] [TABLE_SPEC]

    SOURCE: Import from dataset: "FORMAT:PATH" eg. "GPKG:my.gpkg"

    TABLE_SPEC: Import a particular table, optionally with a new name: "TABLE[:AS_NAME]"
    eg. "2019_08_06_median_waterlevel:waterlevel"

    $ sno import GPKG:my.gpkg [table1[:new_name1] [table2[:new_name2]]

    To show available tables in the import data, use

    $ sno import --list GPKG:my.gpkg
    """

    if output_format == 'json' and not do_list:
        raise click.UsageError(
            "Illegal usage: '--output-format=json' only supports --list"
        )

    use_repo_ctx = not do_list
    if use_repo_ctx:
        repo = ctx.obj.repo
        check_git_user(repo)

    source_loader = OgrImporter.open(source, None)
    if do_list:
        source_loader.print_table_list(do_json=output_format == 'json')
        return
    elif all_tables:
        tables = source_loader.get_tables().keys()
    else:
        if not tables:
            tables = [source_loader.prompt_for_table("Select a table to import")]

    loaders = {}
    for table in tables:
        (src_table, *rest) = table.split(':', 1)
        dst_table = rest[0] if rest else src_table
        if not dst_table:
            raise click.BadParameter("Invalid table name", param_hint="tables")
        if is_windows:
            dst_table = dst_table.replace("\\", "/")  # git paths use / as a delimiter

        if dst_table in loaders:
            raise click.UsageError(
                f'table "{dst_table}" was specified more than once', param_hint="tables"
            )
        loaders[dst_table] = source_loader.clone_for_table(
            src_table, primary_key=primary_key
        )

    structure.fast_import_tables(repo, loaders, message=message, version=version)
    rs = structure.RepositoryStructure(repo)
    if rs.working_copy:
        # Update working copy with new datasets
        for dst_table in loaders:
            dataset = rs[dst_table]
            rs.working_copy.write_full(rs.head_commit, dataset)


@click.command()
@click.pass_context
@click.argument(
    "directory", type=click.Path(writable=True, file_okay=False), required=False
)
@click.option(
    "--import",
    "import_from",
    help='Import a database (all tables): "FORMAT:PATH" eg. "GPKG:my.gpkg"',
)
@click.option(
    "--checkout/--no-checkout",
    "do_checkout",
    is_flag=True,
    default=True,
    help="Whether to checkout a working copy in the repository",
)
@click.option(
    "--message",
    "-m",
    help="Commit message (when used with --import). By default this is auto-generated.",
)
@click.option(
    "--version",
    type=click.Choice(structure.DatasetStructure.version_numbers()),
    default=structure.DatasetStructure.version_numbers()[0],
    hidden=True,
)
def init(ctx, do_checkout, message, directory, version, import_from):
    """
    Initialise a new repository and optionally import data.
    DIRECTORY must be empty. Defaults to the current directory.
    """

    if directory is None:
        directory = os.curdir
    elif not Path(directory).exists():
        Path(directory).mkdir(parents=True)

    repo_path = Path(directory).resolve()
    if any(repo_path.iterdir()):
        raise InvalidOperation(f'"{repo_path}" isn\'t empty', param_hint="directory")

    if import_from:
        check_git_user(repo=None)
        source_loader = OgrImporter.open(import_from, None)

        # Import all tables.
        # If you need finer grained control than this,
        # use `sno init` and *then* `sno import` as a separate command.
        tables = source_loader.get_tables().keys()
        loaders = {t: source_loader.clone_for_table(t) for t in tables}

    # Create the repository
    repo = pygit2.init_repository(str(repo_path), bare=True)

    if import_from:
        structure.fast_import_tables(repo, loaders, message=message, version=version)

        if do_checkout:
            # Checkout a working copy
            wc_path = repo_path / f"{repo_path.stem}.gpkg"

            click.echo(f"Checkout to {wc_path} as GPKG ...")

            checkout.checkout_new(
                repo_structure=structure.RepositoryStructure(repo),
                path=wc_path.name,
                commit=repo.head.peel(pygit2.Commit),
            )
    else:
        click.echo(
            f"Created an empty repository at {repo_path} — import some data with `sno import`"
        )
