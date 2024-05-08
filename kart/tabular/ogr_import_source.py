import functools
import os
import re
import sys
from pathlib import Path
from urllib.parse import parse_qsl, unquote, urlsplit

import click
from osgeo import gdal, ogr

from kart import crs_util, ogr_util
from kart.exceptions import (
    NO_IMPORT_SOURCE,
    NO_TABLE,
    InvalidOperation,
    NotFound,
)
from kart.geometry import ogr_to_gpkg_geom
from kart.output_util import dump_json_output
from kart.schema import ColumnSchema, Schema
from kart.tabular.ogr_adapter import (
    ogr_field_definition_to_kart_type,
    ogr_geometry_type_to_kart_geometry_type,
)
from kart.utils import chunk, ungenerator

from .import_source import TableImportSource

# This defines what formats are allowed, as well as mapping
# Kart prefixes onto an OGR format shortname.
FORMAT_TO_OGR_MAP = {
    "GPKG": "GPKG",
    "SHP": "ESRI Shapefile",
    # https://github.com/koordinates/kart/issues/86
    # 'TAB': 'MapInfo File',
    "PG": "PostgreSQL",
}
# The set of format prefixes where a local path is expected
# (as opposed to a URL / something else)
LOCAL_PATH_FORMATS = set(FORMAT_TO_OGR_MAP.keys()) - {"PG"}


class OgrTableImportSource(TableImportSource):
    """
    Imports from an OGR source, currently from a whitelist of formats.
    """

    DEFAULT_GEOMETRY_COLUMN_NAME = "geom"

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
            if prefix == "OGR":
                # Don't specify a driver; let OGR just do whatever it can do.
                # We don't 'support' this, but it will probably work fine for some datasources.
                allowed_formats = None
            else:
                allowed_formats = [prefix]

                if prefix in LOCAL_PATH_FORMATS:
                    # resolve GPKG:~/foo.gpkg and GPKG:~me/foo.gpkg
                    # usually this is handled by the shell, but the GPKG: prefix prevents that
                    ogr_source = os.path.expanduser(ogr_source)

                if prefix in ("CSV", "PG"):
                    # OGR actually handles these prefixes itself...
                    ogr_source = f"{prefix}:{ogr_source}"
            if prefix in LOCAL_PATH_FORMATS:
                if not os.path.exists(ogr_source):
                    raise NotFound(
                        f"Couldn't find {ogr_source!r}", exit_code=NO_IMPORT_SOURCE
                    )
        else:
            # see if any subclasses have a handler for this.
            for subclass in cls._all_subclasses():
                if "handle_source_string" in subclass.__dict__:
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
                "allowed_drivers": [FORMAT_TO_OGR_MAP[x] for x in allowed_formats]
            }
        try:
            ds = cls._ogr_open(ogr_source, **open_kwargs)
        except RuntimeError as e:
            # Check whether the source is a shapefile, and if so, whether it's missing an .shx file
            if source.lower().endswith(".shp"):
                base_filename = os.path.splitext(os.path.basename(source))[0]
                shx_file = base_filename + ".shx"
                if not os.path.exists(shx_file):
                    raise NotFound(
                        f"Import source was missing some required files: {shx_file}",
                        exit_code=NO_IMPORT_SOURCE,
                    ) from e
            raise NotFound(
                f"{ogr_source!r} doesn't appear to be valid "
                f"(tried formats: {','.join(allowed_formats) if allowed_formats else '(all)'})\n{e}",
                exit_code=NO_IMPORT_SOURCE,
            ) from e

        try:
            klass = globals()[
                f"{ds.GetDriver().ShortName.replace(' ', '')}ImportSource"
            ]
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
        return ".".join([cls.quote_ident_part(p) for p in parts])

    def __init__(
        self,
        ogr_ds,
        table=None,
        *,
        source,
        ogr_source,
        dest_path=None,
        primary_key=None,
        meta_overrides=None,
    ):
        self.ds = ogr_ds
        self.driver = self.ds.GetDriver()
        self.table = table
        self.source = source
        self.ogr_source = ogr_source
        if dest_path:
            self.dest_path = dest_path
        self._primary_key = self._check_primary_key_option(primary_key)
        self.meta_overrides = {
            k: v for k, v in (meta_overrides or {}).items() if v is not None
        }

    def default_dest_path(self):
        return self._normalise_dataset_path(self.table)

    def import_source_desc(self):
        return f"Import from {self.source_name}:{self.table} to {self.dest_path}/"

    def aggregate_import_source_desc(self, import_sources):
        if len(import_sources) == 1:
            return next(iter(import_sources)).import_source_desc()

        desc = f"Import {len(import_sources)} datasets from {self.source_name}:"
        for source in import_sources:
            if source.dest_path == source.table:
                desc += f"\n * {source.table}/"
            else:
                desc += f"\n * {source.dest_path} (from {source.table})"
        return desc

    @property
    def source_name(self):
        return Path(self.source).name

    def clone_for_table(
        self, table, *, dest_path=None, primary_key=None, meta_overrides={}
    ):
        meta_overrides = {**self.meta_overrides, **meta_overrides}
        self.check_table(table)

        return self.__class__(
            self.ds,
            table=table,
            dest_path=dest_path,
            source=self.source,
            ogr_source=self.ogr_source,
            primary_key=primary_key or self._primary_key,
            meta_overrides=meta_overrides,
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
                pretty_name = ogrlayer.GetMetadata_Dict()["IDENTIFIER"]
            except KeyError:
                pretty_name = table_name
            names[table_name] = pretty_name
        if do_json:
            dump_json_output({"kart.tables/v1": names}, sys.stdout)
        else:
            click.secho(f"Tables found:", bold=True)
            for table_name, pretty_name in names.items():
                click.echo(f"  {table_name} - {pretty_name}")
        return names

    def __str__(self):
        s = str(self.source)
        if self.table:
            s += f":{self.table}"
        return s

    def check_table(self, table_name):
        if table_name not in self.get_tables():
            raise NotFound(
                f"Table '{table_name}' not found",
                exit_code=NO_TABLE,
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
    def feature_count(self):
        return self.ogrlayer.GetFeatureCount(force=False)

    @property
    @functools.lru_cache(maxsize=1)
    def primary_key(self):
        # NOTE: We don't call ogrlayer.GetFIDColumn() here, for a few reasons:
        # - Sometimes OGR promotes the record-offset to a primary-key field (ie, the FID of a .SHP is treated as such).
        # We don't want to treat something as a PK unless it genuinely identifies the record across versions, which
        # the record-offset generally would not do.
        # - Sometimes OGR fails to report any primary key column if it has an unexpected type (eg non-integer).
        # So, instead we have to implement custom PK behaviour in driver-specific subclasses.
        return self._primary_key

    @property
    def layer_defn(self):
        return self.ogrlayer.GetLayerDefn()

    @property
    @functools.lru_cache(maxsize=1)
    def has_geometry(self):
        return bool(self.layer_defn.GetGeomFieldCount())

    def _check_primary_key_option(self, primary_key_name):
        self.use_ogc_fid_as_pk = False
        if primary_key_name is None:
            return None
        if primary_key_name:
            ld = self.layer_defn

            for i in range(ld.GetFieldCount()):
                field = ld.GetFieldDefn(i)
                if primary_key_name == field.GetName():
                    return primary_key_name

            if primary_key_name == self.ogrlayer.GetFIDColumn():
                # OGR automatically turns 'ogc_fid' column in postgres into an FID,
                # and removes it from the list of fields below.
                self.use_ogc_fid_as_pk = True
                return primary_key_name

        raise InvalidOperation(
            f"'{primary_key_name}' was not found in the dataset",
            param_hint="--primary-key",
        )

    def _get_primary_key_value(self, ogr_feature, name):
        return ogr_feature.GetFID()

    @property
    @functools.lru_cache(maxsize=1)
    def field_adapter_map(self):
        return {
            col.name: self._get_type_value_adapter(col.name, col.data_type)
            for col in self.schema
        }

    def _get_type_value_adapter(self, name, v2_type):
        return ogr_util.get_type_value_adapter(v2_type)

    @ungenerator(dict)
    def _ogr_feature_to_kart_feature(self, ogr_feature):
        for name, adapter in self.field_adapter_map.items():
            if name == self.primary_key and self.use_ogc_fid_as_pk:
                value = ogr_feature.GetFID()
            elif name in self.geometry_column_names:
                value = ogr_feature.GetGeometryRef()
            else:
                value = ogr_feature.GetField(name)
            yield name, adapter(value)

    def _iter_ogr_features(self, filter_sql=None):
        l = self.ogrlayer
        l.ResetReading()
        if filter_sql is not None:
            l.SetAttributeFilter(filter_sql)
        f = l.GetNextFeature()
        while f is not None:
            yield f
            f = l.GetNextFeature()
        # end of iter
        l.ResetReading()

    def features(self):
        for ogr_feature in self._iter_ogr_features():
            yield self._ogr_feature_to_kart_feature(ogr_feature)

    def _ogr_sql_quote_literal(self, x):
        # OGR follows normal SQL92 string literal quoting rules.
        # There's no params argument to SetAttributeFilter(),
        # so we have to quote things ourselves.
        if isinstance(x, int):
            return str(x)
        else:
            return "'{}'".format(str(x).replace("'", "''"))

    def _first_pk_values(self, row_pks):
        # (123,) --> 123. we only handle one pk field
        for x in row_pks:
            assert len(x) == 1
            yield x[0]

    def get_features(self, row_pks, *, ignore_missing=False):
        pk_field = self.primary_key

        for batch in chunk(self._first_pk_values(row_pks), 1000):
            quoted_pks = ",".join(self._ogr_sql_quote_literal(x) for x in batch)
            filter_sql = f"{self.quote_ident(pk_field)} IN ({quoted_pks})"

            for ogr_feature in self._iter_ogr_features(filter_sql=filter_sql):
                yield self._ogr_feature_to_kart_feature(ogr_feature)

    def sample_geometry(self, geom_col=None):
        for ogr_feature in self._iter_ogr_features():
            geom = ogr_feature.GetGeometryRef()
            if geom:
                return ogr_to_gpkg_geom(geom)
        return None

    def align_schema_to_existing_schema(self, existing_schema):
        aligned_schema = existing_schema.align_to_self(self.schema)
        self.meta_overrides["schema.json"] = aligned_schema
        assert self.schema == aligned_schema

    def meta_items(self):
        return {**self.meta_items_from_db(), **self.meta_overrides}

    @functools.lru_cache()
    @ungenerator(dict)
    def meta_items_from_db(self):
        ogr_metadata = self.ogrlayer.GetMetadata()
        yield "title", ogr_metadata.get("IDENTIFIER")
        yield "description", ogr_metadata.get("DESCRIPTION")
        yield "schema.json", self._schema_from_db()

        for identifier, definition in self.crs_definitions().items():
            yield f"crs/{identifier}.wkt", definition

    @functools.lru_cache(maxsize=1)
    @ungenerator(dict)
    def crs_definitions(self):
        ld = self.layer_defn
        for i in range(ld.GetGeomFieldCount()):
            spatial_ref = ld.GetGeomFieldDefn(i).GetSpatialRef()
            if spatial_ref:
                yield (
                    crs_util.get_identifier_str(spatial_ref),
                    crs_util.normalise_wkt(spatial_ref.ExportToWkt()),
                )

    @property
    def pk_column_schema(self):
        if not self.primary_key:
            return None

        ld = self.ogrlayer.GetLayerDefn()

        ogr_pk_index = ld.GetFieldIndex(self.primary_key)
        if ogr_pk_index != -1:
            return self._field_to_v2_column_schema(ld.GetFieldDefn(ogr_pk_index))
        else:
            # FID field, isn't an OGR field
            return ColumnSchema(
                ColumnSchema.new_id(), self.primary_key, "integer", 0, size=64
            )

    @property
    @functools.lru_cache(maxsize=1)
    def geometry_column_names(self):
        ld = self.layer_defn
        # Some OGR drivers don't support named geometry fields; the dataset either has a geometry or doesn't.
        # In situations where there _is_ a field, it doesn't necessarily have a name.
        # So here we pick `DEFAULT_GEOMETRY_COLUMN_NAME` ("geom") as the default name.
        # Where there are multiple geom fields, they have names.
        return [
            ld.GetGeomFieldDefn(i).GetName() or self.DEFAULT_GEOMETRY_COLUMN_NAME
            for i in range(ld.GetGeomFieldCount())
        ]

    @property
    def geometry_columns_schema(self):
        ld = self.layer_defn
        return [
            self._geom_field_to_v2_column_schema(ld.GetGeomFieldDefn(i))
            for i in range(ld.GetGeomFieldCount())
        ]

    @property
    def regular_columns_schema(self):
        ld = self.layer_defn
        return [
            self._field_to_v2_column_schema(ld.GetFieldDefn(i))
            for i in range(ld.GetFieldCount())
            if ld.GetFieldDefn(i).GetName() != self.primary_key
        ]

    def _should_import_as_numeric(self, fd):
        if fd.GetSubType():
            return False
        if fd.GetType() not in (ogr.OFTReal, ogr.OFTInteger, ogr.OFTInteger64):
            return False
        # We import numeric real/integer as numeric if they have a
        # nonzero width specified.
        # Unfortunately, that means these three all collide:
        #    NUMERIC (unqualified) --> ogr.Real(0.0) --> double
        #    FLOAT --> ogr.Real(0.0) --> double
        #    DOUBLE PRECISION --> ogr.Real(0.0) --> double
        # This is one reason why we use Kart adapter code rather than OGR code for import,
        # whereever we can.
        #
        # Note that ESRIShapefileImportSource overrides this since OGR *always* reports
        # a nonzero width for floats/ints in shapefiles.
        return fd.GetWidth() != 0

    def _field_to_v2_column_schema(self, fd):
        if self._should_import_as_numeric(fd):
            data_type = "numeric"
            # Note 2: Rather confusingly, OGR's concepts of 'width' and 'precision'
            # correspond to 'precision' and 'scale' in most other systems, respectively:
            extra_type_info = {
                # total number of decimal digits
                "precision": ogr.GetWidth(),
                # total number of decimal digits to the right of the decimal point
                "scale": ogr.GetPrecision(),
            }
        else:
            data_type, extra_type_info = ogr_field_definition_to_kart_type(fd)

        name = fd.GetName()
        pk_index = 0 if name == self.primary_key else None
        return ColumnSchema(
            id=ColumnSchema.new_id(),
            name=name,
            data_type=data_type,
            pk_index=pk_index,
            **extra_type_info,
        )

    def _geom_field_to_v2_column_schema(self, geom_fd):
        name = geom_fd.GetName() or self.DEFAULT_GEOMETRY_COLUMN_NAME
        v2_geom_type = self._get_v2_geometry_type(geom_fd)
        extra_type_info = {"geometryType": v2_geom_type}

        # TODO: Support tables with different CRSs in different columns.
        crs_definitions = list(self.crs_definitions().items())
        if crs_definitions:
            extra_type_info["geometryCRS"] = crs_definitions[0][0]

        return ColumnSchema(
            id=ColumnSchema.new_id(), name=name, data_type="geometry", **extra_type_info
        )

    def _get_v2_geometry_type(self, geom_fd):
        name = geom_fd.GetName() or self.DEFAULT_GEOMETRY_COLUMN_NAME
        v2_type = ogr_geometry_type_to_kart_geometry_type(geom_fd.GetType())

        if self._should_promote_to_multi(name, v2_type):
            return f"MULTI{v2_type}"

        return v2_type

    def _should_promote_to_multi(self, name, v2_geom_type):
        return False

    def _schema_from_db(self):
        pk_col = self.pk_column_schema
        pk_cols = [pk_col] if pk_col else []
        columns = pk_cols + self.geometry_columns_schema + self.regular_columns_schema
        return Schema(columns)

    _KNOWN_METADATA_URIS = {
        "GDALMultiDomainMetadata": "http://gdal.org",
    }


class ESRIShapefileImportSource(OgrTableImportSource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.force_promote_geom_columns = {}

    def _should_import_as_numeric(self, fd):
        if not super()._should_import_as_numeric(fd):
            return False

        # Generally speaking, we import Real/Integer with nonzero 'width'
        # as fixed-width NUMERIC.
        # However, OGR *always* reports a nonzero 'width' for real/integer
        # in shapefiles. They have specific widths.
        # If we find fields with those specific widths, we can assume they're
        # actually ints/doubles, not NUMERIC
        ogr_type = fd.GetType()
        ogr_width = fd.GetWidth()
        ogr_precision = fd.GetPrecision()
        if (
            # double or float
            (ogr_type == ogr.OFTReal and ogr_width == 24 and ogr_precision == 15)
            # smallint or integer. normally integer is 9 but some can be 10
            or (ogr_type == ogr.OFTInteger and ogr_width in (5, 9, 10))
            # integer64. normally width is 18 but can be up to 20
            or (ogr_type == ogr.OFTInteger64 and ogr_width in (18, 19, 20))
        ):
            return False
        return True

    def _should_promote_to_multi(self, name, v2_geom_type):
        # Shapefiles don't distinguish between single- and multi- versions of these geometry types -
        # so we promote the column to the multi-type on import in case there are any multi- instances in that column.
        if v2_geom_type in ("LINESTRING", "POLYGON"):
            forced_type = f"MULTI{v2_geom_type}"
            self.force_promote_geom_columns[name] = forced_type
            return True
        return False

    def _get_type_value_adapter(self, name, v2_type):
        if name in self.force_promote_geom_columns:
            forced_type = self.force_promote_geom_columns[name]
            if forced_type == "MULTILINESTRING":
                return adapt_ogr_force_multilinestring
            elif forced_type == "MULTIPOLYGON":
                return adapt_ogr_force_multipolygon
        return super()._get_type_value_adapter(name, v2_type)


def adapt_ogr_force_multilinestring(value):
    if value is None:
        return value
    return ogr_util.adapt_ogr_geometry(ogr.ForceToMultiLineString(value))


def adapt_ogr_force_multipolygon(value):
    if value is None:
        return value
    return ogr_util.adapt_ogr_geometry(ogr.ForceToMultiPolygon(value))


def postgres_url_to_ogr_conn_str(url):
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
    if scheme not in ("postgres", "postgresql"):
        raise ValueError("Bad scheme")

    # Start with everything from the querystring.
    params = dict(parse_qsl(url.query))

    # Each of these fields can come from the main part of the URL,
    # OR can come from the querystring.
    # If both are specified, the querystring has precedence.
    # So in 'postgresql://host1/?host=host2', the resultant host is 'host2'
    if url.username:
        params.setdefault("user", url.username)
    if url.password:
        params.setdefault("password", url.password)
    if url.hostname:
        params.setdefault("host", unquote(url.hostname))
    if url.port:
        params.setdefault("port", url.port)
    dbname = (url.path or "/")[1:]
    if dbname:
        params.setdefault("dbname", dbname)

    conn_str = " ".join(sorted(f"{k}={v}" for (k, v) in params.items()))
    return f"PG:{conn_str}"
