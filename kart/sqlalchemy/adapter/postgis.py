import decimal
import re

from osgeo.osr import SpatialReference
from psycopg2.extensions import Binary
import sqlalchemy as sa
from sqlalchemy.sql.functions import Function
from sqlalchemy.types import TEXT


from kart import crs_util
from kart.geometry import Geometry
from kart.schema import Schema, ColumnSchema
from kart.sqlalchemy.postgis import Db_Postgis
from kart.sqlalchemy.adapter.base import (
    BaseKartAdapter,
    ConverterType,
    aliased_converter_type,
)
from kart.utils import ungenerator


class KartAdapter_Postgis(BaseKartAdapter, Db_Postgis):
    """
    Adapts a table in PostGIS (and the attached CRS, if there is one) to a V2 dataset.
    Or, does the reverse - adapts a V2 dataset to a PostGIS table (plus attached CRS).
    """

    V2_TYPE_TO_SQL_TYPE = {
        "boolean": "BOOLEAN",
        "blob": "BYTEA",
        "date": "DATE",
        "float": {0: "REAL", 32: "REAL", 64: "DOUBLE PRECISION"},
        "geometry": "GEOMETRY",
        "integer": {
            0: "INTEGER",
            8: "SMALLINT",  # Approximated as smallint (int16)
            16: "SMALLINT",
            32: "INTEGER",
            64: "BIGINT",
        },
        "interval": "INTERVAL",
        "numeric": "NUMERIC",
        "text": "TEXT",
        "time": "TIME",
        "timestamp": {"UTC": "TIMESTAMPTZ", None: "TIMESTAMP"},
    }

    SQL_TYPE_TO_V2_TYPE = {
        "BOOLEAN": "boolean",
        "SMALLINT": ("integer", 16),
        "INTEGER": ("integer", 32),
        "BIGINT": ("integer", 64),
        "REAL": ("float", 32),
        "DOUBLE PRECISION": ("float", 64),
        "BYTEA": "blob",
        "CHARACTER VARYING": "text",
        "DATE": "date",
        "GEOMETRY": "geometry",
        "INTERVAL": "interval",
        "NUMERIC": "numeric",
        "TEXT": "text",
        "TIME": "time",
        "TIMETZ": "time",
        "TIMESTAMP": ("timestamp", None),
        "TIMESTAMPTZ": ("timestamp", "UTC"),
        "VARCHAR": "text",
    }

    # Types that can't be roundtripped perfectly in PostGIS, and what they end up as.
    APPROXIMATED_TYPES = {("integer", 8): ("integer", 16)}

    ZM_FLAG_TO_STRING = {0: "", 1: "M", 2: "Z", 3: "ZM"}

    @classmethod
    def v2_column_schema_to_sql_spec(cls, col, v2_obj=None, has_int_pk=False):
        col_name = cls.quote(col.name)
        sql_type = cls.v2_type_to_sql_type(col, v2_obj)

        # Make int PKs auto-increment.
        if has_int_pk and col.pk_index is not None:
            # SMALLINT, INTEGER, BIGINT -> SMALLSERIAL, SERIAL, BIGSERIAL
            sql_type = re.sub("INT(EGER)?", "SERIAL", sql_type)

        return f"{col_name} {sql_type}"

    @classmethod
    def v2_type_to_sql_type(cls, col, v2_obj=None):
        sql_type = super().v2_type_to_sql_type(col, v2_obj)

        extra_type_info = col.extra_type_info
        if sql_type == "GEOMETRY":
            return cls._v2_geometry_type_to_sql_type(col, v2_obj)

        if sql_type == "TEXT":
            length = extra_type_info.get("length")
            return f"VARCHAR({length})" if length is not None else "TEXT"

        if sql_type == "NUMERIC":
            precision = extra_type_info.get("precision")
            scale = extra_type_info.get("scale")
            if precision is not None and scale is not None:
                return f"NUMERIC({precision},{scale})"
            elif precision is not None:
                return f"NUMERIC({precision})"
            else:
                return "NUMERIC"

        return sql_type

    @classmethod
    def _v2_geometry_type_to_sql_type(cls, col, v2_obj=None):
        extra_type_info = col.extra_type_info
        geometry_type = extra_type_info.get("geometryType")
        if geometry_type is None:
            return "GEOMETRY"

        geometry_type = geometry_type.replace(" ", "")

        crs_id = None
        crs_name = extra_type_info.get("geometryCRS")
        if crs_name is not None and v2_obj is not None:
            crs_id = crs_util.get_identifier_int_from_dataset(v2_obj, crs_name)
        if crs_id is None:
            return f"GEOMETRY({geometry_type})"

        return f"GEOMETRY({geometry_type},{crs_id})"

    @classmethod
    @ungenerator(dict)
    def all_v2_meta_items_including_empty(
        cls, sess, db_schema, table_name, id_salt, include_legacy_items=False
    ):
        """
        Generate all V2 meta items for the given table.
        Varying the id_salt varies the ids that are generated for the schema.json item.
        """
        table_identifier = cls.quote_table(db_schema=db_schema, table_name=table_name)

        title = sess.scalar(
            "SELECT obj_description((:table_identifier)::regclass, 'pg_class');",
            {"table_identifier": table_identifier},
        )
        yield "title", title

        primary_key_sql = """
            SELECT KCU.* FROM information_schema.key_column_usage KCU
            INNER JOIN information_schema.table_constraints TC
            ON KCU.constraint_schema = TC.constraint_schema
            AND KCU.constraint_name = TC.constraint_name
            WHERE TC.constraint_type = 'PRIMARY KEY'
        """

        table_info_sql = f"""
            SELECT
                C.column_name, C.ordinal_position, C.data_type, C.udt_name,
                C.character_maximum_length, C.numeric_precision, C.numeric_scale,
                PK.ordinal_position AS pk_ordinal_position,
                upper(postgis_typmod_type(A.atttypmod)) AS geometry_type,
                postgis_typmod_srid(A.atttypmod) AS geometry_srid
            FROM information_schema.columns C
            LEFT OUTER JOIN ({primary_key_sql}) PK
            ON (PK.table_schema = C.table_schema)
            AND (PK.table_name = C.table_name)
            AND (PK.column_name = C.column_name)
            LEFT OUTER JOIN pg_attribute A
            ON (A.attname = C.column_name)
            AND (A.attrelid = (:table_identifier)::regclass::oid)
            WHERE C.table_schema=:table_schema AND C.table_name=:table_name
            ORDER BY C.ordinal_position;
        """
        r = sess.execute(
            table_info_sql,
            {
                "table_identifier": table_identifier,
                "table_schema": db_schema,
                "table_name": table_name,
            },
        )
        pg_table_info = list(r)

        # Get all the information on the geometry columns that we can get without sampling the geometries:
        geom_cols_info_sql = """
            SELECT GC.f_geometry_column AS column_name, GC.srid, SRS.srtext
            FROM geometry_columns GC
            LEFT OUTER JOIN spatial_ref_sys SRS ON (GC.srid = SRS.srid)
            WHERE GC.f_table_schema=:table_schema AND GC.f_table_name=:table_name;
        """
        r = sess.execute(
            geom_cols_info_sql,
            {"table_schema": db_schema, "table_name": table_name},
        )
        geom_cols_info = [cls._filter_row_to_dict(row) for row in r]

        # Improve the geometry information by sampling one geometry from each column, where available.
        for col_info in geom_cols_info:
            c = col_info["column_name"]
            row = sess.execute(
                f"""
                SELECT ST_Zmflag({cls.quote(c)}) AS zm,
                ST_SRID({cls.quote(c)}) AS srid, SRS.srtext
                FROM {table_identifier} LEFT OUTER JOIN spatial_ref_sys SRS
                ON SRS.srid = ST_SRID({cls.quote(c)})
                WHERE {cls.quote(c)} IS NOT NULL LIMIT 1;
                """,
            ).fetchone()
            if row:
                sampled_info = cls._filter_row_to_dict(row)
                sampled_info['zm'] = cls.ZM_FLAG_TO_STRING.get(sampled_info.get('zm'))
                # Original col_info from geometry_columns takes precedence, where it exists:
                col_info.update({**sampled_info, **col_info})

        schema = cls.postgis_to_v2_schema(pg_table_info, geom_cols_info, id_salt)
        yield "schema.json", schema.to_column_dicts() if schema else None

        for col_info in geom_cols_info:
            wkt = col_info["srtext"]
            id_str = crs_util.get_identifier_str(wkt)
            yield f"crs/{id_str}.wkt", crs_util.normalise_wkt(wkt)

    @classmethod
    def postgis_to_v2_schema(cls, pg_table_info, geom_cols_info, id_salt):
        """Generate a V2 schema from the given postgis metadata tables."""
        return Schema(
            [
                cls._postgis_to_column_schema(col, geom_cols_info, id_salt)
                for col in pg_table_info
            ]
        )

    @classmethod
    def _postgis_to_column_schema(cls, pg_col_info, geom_cols_info, id_salt):
        """
        Given the postgis column info for a particular column, and some extra context in
        case it is a geometry column, converts it to a ColumnSchema. The extra context will
        only be used if the given pg_col_info is the geometry column.
        Parameters:
        pg_col_info - info about a single column from pg_table_info.
        geom_cols_info - a list of dicts, where keys are "column_name", "srid", "srtext", "zm".
        id_salt - the UUIDs of the generated ColumnSchema are deterministic and depend on
        the name and type of the column, and on this salt.
        """
        name = pg_col_info["column_name"]
        pk_index = pg_col_info["pk_ordinal_position"]
        if pk_index is not None:
            pk_index -= 1
        data_type, extra_type_info = cls._pg_type_to_v2_type(
            pg_col_info, geom_cols_info
        )

        col_id = ColumnSchema.deterministic_id(name, data_type, id_salt)
        return ColumnSchema(col_id, name, data_type, pk_index, **extra_type_info)

    @classmethod
    def _pg_type_to_v2_type(cls, pg_col_info, geom_cols_info):
        sql_type = pg_col_info["data_type"].upper()
        if sql_type not in cls.SQL_TYPE_TO_V2_TYPE:
            sql_type = pg_col_info["udt_name"].upper()

        v2_type, extra_type_info = super().sql_type_to_v2_type(sql_type)

        if v2_type == "geometry":
            return cls._pg_type_to_v2_geometry_type(pg_col_info, geom_cols_info)

        if v2_type == "text":
            length = pg_col_info["character_maximum_length"] or None
            if length is not None:
                extra_type_info["length"] = length

        if v2_type == "numeric":
            extra_type_info["precision"] = pg_col_info["numeric_precision"] or None
            extra_type_info["scale"] = pg_col_info["numeric_scale"] or None

        return v2_type, extra_type_info

    @classmethod
    def _pg_type_to_v2_geometry_type(cls, pg_col_info, geom_cols_info):
        """
        col_name - the name of the column.
        geom_cols_info - a list of dicts, where keys are "column_name", "srid", "srtext", "zm".
        """
        name = pg_col_info["column_name"]
        geometry_type = pg_col_info["geometry_type"].upper()
        # Look for Z, M, or ZM suffix
        geometry_type, zm = cls._separate_zm_suffix(geometry_type)
        geometry_crs = None

        geom_col_info = next(
            (g for g in geom_cols_info if g["column_name"] == name), None
        )
        if geom_col_info:
            zm = zm or geom_col_info.get("zm") or ""
            wkt = geom_col_info.get("srtext")
            if wkt:
                geometry_crs = crs_util.get_identifier_str(wkt)

        geometry_type = f"{geometry_type} {zm}".strip()

        return "geometry", {"geometryType": geometry_type, "geometryCRS": geometry_crs}

    @classmethod
    def _separate_zm_suffix(cls, geometry_type):
        for suffix in ("ZM", "Z", "M"):
            if geometry_type.endswith(suffix):
                return geometry_type[: -len(suffix)].strip(), suffix
        return geometry_type, ""

    @classmethod
    def generate_postgis_spatial_ref_sys(cls, v2_obj):
        """
        Generates the contents of the spatial_ref_sys table from the v2 object.
        The result is a list containing a dict per table row.
        Each dict has the format {column-name: value}.
        """
        result = []
        for crs_name, definition in v2_obj.crs_definitions().items():
            spatial_ref = SpatialReference(definition)
            auth_name = spatial_ref.GetAuthorityName(None) or "NONE"
            crs_id = crs_util.get_identifier_int(spatial_ref)
            result.append(
                {
                    "srid": crs_id,
                    "auth_name": auth_name,
                    "auth_srid": crs_id,
                    "srtext": definition,
                    "proj4text": spatial_ref.ExportToProj4(),
                }
            )
        return result

    @classmethod
    @ungenerator(dict)
    def _filter_row_to_dict(cls, row):
        """Turns a db row into a dict, but leaves out key-value pairs with falsey values."""
        for key, value in zip(row.keys(), row):
            if value:
                yield key, value

    @classmethod
    def _type_def_for_column_schema(cls, col, dataset=None):
        if col.data_type == "geometry":
            # This :converter-type adapts Kart's GPKG geometry to EWKB which is what PostGIS supports.
            return GeometryType
        elif col.data_type == "blob":
            return BlobType
        elif col.data_type == "date":
            return DateType
        elif col.data_type == "interval":
            return IntervalType
        elif col.data_type == "numeric":
            return NumericType
        elif col.data_type == "time":
            return TimeType
        elif col.data_type == "timestamp":
            return TimestampType(col.extra_type_info.get("timezone"))
        elif col.data_type == "text":
            return TextType
        else:
            # Don't need to specify type information for other columns at present, since we just pass through the values.
            return None


@aliased_converter_type
class GeometryType(ConverterType):
    """ConverterType so that V2 geometry is adapted to EWKB for PostGIS."""

    def python_prewrite(self, geom):
        return Binary(geom.to_ewkb()) if geom is not None else None

    def python_postread(self, geom):
        return Geometry.from_hex_ewkb(geom)


@aliased_converter_type
class BlobType(ConverterType):
    # ConverterType to get read blobs as type <bytes> instead of type <memory>.
    def python_postread(self, blob):
        return bytes(blob) if blob is not None else None


@aliased_converter_type
class DateType(ConverterType):
    # ConverterType to read dates as text. They are stored in PG as DATE but we read them back as text.
    def sql_read(self, column):
        return Function("to_char", column, "YYYY-MM-DD", type_=self)


@aliased_converter_type
class IntervalType(ConverterType):
    """ConverterType to that casts intervals to text - ISO8601 mode is set for durations so this does what we want."""

    def sql_read(self, column):
        return sa.cast(column, TEXT)


@aliased_converter_type
class NumericType(ConverterType):
    """ConverterType to read numerics as text. They are stored in PG as NUMERIC but we read them back as text."""

    def python_postread(self, value):
        return (
            str(value).rstrip("0").rstrip(".")
            if isinstance(value, decimal.Decimal)
            else value
        )


@aliased_converter_type
class TimeType(ConverterType):
    # ConverterType to read times as text. They are stored in PG as TIME but we read them back as text.
    def sql_read(self, column):
        return Function("to_char", column, "HH24:MI:SS", type_=self)


@aliased_converter_type
class TimestampType(ConverterType):
    """
    ConverterType so that the Z timezone suffix is added in when written (for UTC timestamps),
    and so that timestamps are read as text, without a timezone.
    """

    def __init__(self, timezone):
        self.timezone = timezone

    def python_prewrite(self, timestamp):
        if isinstance(timestamp, str):
            if self.timezone is None:
                return timestamp.rstrip("Z")
            elif self.timezone == "UTC" and not timestamp.endswith("Z"):
                return f"{timestamp}Z"

        return timestamp

    def sql_read(self, column):
        return Function("to_char", column, 'YYYY-MM-DD"T"HH24:MI:SS', type_=self)


@aliased_converter_type
class TextType(ConverterType):
    """ConverterType to that casts everything to text in the Python layer. Handles things like UUIDs."""

    def python_postread(self, value):
        return str(value) if value is not None else None
