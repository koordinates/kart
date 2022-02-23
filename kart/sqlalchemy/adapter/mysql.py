import decimal


import sqlalchemy as sa
from sqlalchemy.sql.functions import Function
from sqlalchemy.dialects.mysql.types import DOUBLE


from kart import crs_util
from kart.geometry import Geometry
from kart.exceptions import NotYetImplemented
from kart.tabular.schema import Schema, ColumnSchema
from kart.sqlalchemy.mysql import Db_MySql
from kart.sqlalchemy.adapter.base import (
    BaseKartAdapter,
    ConverterType,
    aliased_converter_type,
)
from kart.utils import ungenerator


class KartAdapter_MySql(BaseKartAdapter, Db_MySql):
    """
    Adapts a table in MySQL (and the attached CRS, if there is one) to a V2 dataset.
    Or, does the reverse - adapts a V2 dataset to a MySQL table (plus attached CRS).
    """

    V2_TYPE_TO_SQL_TYPE = {
        "boolean": "BIT",
        "blob": "LONGBLOB",
        "date": "DATE",
        "float": {0: "FLOAT", 32: "FLOAT", 64: "DOUBLE PRECISION"},
        "geometry": "GEOMETRY",
        "integer": {
            0: "INT",
            8: "TINYINT",
            16: "SMALLINT",
            32: "INT",
            64: "BIGINT",
        },
        "interval": "TEXT",
        "numeric": "NUMERIC",
        "text": "LONGTEXT",
        "time": "TIME",
        "timestamp": {"UTC": "TIMESTAMP", None: "DATETIME"},
    }

    _TEXT_AND_BLOB_PREFIXES = ("TINY", "MEDIUM", "LONG")

    SQL_TYPE_TO_V2_TYPE = {
        "BIT": "boolean",
        "TINYINT": ("integer", 8),
        "SMALLINT": ("integer", 16),
        "INT": ("integer", 32),
        "BIGINT": ("integer", 64),
        "FLOAT": ("float", 32),
        "DOUBLE": ("float", 64),
        "DOUBLE PRECISION": ("float", 64),
        "BINARY": "blob",
        "BLOB": "blob",
        "CHAR": "text",
        "DATE": "date",
        "DATETIME": ("timestamp", None),
        "DECIMAL": "numeric",
        "GEOMETRY": "geometry",
        "NUMERIC": "numeric",
        "TEXT": "text",
        "TIME": "time",
        "TIMESTAMP": ("timestamp", "UTC"),
        "VARCHAR": "text",
        "VARBINARY": "blob",
        **{f"{prefix}TEXT": "text" for prefix in _TEXT_AND_BLOB_PREFIXES},
        **{f"{prefix}BLOB": "blob" for prefix in _TEXT_AND_BLOB_PREFIXES},
    }

    # Types that can't be roundtripped perfectly in MySQL, and what they end up as.
    APPROXIMATED_TYPES = {"interval": "text"}

    # Extra type info that might be missing/extra due to an approximated type.
    APPROXIMATED_TYPES_EXTRA_TYPE_INFO = ("length",)

    MYSQL_GEOMETRY_TYPES = {
        "GEOMETRY",
        "POINT",
        "LINESTRING",
        "POLYGON",
        "MULTIPOINT",
        "MULTILINESTRING",
        "MULTIPOLYGON",
        "GEOMETRYCOLLECTION",
    }

    @classmethod
    def v2_column_schema_to_sql_spec(cls, col, v2_obj=None, has_int_pk=False):
        result = super().v2_column_schema_to_sql_spec(col, v2_obj)

        # Make int PKs auto-increment.
        if has_int_pk and col.pk_index is not None:
            result += " AUTO_INCREMENT"

        return result

    @classmethod
    def v2_type_to_sql_type(cls, col, v2_obj=None):
        sql_type = super().v2_type_to_sql_type(col, v2_obj)

        extra_type_info = col.extra_type_info
        if sql_type == "GEOMETRY":
            return cls._v2_geometry_type_to_sql_type(col, v2_obj)

        if sql_type in ("LONGTEXT, LONGBLOB"):
            length = extra_type_info.get("length")
            if length and length > 0 and length <= cls._MAX_SPECIFIABLE_LENGTH:
                if sql_type == "LONGTEXT":
                    return f"VARCHAR({length})"
                elif sql_type == "LONGBLOB":
                    return f"VARBINARY({length})"
            return sql_type

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
    def _v2_geometry_type_to_sql_type(cls, column_schema, v2_obj=None):
        extra_type_info = column_schema.extra_type_info
        geometry_type = extra_type_info.get("geometryType", "geometry")
        geometry_type_parts = geometry_type.strip().split(" ")
        if len(geometry_type_parts) > 1:
            raise NotYetImplemented(
                "Three or four dimensional geometries are not supported by MySQL working copy: "
                f'("{column_schema.name}" {geometry_type.upper()})'
            )

        mysql_type = geometry_type_parts[0]

        crs_id = None
        crs_name = extra_type_info.get("geometryCRS")
        if crs_name is not None and v2_obj is not None:
            crs_id = crs_util.get_identifier_int_from_dataset(v2_obj, crs_name)
        if crs_id is not None:
            mysql_type += f" SRID {crs_id}"

        return mysql_type

    @classmethod
    @ungenerator(dict)
    def all_v2_meta_items_including_empty(
        cls, sess, db_schema, table_name, id_salt=None, include_legacy_items=False
    ):
        title = sess.scalar(
            """
            SELECT table_comment FROM information_schema.tables
            WHERE table_schema=:table_schema AND table_name=:table_name;
            """,
            {"table_schema": db_schema, "table_name": table_name},
        )
        yield "title", title

        # Primary key SQL is a bit different for MySQL since constraints are named within the namespace of a table -
        # they don't names that are globally unique within the db-schema.
        primary_key_sql = """
            SELECT KCU.* FROM information_schema.key_column_usage KCU
            INNER JOIN information_schema.table_constraints TC
            ON KCU.table_schema = TC.table_schema
            AND KCU.table_name = TC.table_name
            AND KCU.constraint_schema = TC.constraint_schema
            AND KCU.constraint_name = TC.constraint_name
            WHERE TC.constraint_type = 'PRIMARY KEY'
        """

        table_info_sql = f"""
            SELECT
                C.column_name, C.ordinal_position, C.data_type, C.srs_id,
                C.character_maximum_length, C.numeric_precision, C.numeric_scale,
                PK.ordinal_position AS pk_ordinal_position
            FROM information_schema.columns C
            LEFT OUTER JOIN ({primary_key_sql}) PK
            ON (PK.table_schema = C.table_schema)
            AND (PK.table_name = C.table_name)
            AND (PK.column_name = C.column_name)
            WHERE C.table_schema=:table_schema AND C.table_name=:table_name
            ORDER BY C.ordinal_position;
        """
        r = sess.execute(
            table_info_sql,
            {"table_schema": db_schema, "table_name": table_name},
        )
        mysql_table_info = list(r)

        spatial_ref_sys_sql = """
            SELECT SRS.* FROM information_schema.st_spatial_reference_systems SRS
            LEFT OUTER JOIN information_schema.st_geometry_columns GC ON (GC.srs_id = SRS.srs_id)
            WHERE GC.table_schema=:table_schema AND GC.table_name=:table_name;
        """
        r = sess.execute(
            spatial_ref_sys_sql,
            {"table_schema": db_schema, "table_name": table_name},
        )
        mysql_spatial_ref_sys = list(r)

        schema = KartAdapter_MySql.sqlserver_to_v2_schema(
            mysql_table_info, mysql_spatial_ref_sys, id_salt
        )
        yield "schema.json", schema.to_column_dicts()

        for crs_info in mysql_spatial_ref_sys:
            wkt = crs_info["DEFINITION"]
            id_str = crs_util.get_identifier_str(wkt)
            yield f"crs/{id_str}.wkt", crs_util.normalise_wkt(wkt)

    _MAX_SPECIFIABLE_LENGTH = 0xFFFF

    @classmethod
    def sqlserver_to_v2_schema(cls, mysql_table_info, mysql_crs_info, id_salt):
        """Generate a V2 schema from the given My SQL metadata."""
        return Schema(
            [
                cls._mysql_to_column_schema(col, mysql_crs_info, id_salt)
                for col in mysql_table_info
            ]
        )

    @classmethod
    def _mysql_to_column_schema(cls, mysql_col_info, mysql_crs_info, id_salt):
        """
        Given the MySQL column info for a particular column, converts it to a ColumnSchema.

        Parameters:
        mysql_col_info - info about a single column from mysql_table_info.
        mysql_crs_info - info about all the CRS, in case this column is a geometry column.
        id_salt - the UUIDs of the generated ColumnSchema are deterministic and depend on
                  the name and type of the column, and on this salt.
        """
        name = mysql_col_info["COLUMN_NAME"]
        pk_index = mysql_col_info["pk_ordinal_position"]
        if pk_index is not None:
            pk_index -= 1
        if mysql_col_info["DATA_TYPE"].upper() in cls.MYSQL_GEOMETRY_TYPES:
            data_type, extra_type_info = cls._mysql_type_to_v2_geometry_type(
                mysql_col_info, mysql_crs_info
            )
        else:
            data_type, extra_type_info = cls._mysql_type_to_v2_type(mysql_col_info)

        col_id = ColumnSchema.deterministic_id(name, data_type, id_salt)
        return ColumnSchema(col_id, name, data_type, pk_index, **extra_type_info)

    @classmethod
    def _mysql_type_to_v2_type(cls, mysql_col_info):
        sql_type = mysql_col_info["DATA_TYPE"].upper()
        v2_type, extra_type_info = super().sql_type_to_v2_type(sql_type)

        if v2_type in ("text", "blob"):
            length = mysql_col_info["CHARACTER_MAXIMUM_LENGTH"]
            if length and length > 0 and length <= cls._MAX_SPECIFIABLE_LENGTH:
                extra_type_info["length"] = length

        if v2_type == "numeric":
            extra_type_info["precision"] = mysql_col_info["NUMERIC_PRECISION"] or None
            extra_type_info["scale"] = mysql_col_info["NUMERIC_SCALE"] or None

        return v2_type, extra_type_info

    @classmethod
    def _mysql_type_to_v2_geometry_type(cls, mysql_col_info, mysql_crs_info):
        geometry_type = mysql_col_info["DATA_TYPE"].upper()
        geometry_crs = None

        crs_id = mysql_col_info["SRS_ID"]
        if crs_id:
            crs_info = next((r for r in mysql_crs_info if r["SRS_ID"] == crs_id), None)
            if crs_info:
                geometry_crs = crs_util.get_identifier_str(crs_info["DEFINITION"])

        return "geometry", {"geometryType": geometry_type, "geometryCRS": geometry_crs}

    @classmethod
    def generate_mysql_spatial_ref_sys(cls, v2_obj):
        """
        Generates the contents of the spatial_referece_system table from the v2 object.
        The result is a list containing a dict per table row.
        Each dict has the format {column-name: value}.
        """
        result = []
        for crs_name, definition in v2_obj.crs_definitions().items():
            auth_name, auth_code = crs_util.parse_authority(definition)
            crs_id = crs_util.get_identifier_int(definition)
            result.append(
                {
                    "srs_id": crs_id,
                    "name": crs_util.parse_name(definition),
                    "definition": crs_util.mysql_compliant_wkt(definition),
                    "organization": auth_name,
                    "org_id": crs_id,
                }
            )
        return result

    @classmethod
    def _type_def_for_column_schema(self, col, dataset=None):
        if col.data_type == "geometry":
            crs_name = col.extra_type_info.get("geometryCRS")
            crs_id = None
            if dataset is not None:
                crs_id = (
                    crs_util.get_identifier_int_from_dataset(dataset, crs_name) or 0
                )
            # This user-defined GeometryType adapts Kart's GPKG geometry to SQL Server's native geometry type.
            return GeometryType(crs_id)
        elif col.data_type == "boolean":
            return BooleanType
        elif col.data_type == "float" and col.extra_type_info.get("size") != 64:
            return FloatType
        elif col.data_type == "date":
            return DateType
        elif col.data_type == "numeric":
            return NumericType
        elif col.data_type == "time":
            return TimeType
        elif col.data_type == "timestamp":
            return TimestampType
        elif col.data_type == "text":
            return TextType
        else:
            # Don't need to specify type information for other columns at present, since we just pass through the values.
            return None


@aliased_converter_type
class GeometryType(ConverterType):
    """ConverterType so that V2 geometry is adapted to MySQL binary format."""

    # In Kart, all geometries are stored as WKB with axis-order=long-lat - since this is the GPKG
    # standard, and a Kart geometry is a normalised GPKG geometry. MySQL has to be explicitly told
    # that this is the ordering we use in WKB, since MySQL would otherwise expect lat-long ordering
    # as specified by ISO 19128:2005.
    AXIS_ORDER = "axis-order=long-lat"

    def __init__(self, crs_id):
        self.crs_id = crs_id

    def python_prewrite(self, geom):
        # 1. Writing - Python layer - convert Kart geometry to WKB
        return geom.to_wkb() if geom else None

    def sql_write(self, bindvalue):
        # 2. Writing - SQL layer - wrap in call to ST_GeomFromWKB to convert WKB to MySQL binary.
        return Function(
            "ST_GeomFromWKB", bindvalue, self.crs_id, self.AXIS_ORDER, type_=self
        )

    def sql_read(self, column):
        # 3. Reading - SQL layer - wrap in call to ST_AsBinary() to convert MySQL binary to WKB.
        return Function("ST_AsBinary", column, self.AXIS_ORDER, type_=self)

    def python_postread(self, wkb):
        # 4. Reading - Python layer - convert WKB to Kart geometry.
        return Geometry.from_wkb(wkb)


@aliased_converter_type
class BooleanType(ConverterType):
    # ConverterType to read booleans. They are stored in MySQL as Bits but we read them back as bools.
    def python_postread(self, bits):
        # Reading - Python layer - convert bytes to boolean.
        value = int.from_bytes(bits, "big") if isinstance(bits, bytes) else bits
        return bool(value) if value in (0, 1) else value


@aliased_converter_type
class DateType(ConverterType):
    # ConverterType to read Dates as text. They are stored in MySQL as Dates but we read them back as text.
    def sql_read(self, column):
        # Reading - SQL layer - convert date to string in ISO8601.
        # https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html
        return Function("DATE_FORMAT", column, "%Y-%m-%d", type_=self)


@aliased_converter_type
class FloatType(ConverterType):
    # ConverterType to read floats as doubles. For some reason, floats they are rounded so they keep
    # even less than single-float precision if we read them as floats.
    def sql_read(self, col):
        return sa.cast(col, DOUBLE)


@aliased_converter_type
class NumericType(ConverterType):
    """ConverterType to read numerics as text. They are stored in MySQL as NUMERIC but we read them back as text."""

    def python_postread(self, value):
        return (
            str(value).rstrip("0").rstrip(".")
            if isinstance(value, decimal.Decimal)
            else value
        )


@aliased_converter_type
class TimeType(ConverterType):
    # ConverterType to read Times as text. They are stored in MySQL as Times but we read them back as text.
    def sql_read(self, col):
        # Reading - SQL layer - convert timestamp to string in ISO8601.
        # https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html
        return Function("DATE_FORMAT", col, "%H:%i:%S", type_=self)


@aliased_converter_type
class TimestampType(ConverterType):
    """
    ConverterType to read Timestamps as text. They are stored in MySQL as Timestamps but we read them back as text.
    """

    def python_prewrite(self, timestamp):
        # 1. Writing - Python layer - remove timezone specifier - MySQL can't read timezone specifiers.
        # Instead, it uses the session timezone (UTC) when writing a timestamp with timezone.
        # (Datasets V2 shouldn't have a timezone specifier anyway, but it may be present for legacy reasons).
        return timestamp.rstrip("Z") if isinstance(timestamp, str) else timestamp

    def sql_read(self, col):
        # 2. Reading - SQL layer - convert timestamp to string in ISO8601 with Z as the timezone specifier.
        # https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html
        return Function("DATE_FORMAT", col, "%Y-%m-%dT%H:%i:%S", type_=self)


@aliased_converter_type
class TextType(ConverterType):
    """ConverterType to that casts everything to text in the Python layer. Handles things like UUIDs."""

    def python_postread(self, value):
        return str(value) if value is not None else None
