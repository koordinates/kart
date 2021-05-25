from kart import crs_util
from kart.exceptions import NotYetImplemented
from kart.schema import Schema, ColumnSchema

from kart.sqlalchemy.mysql import Db_MySql
from kart.sqlalchemy.adapter.base import BaseKartAdapter


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
        "timestamp": "TIMESTAMP",
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
        "DATETIME": "timestamp",
        "DECIMAL": "numeric",
        "GEOMETRY": "geometry",
        "NUMERIC": "numeric",
        "TEXT": "text",
        "TIME": "time",
        "TIMESTAMP": "timestamp",
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
    def v2_schema_to_sql_spec(cls, schema, v2_obj):
        """
        Generate the SQL CREATE TABLE spec from a V2 object eg:
        'fid INTEGER, geom POINT WITH CRSID 2136, desc VARCHAR(128), PRIMARY KEY(fid)'
        """
        result = [cls.v2_column_schema_to_mysql_spec(col, v2_obj) for col in schema]

        if schema.pk_columns:
            pk_col_names = ", ".join((cls.quote(col.name) for col in schema.pk_columns))
            result.append(f"PRIMARY KEY({pk_col_names})")

        return ", ".join(result)

    @classmethod
    def all_v2_meta_items(cls, sess, db_schema, table_name, id_salt=None):
        title = sess.scalar(
            """
            SELECT table_comment FROM information_schema.tables
            WHERE table_schema=:table_schema AND table_name=:table_name;
            """,
            {"table_schema": db_schema, "table_name": table_name},
        )
        yield "title", title

        table_info_sql = """
            SELECT
                C.column_name, C.ordinal_position, C.data_type, C.srs_id,
                C.character_maximum_length, C.numeric_precision, C.numeric_scale,
                KCU.ordinal_position AS pk_ordinal_position
            FROM information_schema.columns C
            LEFT OUTER JOIN information_schema.key_column_usage KCU
            ON (KCU.table_schema = C.table_schema)
            AND (KCU.table_name = C.table_name)
            AND (KCU.column_name = C.column_name)
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

    @classmethod
    def v2_column_schema_to_mysql_spec(cls, column_schema, v2_obj):
        name = column_schema.name
        mysql_type = cls._v2_type_to_mysql_type(column_schema, v2_obj)

        return " ".join([cls.quote(name), mysql_type])

    _MAX_SPECIFIABLE_LENGTH = 0xFFFF

    @classmethod
    def _v2_type_to_mysql_type(cls, column_schema, v2_obj):
        """Convert a v2 schema type to a MySQL type."""
        v2_type = column_schema.data_type
        if v2_type == "geometry":
            return cls._v2_geometry_type_to_mysql_type(column_schema, v2_obj)

        extra_type_info = column_schema.extra_type_info

        mysql_type_info = cls.V2_TYPE_TO_SQL_TYPE.get(v2_type)
        if mysql_type_info is None:
            raise ValueError(f"Unrecognised data type: {v2_type}")

        if isinstance(mysql_type_info, dict):
            return mysql_type_info.get(extra_type_info.get("size", 0))

        mysql_type = mysql_type_info

        length = extra_type_info.get("length", None)
        if length and length > 0 and length <= cls._MAX_SPECIFIABLE_LENGTH:
            if mysql_type == "LONGTEXT":
                return f"VARCHAR({length})"
            elif mysql_type == "longblob":
                return f"VARBINARY({length})"

        if mysql_type == "NUMERIC":
            precision = extra_type_info.get("precision", None)
            scale = extra_type_info.get("scale", None)
            if precision is not None and scale is not None:
                return f"NUMERIC({precision},{scale})"
            elif precision is not None:
                return f"NUMERIC({precision})"
            else:
                return "NUMERIC"

        return mysql_type

    @classmethod
    def _v2_geometry_type_to_mysql_type(cls, column_schema, v2_obj):
        extra_type_info = column_schema.extra_type_info
        geometry_type = extra_type_info.get("geometryType", "geometry")
        geometry_type_parts = geometry_type.strip().split(" ")
        if len(geometry_type_parts) > 1:
            raise NotYetImplemented(
                "Three or four dimensional geometries are not supported by MySQL working copy: "
                f'("{column_schema.name}" {geometry_type.upper()})'
            )

        mysql_type = geometry_type_parts[0]

        crs_name = extra_type_info.get("geometryCRS")
        crs_id = crs_util.get_identifier_int_from_dataset(v2_obj, crs_name)
        if crs_id is not None:
            mysql_type += f" SRID {crs_id}"

        return mysql_type

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
        mysql_type = mysql_col_info["DATA_TYPE"].upper()
        v2_type_info = cls.SQL_TYPE_TO_V2_TYPE.get(mysql_type)

        if isinstance(v2_type_info, tuple):
            v2_type = v2_type_info[0]
            extra_type_info = {"size": v2_type_info[1]}
        else:
            v2_type = v2_type_info
            extra_type_info = {}

        if v2_type in ("text", "blob"):
            length = mysql_col_info["CHARACTER_MAXIMUM_LENGTH"] or None
            if (
                length is not None
                and length > 0
                and length <= cls._MAX_SPECIFIABLE_LENGTH
            ):
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
        for crs_name, definition in v2_obj.crs_definitions():
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
