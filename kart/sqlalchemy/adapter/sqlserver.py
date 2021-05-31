from sqlalchemy import literal_column
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import quoted_name
from sqlalchemy.sql.functions import Function


from kart import crs_util
from kart.geometry import Geometry
from kart.schema import Schema, ColumnSchema
from kart.sqlalchemy.sqlserver import Db_SqlServer
from kart.sqlalchemy.adapter.base import (
    BaseKartAdapter,
    ConverterType,
    aliased_converter_type,
)


# Adds all CURVE subtypes to GEOMETRY's subtypes since CURVE is a subtype of GEOMETRY, and so on.
def _build_transitive_subtypes(direct_subtypes, type_, result=None):
    if result is None:
        result = {}

    subtypes = set()
    subtypes |= direct_subtypes.get(type_, set())
    sub_subtypes = set()
    for subtype in subtypes:
        sub_subtypes |= _build_transitive_subtypes(direct_subtypes, subtype, result)[
            subtype
        ]

    subtypes |= sub_subtypes
    # type_ is also considered to be a subtype of type_ for our purposes:
    subtypes.add(type_)
    result[type_] = subtypes

    # Also key this data by upper case name, so we can find it in a case-insensitive manner
    # (since V2 geometry types are uppercase).
    result[type_.upper()] = subtypes
    return result


class KartAdapter_SqlServer(BaseKartAdapter, Db_SqlServer):
    """
    Adapts a table in SQL Server (and the attached CRS, if there is one) to a V2 dataset.
    Or, does the reverse - adapts a V2 dataset to a SQL Server table.
    Note that writing custom CRS to a SQL Server instance is not possible.
    """

    V2_TYPE_TO_SQL_TYPE = {
        "boolean": "BIT",
        "blob": "VARBINARY",
        "date": "DATE",
        "float": {0: "REAL", 32: "REAL", 64: "FLOAT"},
        "geometry": "GEOMETRY",
        "integer": {
            0: "INT",
            8: "TINYINT",
            16: "SMALLINT",
            32: "INT",
            64: "BIGINT",
        },
        "interval": "TEXT",  # Approximated
        "numeric": "NUMERIC",
        "text": "NVARCHAR",
        "time": "TIME",
        "timestamp": "DATETIMEOFFSET",
    }

    SQL_TYPE_TO_V2_TYPE = {
        "BIT": "boolean",
        "TINYINT": ("integer", 8),
        "SMALLINT": ("integer", 16),
        "INT": ("integer", 32),
        "BIGINT": ("integer", 64),
        "REAL": ("float", 32),
        "FLOAT": ("float", 64),
        "BINARY": "blob",
        "CHAR": "text",
        "DATE": "date",
        "DATETIME": "timestamp",
        "DATETIME2": "timestamp",
        "DATETIMEOFFSET": "timestamp",
        "DECIMAL": "numeric",
        "GEOGRAPHY": "geometry",
        "GEOMETRY": "geometry",
        "NCHAR": "text",
        "NUMERIC": "numeric",
        "NVARCHAR": "text",
        "NTEXT": "text",
        "TEXT": "text",
        "TIME": "time",
        "VARCHAR": "text",
        "VARBINARY": "blob",
    }

    # Types that can't be roundtripped perfectly in SQL Server, and what they end up as.
    APPROXIMATED_TYPES = {"interval": "text"}
    # Note that although this means that all other V2 types above can be roundtripped, it
    # doesn't mean that extra type info is always preserved.
    # Specifically, the geometryType is not roundtripped.

    # Extra type info that might be missing/extra due to an approximated type.
    APPROXIMATED_TYPES_EXTRA_TYPE_INFO = ("length",)

    # Used for constraining a column to be of a certain type, including subtypes of that type.
    # The CHECK need to explicitly list all types and subtypes, eg for SURFACE:
    # >>> CHECK(geom.STGeometryType() IN ('SURFACE','POLYGON','CURVEPOLYGON'))
    _MS_GEOMETRY_DIRECT_SUBTYPES = {
        "Geometry": set(["Point", "Curve", "Surface", "GeometryCollection"]),
        "Curve": set(["LineString", "CircularString", "CompoundCurve"]),
        "Surface": set(["Polygon", "CurvePolygon"]),
        "GeometryCollection": set(["MultiPoint", "MultiCurve", "MultiSurface"]),
        "MultiCurve": set(["MultiLineString"]),
        "MultiSurface": set(["MultiPolygon"]),
    }

    _MS_GEOMETRY_SUBTYPES = _build_transitive_subtypes(
        _MS_GEOMETRY_DIRECT_SUBTYPES, "Geometry"
    )

    @classmethod
    def v2_column_schema_to_sql_spec(cls, col, v2_obj=None):
        result = super().v2_column_schema_to_sql_spec(col, v2_obj)
        constraints = cls.get_sql_type_constraints(col, v2_obj)
        return f"{result} {constraints}" if constraints else result

    @classmethod
    def get_sql_type_constraints(cls, col, v2_obj=None):
        if col.data_type != "geometry":
            return None

        constraints = []

        extra_type_info = col.extra_type_info
        geometry_type = extra_type_info.get("geometryType")
        if geometry_type is not None:
            geometry_type = geometry_type.split(" ")[0].upper()
            if geometry_type != "GEOMETRY":
                constraints.append(
                    cls._geometry_type_constraint(col.name, geometry_type)
                )

        if v2_obj is not None:
            crs_name = extra_type_info.get("geometryCRS")
            crs_id = crs_util.get_identifier_int_from_dataset(v2_obj, crs_name)
            if crs_id is not None:
                constraints.append(cls._geometry_crs_constraint(col.name, crs_id))

        return f"CHECK({' AND '.join(constraints)})" if constraints else None

    @classmethod
    def v2_type_to_sql_type(cls, col, v2_obj=None):
        sql_type = super().v2_type_to_sql_type(col, v2_obj)

        extra_type_info = col.extra_type_info
        if sql_type in ("VARCHAR", "NVARCHAR", "VARBINARY"):
            length = extra_type_info.get("length")
            return f"{sql_type}({length})" if length is not None else f"{sql_type}(max)"

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
    def all_v2_meta_items(cls, sess, db_schema, table_name, id_salt=None):
        """
        Generate all V2 meta items for the given table.
        Varying the id_salt varies the ids that are generated for the schema.json item.
        """
        title = sess.scalar(
            """
            SELECT CAST(value AS NVARCHAR) FROM::fn_listextendedproperty(
                'MS_Description', 'schema', :schema, 'table', :table, null, null);
            """,
            {"schema": db_schema, "table": table_name},
        )
        yield "title", title

        table_info_sql = """
            SELECT
                C.column_name, C.ordinal_position, C.data_type,
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
        ms_table_info = list(r)

        geom_cols = [
            row['column_name']
            for row in ms_table_info
            if row['data_type'] in ('geometry', 'geography')
        ]

        table_identifier = cls.quote_table(db_schema=db_schema, table_name=table_name)
        ms_spatial_ref_sys = [
            sess.execute(
                f"""
                SELECT TOP 1 :column_name AS column_name, {cls.quote(g)}.STSrid AS srid, SRS.*
                FROM {table_identifier}
                LEFT OUTER JOIN sys.spatial_reference_systems SRS
                ON SRS.spatial_reference_id = {cls.quote(g)}.STSrid
                WHERE {cls.quote(g)} IS NOT NULL;
                """,
                {"column_name": g},
            ).fetchone()
            for g in geom_cols
        ]
        ms_spatial_ref_sys = list(filter(None, ms_spatial_ref_sys))  # Remove nulls.

        schema = KartAdapter_SqlServer.sqlserver_to_v2_schema(
            ms_table_info, ms_spatial_ref_sys, id_salt
        )
        yield "schema.json", schema.to_column_dicts()

        for crs_info in ms_spatial_ref_sys:
            auth_name = crs_info["authority_name"]
            auth_code = crs_info["authorized_spatial_reference_id"]
            if not auth_name and not auth_code:
                auth_name, auth_code = "CUSTOM", crs_info["srid"]
            wkt = crs_info["well_known_text"] or ""
            yield f"crs/{auth_name}:{auth_code}.wkt", crs_util.normalise_wkt(
                crs_util.ensure_authority_specified(wkt, auth_name, auth_code)
            )

    @classmethod
    def _geometry_type_constraint(cls, col_name, geometry_type):
        ms_geometry_types = cls._MS_GEOMETRY_SUBTYPES.get(geometry_type.upper())
        ms_geometry_types_sql = ",".join(f"'{g}'" for g in ms_geometry_types)

        result = f"({cls.quote(col_name)}).STGeometryType()"
        if len(ms_geometry_types) > 1:
            result += f" IN ({ms_geometry_types_sql})"
        else:
            result += f" = {ms_geometry_types_sql}"

        return result

    @classmethod
    def _geometry_crs_constraint(cls, col_name, crs_id):
        return f"({cls.quote(col_name)}).STSrid = {crs_id}"

    @classmethod
    def sqlserver_to_v2_schema(cls, ms_table_info, ms_crs_info, id_salt):
        """Generate a V2 schema from the given SQL server metadata."""
        return Schema(
            [
                cls._sqlserver_to_column_schema(col, ms_crs_info, id_salt)
                for col in ms_table_info
            ]
        )

    @classmethod
    def _sqlserver_to_column_schema(cls, ms_col_info, ms_crs_info, id_salt):
        """
        Given the MS column info for a particular column, converts it to a ColumnSchema.

        Parameters:
        ms_col_info - info about a single column from ms_table_info.
        id_salt - the UUIDs of the generated ColumnSchema are deterministic and depend on
                  the name and type of the column, and on this salt.
        """
        name = ms_col_info["column_name"]
        pk_index = ms_col_info["pk_ordinal_position"]
        if pk_index is not None:
            pk_index -= 1

        if ms_col_info["data_type"] in ("geometry", "geography"):
            data_type, extra_type_info = cls._ms_type_to_v2_geometry_type(
                ms_col_info, ms_crs_info
            )
        else:
            data_type, extra_type_info = cls._ms_type_to_v2_type(ms_col_info)

        col_id = ColumnSchema.deterministic_id(name, data_type, id_salt)
        return ColumnSchema(col_id, name, data_type, pk_index, **extra_type_info)

    @classmethod
    def _ms_type_to_v2_type(cls, ms_col_info):
        ms_type = ms_col_info["data_type"].upper()
        v2_type_info = cls.SQL_TYPE_TO_V2_TYPE.get(ms_type)

        if isinstance(v2_type_info, tuple):
            v2_type = v2_type_info[0]
            extra_type_info = {"size": v2_type_info[1]}
        else:
            v2_type = v2_type_info
            extra_type_info = {}

        if v2_type in ("text", "blob"):
            length = ms_col_info["character_maximum_length"] or None
            if length is not None and length > 0:
                extra_type_info["length"] = length

        if v2_type == "numeric":
            extra_type_info["precision"] = ms_col_info["numeric_precision"] or None
            extra_type_info["scale"] = ms_col_info["numeric_scale"] or None

        return v2_type, extra_type_info

    @classmethod
    def _ms_type_to_v2_geometry_type(cls, ms_col_info, ms_crs_info):
        extra_type_info = {"geometryType": "geometry"}

        crs_row = next(
            (r for r in ms_crs_info if r["column_name"] == ms_col_info["column_name"]),
            None,
        )
        if crs_row:
            auth_name = crs_row['authority_name']
            auth_code = crs_row['authorized_spatial_reference_id']
            if not auth_name and not auth_code:
                auth_name, auth_code = "CUSTOM", crs_row['srid']
            geometry_crs = f"{auth_name}:{auth_code}"
            extra_type_info["geometryCRS"] = geometry_crs

        return "geometry", extra_type_info

    @classmethod
    def _type_def_for_column_schema(cls, col, dataset):
        if col.data_type == "geometry":
            crs_name = col.extra_type_info.get("geometryCRS")
            crs_id = None
            if dataset is not None:
                crs_id = (
                    crs_util.get_identifier_int_from_dataset(dataset, crs_name) or 0
                )
            # This user-defined GeometryType adapts Kart's GPKG geometry to SQL Server's native geometry type.
            return GeometryType(crs_id)
        elif col.data_type in ("date", "time", "timestamp"):
            return BaseDateOrTimeType
        else:
            # Don't need to specify type information for other columns at present, since we just pass through the values.
            return None


class InstanceFunction(Function):
    """
    An instance function that compiles like this when applied to an element:
    >>> element.function()
    Unlike a normal sqlalchemy function which would compile as follows:
    >>> function(element)
    """


@compiles(InstanceFunction)
def compile_instance_function(element, compiler, **kw):
    return "(%s).%s()" % (element.clauses, element.name)


@aliased_converter_type
class GeometryType(ConverterType):
    """ConverterType so that V2 geometry is adapted to MS binary format."""

    def __init__(self, crs_id):
        self.crs_id = crs_id

    def python_prewrite(self, geom):
        # 1. Writing - Python layer - convert Kart geometry to WKB
        return geom.to_wkb() if geom else None

    def sql_write(self, bindvalue):
        # 2. Writing - SQL layer - wrap in call to STGeomFromWKB to convert WKB to MS binary.
        return Function(
            quoted_name("geometry::STGeomFromWKB", False),
            bindvalue,
            self.crs_id,
            type_=self,
        )

    def sql_read(self, column):
        # 3. Reading - SQL layer - append with call to .AsBinaryZM() to convert MS binary to WKB.
        return InstanceFunction("AsBinaryZM", column, type_=self)

    def python_postread(self, wkb):
        # 4. Reading - Python layer - convert WKB to Kart geometry.
        return Geometry.from_wkb(wkb)


@aliased_converter_type
class BaseDateOrTimeType(ConverterType):
    """
    ConverterType so we read dates, times, and datetimes as text.
    They are stored as date / time / datetime in SQL Server, but read back out as text.
    """

    def sql_read(self, column):
        # When reading, convert dates and times to strings using style 127: ISO8601 with time zone Z.
        # https://docs.microsoft.com/en-us/sql/t-sql/functions/cast-and-convert-transact-sql
        return Function(
            "CONVERT",
            literal_column("NVARCHAR"),
            column,
            literal_column("127"),
            type_=self,
        )
