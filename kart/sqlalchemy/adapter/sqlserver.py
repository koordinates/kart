import sqlalchemy as sa
from kart import crs_util
from kart.geometry import Geometry
from kart.sqlalchemy.adapter.base import (
    BaseKartAdapter,
    ConverterType,
    aliased_converter_type,
)
from kart.sqlalchemy.sqlserver import Db_SqlServer
from kart.schema import ColumnSchema, Schema
from kart.utils import ungenerator
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import quoted_name  # type: ignore[attr-defined]
from sqlalchemy.sql.functions import Function


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
        "timestamp": {"UTC": "DATETIMEOFFSET", None: "DATETIME2"},
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
        "SMALLDATETIME": ("timestamp", None),
        "DATETIME": ("timestamp", None),
        "DATETIME2": ("timestamp", None),
        "DATETIMEOFFSET": ("timestamp", "UTC"),
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
    def v2_column_schema_to_sql_spec(cls, col, v2_obj=None, has_int_pk=False):
        result = super().v2_column_schema_to_sql_spec(col, v2_obj)
        constraints = cls.get_sql_type_constraints(col, v2_obj)
        return f"{result} {constraints}" if constraints else result

    @classmethod
    def get_sql_type_constraints(cls, col, v2_obj=None):
        if col.data_type != "geometry":
            return None

        constraints = []

        geometry_type = col.get("geometryType")
        if geometry_type is not None:
            geometry_type = geometry_type.split(" ")[0].upper()
            if geometry_type != "GEOMETRY":
                constraints.append(
                    cls._geometry_type_constraint(col.name, geometry_type)
                )

        if v2_obj is not None:
            crs_name = col.get("geometryCRS")
            crs_id = crs_util.get_identifier_int_from_dataset(v2_obj, crs_name)
            if crs_id is not None:
                constraints.append(cls._geometry_crs_constraint(col.name, crs_id))

        return f"CHECK({' AND '.join(constraints)})" if constraints else None

    @classmethod
    def v2_type_to_sql_type(cls, col, v2_obj=None):
        sql_type = super().v2_type_to_sql_type(col, v2_obj)

        if sql_type in ("VARCHAR", "NVARCHAR", "VARBINARY"):
            length = col.get("length")
            return f"{sql_type}({length})" if length is not None else f"{sql_type}(max)"

        if sql_type == "NUMERIC":
            precision = col.get("precision")
            scale = col.get("scale")
            if precision is not None and scale is not None:
                return f"NUMERIC({precision},{scale})"
            elif precision is not None:
                return f"NUMERIC({precision})"
            else:
                return "NUMERIC"

        return sql_type

    @classmethod
    @ungenerator(dict)
    def all_v2_meta_items_including_empty(
        cls, sess, db_schema, table_name, id_salt=None
    ):
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

        primary_key_sql = """
            SELECT KCU.* FROM information_schema.key_column_usage KCU
            INNER JOIN information_schema.table_constraints TC
            ON KCU.constraint_schema = TC.constraint_schema
            AND KCU.constraint_name = TC.constraint_name
            WHERE TC.constraint_type = 'PRIMARY KEY'
        """

        table_info_sql = f"""
            SELECT
                C.column_name, C.ordinal_position, C.data_type,
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
        ms_table_info = list(r)

        geom_cols = [
            row["column_name"]
            for row in ms_table_info
            if row["data_type"] in ("geometry", "geography")
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

        has_ogr_spatial_ref_sys_identifier = bool(
            sess.scalar(
                """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema=:table_schema AND table_name='spatial_ref_sys';
                """,
                {"table_schema": db_schema},
            )
        )

        if has_ogr_spatial_ref_sys_identifier:
            ogr_spatial_ref_sys_identifier = cls.quote_table(
                db_schema=db_schema, table_name="spatial_ref_sys"
            )
            ogr_spatial_ref_sys = [
                sess.execute(
                    f"""
                    SELECT TOP 1 :column_name AS column_name, {cls.quote(g)}.STSrid AS srid, SRS.*
                    FROM {table_identifier}
                    LEFT OUTER JOIN {ogr_spatial_ref_sys_identifier} SRS
                    ON SRS.spatial_reference_id = {cls.quote(g)}.STSrid
                    WHERE {cls.quote(g)} IS NOT NULL;
                    """,
                    {"column_name": g},
                ).fetchone()
                for g in geom_cols
            ]
            ogr_spatial_ref_sys = list(
                filter(None, ms_spatial_ref_sys)
            )  # Remove nulls.
        else:
            ogr_spatial_ref_sys = []

        schema = KartAdapter_SqlServer.sqlserver_to_v2_schema(
            ms_table_info, ms_spatial_ref_sys, id_salt
        )
        yield "schema.json", schema

        for crs_info in ms_spatial_ref_sys:
            auth_name = crs_info["authority_name"]
            auth_code = crs_info["authorized_spatial_reference_id"]
            if not auth_name and not auth_code:
                auth_name, auth_code = "CUSTOM", crs_info["srid"]
            wkt = crs_info["well_known_text"] or ""
            yield (
                f"crs/{auth_name}:{auth_code}.wkt",
                crs_util.normalise_wkt(
                    crs_util.ensure_authority_specified(wkt, auth_name, auth_code)
                ),
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
        return ColumnSchema(
            id=col_id,
            name=name,
            data_type=data_type,
            pk_index=pk_index,
            **extra_type_info,
        )

    @classmethod
    def _ms_type_to_v2_type(cls, ms_col_info):
        sql_type = ms_col_info["data_type"].upper()
        v2_type, extra_type_info = super().sql_type_to_v2_type(sql_type)

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
        if crs_row and crs_row["srid"]:
            auth_name = crs_row["authority_name"]
            auth_code = crs_row["authorized_spatial_reference_id"]
            if not auth_name and not auth_code:
                auth_name, auth_code = "CUSTOM", crs_row["srid"]
            geometry_crs = f"{auth_name}:{auth_code}"
            extra_type_info["geometryCRS"] = geometry_crs

        return "geometry", extra_type_info

    @classmethod
    def _type_def_for_column_schema(cls, col, dataset):
        if col.data_type == "geometry":
            crs_name = col.get("geometryCRS")
            crs_id = None
            if dataset is not None:
                crs_id = (
                    crs_util.get_identifier_int_from_dataset(dataset, crs_name) or 0
                )
            # This user-defined GeometryType adapts Kart's GPKG geometry to SQL Server's native geometry type.
            return GeometryType(crs_id)
        elif col.data_type == "date":
            return DateType
        elif col.data_type == "numeric":
            return TextType
        elif col.data_type == "time":
            return TimeType
        elif col.data_type == "timestamp":
            return TimestampType(col.get("timezone"))
        elif col.data_type == "text":
            return TextType
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

    EMPTY_POINT_WKB = "0x0101000000000000000000f87f000000000000f87f"

    def __init__(self, crs_id):
        self.crs_id = crs_id

    def python_prewrite(self, geom):
        # 1. Writing - Python layer - convert Kart geometry to WKB
        return geom.to_wkb() if geom else None

    def sql_write(self, bindvalue):
        # 2. Writing - SQL layer - wrap in call to STGeomFromWKB to convert WKB to MS binary.
        # POINT EMPTY is handled specially since it doesn't have a WKB value the SQL Server accepts.
        return sa.case(
            (
                sa.cast(bindvalue, sa.VARBINARY)
                == sa.literal_column(self.EMPTY_POINT_WKB),
                Function(
                    quoted_name("geometry::STGeomFromText", False),
                    "POINT EMPTY",
                    self.crs_id,
                    type_=self,
                ),
            ),
            else_=Function(
                quoted_name("geometry::STGeomFromWKB", False),
                bindvalue,
                self.crs_id,
                type_=self,
            ),
        )

    def sql_read(self, column):
        # 3. Reading - SQL layer - append with call to .AsBinaryZM() to convert MS binary to WKB.
        # POINT EMPTY is handled specially since SQL Server returns WKB(MULTIPOINT EMPTY) for (POINT EMPTY).AsBinaryZM()
        return sa.case(
            (
                sa.and_(
                    InstanceFunction("STGeometryType", column) == "Point",
                    InstanceFunction("STIsEmpty", column) == 1,
                ),
                sa.literal_column(self.EMPTY_POINT_WKB, type_=self),
            ),
            else_=InstanceFunction("AsBinaryZM", column, type_=self),
        )

    def python_postread(self, wkb):
        # 4. Reading - Python layer - convert WKB to Kart geometry.
        return Geometry.from_wkb(wkb)


@aliased_converter_type
class DateType(ConverterType):
    # ConverterType to read dates as text. They are stored in MS as DATE but we read them back as text.
    def sql_read(self, column):
        return Function("FORMAT", column, "yyyy-MM-dd", type_=self)


@aliased_converter_type
class TimeType(ConverterType):
    # ConverterType to read times as text. They are stored in MS as TIME but we read them back as text.
    def sql_read(self, column):
        return Function("FORMAT", column, r"hh\:mm\:ss", type_=self)


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
        return Function("FORMAT", column, "yyyy-MM-ddTHH:mm:ss", type_=self)


@aliased_converter_type
class TextType(ConverterType):
    """
    ConverterType to that casts everything to text in the Python layer. Handles NUMERICs (which Kart stores as text),
    sometimes rarer things like UUIDs.
    """

    def python_postread(self, value):
        return str(value) if value is not None else None
