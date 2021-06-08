from datetime import datetime
import re


from kart import crs_util
from kart.geometry import normalise_gpkg_geom
from kart.schema import Schema, ColumnSchema
from kart.sqlalchemy.gpkg import Db_GPKG
from kart.sqlalchemy.adapter.base import (
    BaseKartAdapter,
    ConverterType,
    aliased_converter_type,
)
from kart.timestamps import datetime_to_iso8601_utc


class KartAdapter_GPKG(BaseKartAdapter, Db_GPKG):
    """
    Adapts a table in a GPKG (or a table in a GPKG plus metadata rows stored in the gpkg_X tables) to a V2 dataset.
    Or, does the reverse - adapts a V2 dataset to a GPKG table (plus metadata rows in gpkg_X tables).

    Also used for upgrading from V0 and V1 datasets, which stored data in a JSON-serialised GPKG format internally.
    For this reason, has a larger public interface than the other adapters (and because GPKG supports more metadata
    than the others).
    """

    # From http://www.geopackage.org/spec/
    # The columns of tables in a GeoPackage SHALL only be declared using one of the following data types:
    # BOOLEAN, TINYINT, SMALLINT, MEDIUMINT, INT / INTEGER, FLOAT, DOUBLE / REAL,
    # TEXT{(max_len)}, BLOB{(max_len)}, DATE, DATETIME, <geometry_type_name>

    V2_TYPE_TO_SQL_TYPE = {
        "boolean": "BOOLEAN",
        "integer": {
            0: "INTEGER",
            8: "TINYINT",
            16: "SMALLINT",
            32: "MEDIUMINT",
            64: "INTEGER",
        },
        "float": {0: "REAL", 32: "FLOAT", 64: "REAL"},
        "text": "TEXT",
        "blob": "BLOB",
        "date": "DATE",
        "timestamp": {
            "UTC": "DATETIME",
            None: "TEXT",  # Null-timezone timestamp is approximated
        },
        "time": "TEXT",  # Approximated
        "numeric": "TEXT",  # Approximated
        "interval": "TEXT",  # Approximated
        "geometry": "GEOMETRY",
    }

    SQL_TYPE_TO_V2_TYPE = {
        "BOOLEAN": "boolean",
        "TINYINT": ("integer", 8),
        "SMALLINT": ("integer", 16),
        "MEDIUMINT": ("integer", 32),
        "INT": ("integer", 64),
        "INTEGER": ("integer", 64),
        "FLOAT": ("float", 32),
        "DOUBLE": ("float", 64),
        "REAL": ("float", 64),
        "TEXT": "text",
        "BLOB": "blob",
        "DATE": "date",
        "DATETIME": ("timestamp", "UTC"),
        "GEOMETRY": "geometry",
    }

    # Types that can't be roundtripped perfectly in GPKG, and what they end up as.
    APPROXIMATED_TYPES = {
        "interval": "text",
        "time": "text",
        "numeric": "text",
        ("timestamp", None): "text",
    }

    # Extra type info that might be missing/extra due to an approximated type.
    APPROXIMATED_TYPES_EXTRA_TYPE_INFO = ("length", "precision", "scale")

    GPKG_META_ITEM_NAMES = (
        "sqlite_table_info",
        "gpkg_contents",
        "gpkg_geometry_columns",
        "gpkg_spatial_ref_sys",
        "gpkg_metadata",
        "gpkg_metadata_reference",
    )

    @classmethod
    def v2_schema_to_sql_spec(cls, schema, v2_obj=None):
        columns = schema.columns
        if cls._is_conformant_gpkg_pk_column(columns[0]):
            pk_name = columns[0].name
            columns = columns[1:]
        else:
            pk_name = "auto_int_pk"

        # GPKG requires an integer primary key:
        first_col = f"{cls.quote(pk_name)} INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL"
        other_cols = [cls.v2_column_schema_to_sql_spec(col, v2_obj) for col in columns]

        return ", ".join([first_col] + other_cols)

    @classmethod
    def _is_conformant_gpkg_pk_column(cls, first_col):
        return first_col.pk_index == 0 and first_col.data_type == "integer"

    @classmethod
    def v2_column_schema_to_sql_spec(cls, col, v2_obj=None):
        col_name = cls.quote(col.name)
        sql_type = cls.v2_type_to_sql_type(col, v2_obj)
        result = f"{col_name} {sql_type}"

        if col.pk_index is not None:
            # GPKG conformant primary keys are handled by v2_schema_to_sql_spec.
            # This column is not conformant so we demote it to just UNIQUE NOT NULL.
            result += f" UNIQUE NOT NULL CHECK({col_name}<>'')"

        return result

    @classmethod
    def v2_type_to_sql_type(cls, col, v2_obj=None):
        sql_type = super().v2_type_to_sql_type(col, v2_obj)

        extra_type_info = col.extra_type_info
        if sql_type == "GEOMETRY":
            # Return the geometryType, minus the Z or M specifiers.
            return extra_type_info.get("geometryType", "GEOMETRY").split(" ", 1)[0]

        if sql_type in ("TEXT", "BLOB"):
            # Add length specifier if present.
            length = extra_type_info.get("length", None)
            return f"{sql_type}({length})" if length else sql_type

        return sql_type

    @classmethod
    def all_gpkg_meta_items(cls, v2_obj, table_name):
        """Generate all the gpkg_meta_items from the given v2 object (eg dataset)."""
        yield "sqlite_table_info", cls.generate_sqlite_table_info(v2_obj)
        yield "gpkg_contents", cls.generate_gpkg_contents(v2_obj, table_name)
        yield "gpkg_geometry_columns", cls.generate_gpkg_geometry_columns(
            v2_obj, table_name
        )
        yield "gpkg_spatial_ref_sys", cls.generate_gpkg_spatial_ref_sys(v2_obj)
        yield "gpkg_metadata", cls.generate_gpkg_metadata(
            v2_obj, table_name, reference=False
        )
        yield "gpkg_metadata_reference", cls.generate_gpkg_metadata(
            v2_obj, table_name, reference=True
        )

    @classmethod
    def all_v2_meta_items(cls, sess, db_schema, table_name, id_salt):
        """
        Generate all V2 meta items for the given table.
        Varying the id_salt varies the ids that are generated for the schema.json item.
        """
        assert not db_schema

        gpkg_meta_items = dict(cls._gpkg_meta_items_from_db(sess, table_name))
        yield from cls.all_v2_meta_items_from_gpkg_meta_items(gpkg_meta_items, id_salt)

    @classmethod
    def all_v2_meta_items_from_gpkg_meta_items(cls, gpkg_meta_items, id_salt=None):
        """
        Generate all the V2 meta items from the given gpkg_meta_items lists / dicts -
        either loaded from JSON, or generated directly from the database.
        Varying the id_salt varies the ids that are generated for the schema.json item.
        """

        title = cls._nested_get(gpkg_meta_items, "gpkg_contents", "identifier")
        description = cls._nested_get(gpkg_meta_items, "gpkg_contents", "description")
        if title:
            yield "title", title
        if description:
            yield "description", description

        id_salt = id_salt or cls._nested_get(
            gpkg_meta_items, "gpkg_contents", "table_name"
        )
        schema = cls._gpkg_to_v2_schema(gpkg_meta_items, id_salt)
        yield "schema.json", schema.to_column_dicts() if schema else None

        yield "metadata/dataset.json", cls.gpkg_to_json_metadata(gpkg_meta_items)
        yield "metadata.xml", cls.gpkg_to_xml_metadata(gpkg_meta_items)

        gpkg_spatial_ref_sys = gpkg_meta_items.get("gpkg_spatial_ref_sys")
        for gsrs in gpkg_spatial_ref_sys:
            d = gsrs["definition"]
            if not d or d == "undefined":
                continue
            id_str = crs_util.get_identifier_str(d)
            yield f"crs/{id_str}.wkt", crs_util.normalise_wkt(d)

    @classmethod
    def generate_sqlite_table_info(cls, v2_obj):
        """Generate a sqlite_table_info meta item from a dataset."""
        columns = v2_obj.schema.columns
        if cls._is_conformant_gpkg_pk_column(columns[0]):
            pk_name = columns[0].name
            columns = columns[1:]
        else:
            pk_name = "auto_int_pk"

        first_col = {
            "cid": 0,
            "name": pk_name,
            "pk": 1,
            "type": "INTEGER",
            "notnull": 1,
            "dflt_value": None,
        }
        other_cols = [
            cls._column_schema_to_gpkg(i + 1, col) for i, col in enumerate(columns)
        ]

        return [first_col] + other_cols

    @classmethod
    def _column_schema_to_gpkg(cls, cid, column_schema):
        sql_type = cls.v2_type_to_sql_type(column_schema)
        not_null = 1 if column_schema.pk_index is not None else 0
        return {
            "cid": cid,
            "name": column_schema.name,
            "pk": 0,  # GPKG Conformant primary keys are handled by generate_sqlite_table_info.
            "type": sql_type,
            "notnull": not_null,
            "dflt_value": None,
        }

    @classmethod
    def generate_gpkg_contents(cls, v2_obj, table_name):
        """Generate a gpkg_contents meta item from a v2 dataset."""
        result = {
            "identifier": v2_obj.get_meta_item("title") or "",
            "description": v2_obj.get_meta_item("description"),
            "table_name": table_name,
            "data_type": "features" if v2_obj.has_geometry else "attributes",
        }
        if v2_obj.has_geometry:
            result["srs_id"] = crs_util.get_identifier_int_from_dataset(v2_obj)
        return result

    @classmethod
    def generate_gpkg_geometry_columns(cls, v2_obj, table_name):
        """Generate a gpkg_geometry_columns meta item from a v2 dataset."""
        geom_columns = v2_obj.schema.geometry_columns
        if not geom_columns:
            return None

        geometry_type = geom_columns[0].extra_type_info.get("geometryType", "GEOMETRY")
        type_name, *zm = geometry_type.split(" ", 1)
        zm = zm[0] if zm else ""
        z = 1 if "Z" in zm else 0
        m = 1 if "M" in zm else 0

        return {
            "table_name": table_name,
            "column_name": geom_columns[0].name,
            "geometry_type_name": type_name,
            "srs_id": crs_util.get_identifier_int_from_dataset(v2_obj) or 0,
            "z": z,
            "m": m,
        }

    @classmethod
    def generate_gpkg_spatial_ref_sys(cls, v2_obj):
        """Generate a gpkg_spatial_ref_sys meta item from a v2 dataset."""
        geom_columns = v2_obj.schema.geometry_columns
        if not geom_columns:
            return []

        crs_pathname = geom_columns[0].extra_type_info.get("geometryCRS")
        if not crs_pathname:
            return []
        wkt = v2_obj.get_crs_definition(crs_pathname)
        auth_name, auth_code = crs_util.parse_authority(wkt)
        if auth_code and auth_code.isdigit() and int(auth_code) > 0:
            srs_id = int(auth_code)
        else:
            srs_id = crs_util.get_identifier_int(wkt)
        return [
            {
                "srs_name": crs_util.parse_name(wkt),
                "definition": wkt,
                "organization": auth_name or "NONE",
                "srs_id": srs_id,
                "organization_coordsys_id": srs_id,
                "description": None,
            }
        ]

    @classmethod
    def generate_gpkg_metadata(cls, v2_obj, table_name, reference=False):
        metadata_xml = v2_obj.get_meta_item("metadata.xml")
        if metadata_xml is not None:
            return cls.xml_to_gpkg_metadata(metadata_xml, table_name, reference)
        v2json = v2_obj.get_meta_item("metadata/dataset.json")
        if v2json is not None:
            return cls.json_to_gpkg_metadata(v2json, table_name, reference)
        return None

    @classmethod
    def _gpkg_to_v2_schema(cls, gpkg_meta_items, id_salt):
        """Generate a v2 Schema from the given gpkg meta items."""
        sqlite_table_info = gpkg_meta_items.get("sqlite_table_info")
        if not sqlite_table_info:
            return None

        def _sort_by_cid(sqlite_col_info):
            return sqlite_col_info["cid"]

        return Schema(
            [
                cls._gpkg_to_column_schema(col, gpkg_meta_items, id_salt)
                for col in sorted(sqlite_table_info, key=_sort_by_cid)
            ]
        )

    @classmethod
    def _gpkg_to_column_schema(cls, sqlite_col_info, gpkg_meta_items, id_salt):
        """
        Given the sqlite_table_info for a particular column, and some extra context about the
        geometry column, converts it to a ColumnSchema. The extra info will only be used if the
        given sqlite_col_info is the geometry column.
        Parameters:
        sqlite_col_info - a single column from sqlite_table_info.
        gpkg_geometry_columns - meta item about the geometry column, if it exists.
        gpkg_spatial_ref_sys - meta item about the spatial reference system, if it exists.
        id_salt - the UUIDs of the generated ColumnSchema are deterministic and depend on
        the name and type of the column, and on this salt.
        """
        name = sqlite_col_info["name"]

        geom_name = cls._nested_get(
            gpkg_meta_items, "gpkg_geometry_columns", "column_name"
        )
        if name == geom_name:
            data_type, extra_type_info = cls._sql_type_to_v2_geometry_type(
                gpkg_meta_items
            )
        else:
            sql_type = sqlite_col_info["type"]
            data_type, extra_type_info = cls.sql_type_to_v2_type(sql_type)

        pk_index = 0 if sqlite_col_info["pk"] == 1 else None
        col_id = ColumnSchema.deterministic_id(name, data_type, id_salt)
        return ColumnSchema(col_id, name, data_type, pk_index, **extra_type_info)

    @classmethod
    def sql_type_to_v2_type(cls, sql_type):
        sql_type = sql_type.upper()
        m = re.match(r"^(TEXT|BLOB)\(([0-9]+)\)$", sql_type)
        if m:
            return m.group(1).lower(), {"length": int(m.group(2))}

        return super().sql_type_to_v2_type(sql_type)

    @classmethod
    def _sql_type_to_v2_geometry_type(cls, gpkg_meta_items):
        # There's only one geometry column so no need to determine which column.
        gpkg_geometry_columns = gpkg_meta_items["gpkg_geometry_columns"]
        gpkg_spatial_ref_sys = gpkg_meta_items.get("gpkg_spatial_ref_sys")

        geometry_type = gpkg_geometry_columns["geometry_type_name"]
        z = "Z" if gpkg_geometry_columns["z"] else ""
        m = "M" if gpkg_geometry_columns["m"] else ""

        extra_type_info = {
            "geometryType": f"{geometry_type} {z}{m}".strip(),
        }

        wkt = None
        if gpkg_spatial_ref_sys:
            wkt = gpkg_spatial_ref_sys[0].get("definition")
        if wkt and wkt != "undefined":
            extra_type_info["geometryCRS"] = crs_util.get_identifier_str(wkt)

        return "geometry", extra_type_info

    @classmethod
    def json_to_gpkg_metadata(cls, v2_metadata_json, table_name, reference=False):
        """Generates either the gpkg_metadata or gpkg_metadata_reference tables from the given metadata."""
        result = []
        timestamp = datetime_to_iso8601_utc(datetime.now())
        md_file_id = 1

        for uri, uri_metadata in sorted(v2_metadata_json.items()):
            for mime_type, content in sorted(uri_metadata.items()):
                if reference:
                    row = {
                        "reference_scope": "table",
                        "table_name": table_name,
                        "column_name": None,
                        "row_id_value": None,
                        "timestamp": timestamp,
                        "md_file_id": md_file_id,
                        "md_parent_id": None,
                    }
                else:
                    row = {
                        "id": md_file_id,
                        "md_scope": "dataset",
                        "md_standard_uri": uri,
                        "mime_type": mime_type,
                        "metadata": content,
                    }

                result.append(row)
                md_file_id += 1

        return result

    _KNOWN_METADATA_URIS = {"GDALMultiDomainMetadata": "http://gdal.org"}

    @classmethod
    def _get_uri_from_xml(cls, xml_metadata):
        from xml.dom.minidom import parseString

        try:
            doc = parseString(xml_metadata)
            element = doc.documentElement
            return (
                cls._KNOWN_METADATA_URIS.get(element.tagName)
                or element.getAttribute("xmlns")
                or element.namespaceURI
                or "(unknown)"
            )
        except Exception:
            return "(unknown)"

    @classmethod
    def xml_to_gpkg_metadata(cls, metadata_xml, table_name, reference=False):
        if reference:
            timestamp = datetime_to_iso8601_utc(datetime.now())
            return [
                {
                    "reference_scope": "table",
                    "table_name": table_name,
                    "column_name": None,
                    "row_id_value": None,
                    "timestamp": timestamp,
                    "md_file_id": 1,
                    "md_parent_id": None,
                }
            ]
        else:
            return [
                {
                    "id": 1,
                    "md_scope": "dataset",
                    "md_standard_uri": cls._get_uri_from_xml(metadata_xml),
                    "mime_type": "text/xml",
                    "metadata": metadata_xml,
                }
            ]

    @classmethod
    def _join_gpkg_metadata(cls, gpkg_meta_items):
        gpkg_metadata = gpkg_meta_items.get("gpkg_metadata")
        gpkg_metadata_reference = gpkg_meta_items.get("gpkg_metadata_reference")
        if not gpkg_metadata or not gpkg_metadata_reference:
            return None

        id_to_gm = {gm["id"]: gm for gm in gpkg_metadata}
        id_to_gmr = {gmr["md_file_id"]: gmr for gmr in gpkg_metadata_reference}
        return [
            {**id_to_gm[i], **id_to_gmr[i]} for i in id_to_gm.keys() & id_to_gmr.keys()
        ]

    @classmethod
    def gpkg_to_json_metadata(cls, gpkg_meta_items):
        # Note that the rows fetched are only those that match the pattern we care about:
        # Table-scoped XML metadata (see query in gpkg_meta_items_from_db).
        joined_rows = cls._join_gpkg_metadata(gpkg_meta_items)
        if not joined_rows:
            return None

        return {
            row["md_standard_uri"]: {"text/xml": row["metadata"]} for row in joined_rows
        }

    @classmethod
    def gpkg_to_xml_metadata(cls, gpkg_meta_items):
        # Note that the rows fetched are only those that match the pattern we care about:
        # Table-scoped XML metadata (see query in gpkg_meta_items_from_db).
        joined_rows = cls._join_gpkg_metadata(gpkg_meta_items)
        if not joined_rows:
            return None

        xml_list = [row["metadata"] for row in joined_rows]
        if not xml_list:
            return None
        if len(xml_list) == 1:
            return xml_list[0]

        # We can't actually commit a whole list of XML, but we need to return something that makes sense.
        # Simply throwing an error here stops dirty-detection working, and stops commands that would fix the situation
        # from working, like `kart reset --discard-changes` or `kart create-workingcopy --discard-changes`.
        return xml_list

    METADATA_QUERY = """
        SELECT {select}
        FROM gpkg_metadata_reference MR
            INNER JOIN gpkg_metadata M ON (MR.md_file_id = M.id)
        WHERE
            M.md_scope='dataset'
            AND M.mime_type='text/xml'
            AND MR.table_name=:table_name
            AND MR.column_name IS NULL
            AND MR.row_id_value IS NULL;
        """

    @classmethod
    def _gpkg_meta_items_from_db(cls, sess, table_name, keys=None):
        """
        Returns metadata from the gpkg_* tables about this GPKG.
        """

        QUERIES = {
            "sqlite_table_info": (f"PRAGMA table_info({cls.quote(table_name)});", list),
            "gpkg_contents": (
                # we ignore dynamic fields (last-change, min_x, min_y, max_x, max_y)
                """
                SELECT table_name, data_type, identifier, description, srs_id
                FROM gpkg_contents WHERE table_name=:table_name;
                """,
                dict,
            ),
            "gpkg_geometry_columns": (
                """
                SELECT table_name, column_name, geometry_type_name, srs_id, z, m
                FROM gpkg_geometry_columns WHERE table_name=:table_name;
                """,
                dict,
            ),
            "gpkg_metadata": (
                cls.METADATA_QUERY.format(select="M.*"),
                list,
            ),
            "gpkg_metadata_reference": (
                cls.METADATA_QUERY.format(select="MR.*"),
                list,
            ),
            "gpkg_spatial_ref_sys": (
                """
                SELECT DISTINCT SRS.*
                FROM gpkg_spatial_ref_sys SRS
                    LEFT OUTER JOIN gpkg_contents C ON (C.srs_id = SRS.srs_id)
                    LEFT OUTER JOIN gpkg_geometry_columns G ON (G.srs_id = SRS.srs_id)
                WHERE
                    (C.table_name=:table_name OR G.table_name=:table_name)
                """,
                list,
            ),
        }
        for key, (sql, rtype) in QUERIES.items():
            if keys is not None and key not in keys:
                continue
            # check table exists, the metadata ones may not
            if not key.startswith("sqlite_"):
                r = sess.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=:name;",
                    {"name": key},
                )
                if not r.fetchone():
                    continue

            r = sess.execute(sql, {"table_name": table_name})
            value = [dict(sorted(zip(row.keys(), row))) for row in r]
            if rtype is dict:
                value = value[0] if len(value) else None
            yield (key, value)

    @classmethod
    def _nested_get(cls, nested_dict, *keys):
        result = nested_dict
        for key in keys:
            result = result.get(key)
            if result is None:
                return result
        return result

    @classmethod
    def _type_def_for_column_schema(self, col, dataset=None):
        if col.data_type == "geometry":
            # This user-defined GeometryType normalises GPKG geometry to the Kart V2 GPKG geometry.
            return GeometryType
        elif col.data_type == "boolean":
            # Read BOOLEANs as bools, not ints.
            return BooleanType
        elif col.data_type == "timestamp":
            # Add and strip Z suffix from timestamps:
            return TimestampType(col.extra_type_info.get("timezone"))
        # Don't need to specify type information for other columns at present, since we just pass through the values.
        return None


@aliased_converter_type
class GeometryType(ConverterType):
    """ConverterType so that GPKG geometry is normalised to V2 format."""

    def python_postread(self, geom):
        # We normalise geometries to avoid spurious diffs - diffs where nothing
        # of any consequence has changed (eg, only endianness has changed).
        # This includes setting the SRID to zero for each geometry so that we don't store a separate SRID per geometry,
        # but only one per column at most.

        # Its possible in GPKG to put arbitrary values in columns, regardless of type.
        # We don't try to convert them here - we let the commit validation step report this as an error.
        return normalise_gpkg_geom(geom) if isinstance(geom, bytes) else geom


@aliased_converter_type
class BooleanType(ConverterType):
    """ConverterType so that BOOLEANs are read as bools and not ints."""

    def python_postread(self, value):
        # Its possible in GPKG to put arbitrary values in columns, regardless of type.
        # We don't try to convert them here - we let the commit validation step report this as an error.
        return bool(value) if value in (0, 1) else value


@aliased_converter_type
class TimestampType(ConverterType):
    """
    ConverterType so that the Z timezone suffix is added in when written (for UTC timestamps) and stripped on read.
    In Kart, the timezone information is only stored at the column level, not on the value itself.
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

    def python_postread(self, timestamp):
        # Its possible in GPKG to put arbitrary values in columns, regardless of type.
        # We don't try to convert them here - we let the commit validation step report this as an error.
        return timestamp.rstrip("Z") if isinstance(timestamp, str) else timestamp
