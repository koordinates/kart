from datetime import datetime
import re

from kart import crs_util
from kart.schema import Schema, ColumnSchema
from kart.sqlalchemy.gpkg import Db_GPKG
from kart.sqlalchemy.adapter.base import BaseKartAdapter
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
        "timestamp": "DATETIME",
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
        "DATETIME": "timestamp",
        "GEOMETRY": "geometry",
    }

    # Types that can't be roundtripped perfectly in GPKG, and what they end up as.
    APPROXIMATED_TYPES = {"interval": "text", "time": "text", "numeric": "text"}

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
    def all_v2_meta_items(cls, sess, table_name, id_salt=None):
        """
        Generate all V2 meta items for the given table.
        Varying the id_salt varies the ids that are generated for the schema.json item.
        """
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
        yield "title", title
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
        return [
            cls._column_schema_to_gpkg(i, col, v2_obj.has_geometry)
            for i, col in enumerate(v2_obj.schema)
        ]

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
            "srs_id": crs_util.get_identifier_int_from_dataset(v2_obj),
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
    def v2_schema_to_sql_spec(cls, schema):
        """Generate a sqlite schema string from a dataset eg 'fid INTEGER, shape GEOMETRY'."""

        result = [
            cls.v2_column_schema_to_sql_spec(col, schema.has_geometry) for col in schema
        ]
        # GPKG requires an integer primary key for spatial tables, so we add it in if needed:
        if schema.has_geometry and not any("PRIMARY KEY" in c for c in result):
            result = ["auto_int_pk INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL"] + result

        return ",".join(result)

    @classmethod
    def v2_column_schema_to_sql_spec(cls, column_schema, has_geometry):
        gpkg_type = cls._v2_type_to_gpkg_type(column_schema, has_geometry)
        col_name = cls.quote(column_schema.name)
        result = f"{col_name} {gpkg_type}"

        is_pk = column_schema.pk_index is not None
        if is_pk:
            if gpkg_type == "INTEGER":
                result += " PRIMARY KEY AUTOINCREMENT NOT NULL"
            elif has_geometry:
                # GPKG feature-tables only allow integer PKs, so we demote this PK to a regular UNIQUE field.
                result += f" UNIQUE NOT NULL CHECK({col_name}<>'')"
            else:
                # Non-geometry tables are allowed non-integer primary keys
                result += " PRIMARY KEY NOT NULL"

        return result

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
            data_type, extra_type_info = cls._gpkg_geom_to_v2_type(gpkg_meta_items)
        else:
            data_type, extra_type_info = cls._gpkg_to_v2_type(sqlite_col_info["type"])

        pk_index = 0 if sqlite_col_info["pk"] == 1 else None
        col_id = ColumnSchema.deterministic_id(name, data_type, id_salt)
        return ColumnSchema(col_id, name, data_type, pk_index, **extra_type_info)

    @classmethod
    def _column_schema_to_gpkg(cls, cid, column_schema, has_geometry):
        is_pk = 1 if column_schema.pk_index is not None else 0
        not_null = is_pk
        gpkg_type = cls._v2_type_to_gpkg_type(column_schema, has_geometry)
        if gpkg_type != "INTEGER" and has_geometry:
            is_pk = 0  # GPKG features only allow integer PKs, so we demote this PK to a regular field.
        return {
            "cid": cid,
            "name": column_schema.name,
            "pk": is_pk,
            "type": gpkg_type,
            "notnull": not_null,
            "dflt_value": None,
        }

    @classmethod
    def _gpkg_to_v2_type(cls, gpkg_type):
        gpkg_type = gpkg_type.upper()

        """Convert a gpkg type to v2 schema type."""
        m = re.match(r"^(TEXT|BLOB)\(([0-9]+)\)$", gpkg_type)
        if m:
            return m.group(1).lower(), {"length": int(m.group(2))}
        v2_type_info = cls.SQL_TYPE_TO_V2_TYPE.get(gpkg_type)
        if v2_type_info is None:
            raise ValueError(f"Unrecognised GPKG type: {gpkg_type}")
        elif isinstance(v2_type_info, tuple):
            v2_type, size = v2_type_info
            extra_type_info = {"size": size}
        else:
            v2_type = v2_type_info
            extra_type_info = {}
        return v2_type, extra_type_info

    @classmethod
    def _gpkg_geom_to_v2_type(cls, gpkg_meta_items):
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
    def _v2_type_to_gpkg_type(cls, column_schema, has_geometry):
        """Convert a v2 schema type to a gpkg type."""
        if (
            has_geometry
            and column_schema.pk_index is not None
            and column_schema.data_type == "integer"
        ):
            return "INTEGER"  # Must be INTEGER, not MEDIUMINT etc.

        v2_type = column_schema.data_type
        extra_type_info = column_schema.extra_type_info
        if column_schema.data_type == "geometry":
            return extra_type_info.get("geometryType", "GEOMETRY").split(" ", 1)[0]

        gpkg_type_info = cls.V2_TYPE_TO_SQL_TYPE.get(v2_type)
        if gpkg_type_info is None:
            raise ValueError(f"Unrecognised data type: {v2_type}")

        if isinstance(gpkg_type_info, dict):
            return gpkg_type_info.get(extra_type_info.get("size", 0))

        gpkg_type = gpkg_type_info
        length = extra_type_info.get("length", None)
        return f"{gpkg_type}({length})" if length else gpkg_type

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
        Keep this in sync with OgrImportSource.gpkg_meta_items for other datasource types.
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
