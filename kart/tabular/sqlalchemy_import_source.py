import functools
import os
import sys

import click

import sqlalchemy
from kart.exceptions import NO_IMPORT_SOURCE, NO_TABLE, NotFound, NotYetImplemented
from kart.output_util import dump_json_output
from kart.list_of_conflicts import ListOfConflicts
from kart.schema import Schema
from kart.sqlalchemy import DbType, separate_last_path_part, strip_username_and_password
from kart.utils import chunk, ungenerator
from kart.serialise_util import ensure_bytes
from sqlalchemy.orm import sessionmaker

from .import_source import TableImportSource


class SqlAlchemyTableImportSource(TableImportSource):
    """
    TableImportSource that uses SqlAlchemy directly to import into Kart.
    Supports GPKG, Postgres (+ PostGIS), SQL Server, MySQL.
    """

    CURSOR_SIZE = 10000

    @classmethod
    def open(cls, spec, table=None):
        db_type = DbType.from_spec(spec)
        if db_type is None:
            raise cls._bad_import_source_spec(spec)

        if db_type.clearly_doesnt_exist(spec):
            raise NotFound(f"Couldn't find '{spec}'", exit_code=NO_IMPORT_SOURCE)

        path_length = db_type.path_length(spec)
        longest_allowed_path_length = (
            db_type.path_length_for_table
            if not table
            else db_type.path_length_for_table_container
        )
        shortest_allowed_path_length = max(
            db_type.path_length_for_table_container - 1, 0
        )

        if not (
            shortest_allowed_path_length <= path_length <= longest_allowed_path_length
        ):
            raise cls._bad_import_source_spec(spec)

        connect_url = spec
        db_schema = None

        # Handle the case where specification already points to a single table.
        if path_length == db_type.path_length_for_table:
            connect_url, table = separate_last_path_part(connect_url)
            path_length -= 1

        # Handle the case where specification points to a database schema (or similar).
        if path_length > shortest_allowed_path_length:
            connect_url, db_schema = separate_last_path_part(connect_url)

        engine = db_type.class_.create_engine(connect_url)
        return SqlAlchemyTableImportSource(
            spec, db_type=db_type, engine=engine, db_schema=db_schema, table=table
        )

    @classmethod
    def _bad_import_source_spec(self, spec):
        return click.UsageError(
            f"Unrecognised import-source specification: {spec}\n"
            "Try one of:\n"
            "  PATH.gpkg\n"
            "  postgresql://HOST/DBNAME[/DBSCHEMA[/TABLE]]\n"
            "  mssql://HOST/DBNAME[/DBSCHEMA[/TABLE]]\n"
            "  mysql://HOST[/DBNAME[/TABLE]]"
        )

    def __init__(
        self,
        original_spec,
        *,
        db_type,
        engine,
        db_schema,
        table,
        dest_path=None,
        meta_overrides=None,
    ):
        self.original_spec = original_spec
        self.db_type = db_type
        self.db_class = db_type.class_
        self.engine = engine

        self.db_schema = db_schema
        self.table = table
        if dest_path:
            self.dest_path = dest_path
        self.meta_overrides = {
            k: v for k, v in (meta_overrides or {}).items() if v is not None
        }

    @property
    def source_name(self):
        """Returns the container the user specified to find the table or table(s) inside."""
        if self.db_type == DbType.GPKG:
            return os.path.basename(self.original_spec)
        else:
            return strip_username_and_password(self.original_spec)

    @property
    def table_location_within_source(self):
        if self.db_type == DbType.GPKG:
            return self.table

        path_length = self.db_type.path_length(self.original_spec)
        if path_length == self.db_type.path_length_for_table:
            return ""
        elif path_length == self.db_type.path_length_for_table_container:
            return self.table
        else:
            return f"{self.db_schema}/{self.table}"

    @property
    def fully_qualified_table_location(self):
        table_loc = self.table_location_within_source
        if not table_loc:
            return self.source_name

        separator = ":" if self.db_type is DbType.GPKG else "/"
        return f"{self.source_name}{separator}{table_loc}"

    def __str__(self):
        return self.fully_qualified_table_location

    def import_source_desc(self):
        return f"Import from {self.fully_qualified_table_location} to {self.dest_path}/"

    def aggregate_import_source_desc(self, import_sources):
        if len(import_sources) == 1:
            return next(iter(import_sources)).import_source_desc()

        desc = f"Import {len(import_sources)} datasets from {self.source_name}:"
        for source in import_sources:
            if source.dest_path == source.table_location_within_source:
                desc += f"\n * {source.table_location_within_source}/"
            else:
                desc += f"\n * {source.dest_path} (from {source.table_location_within_source})"
        return desc

    def default_dest_path(self):
        return self._normalise_dataset_path(self.table)

    @functools.lru_cache(maxsize=1)
    def get_tables(self):
        with self.engine.connect() as conn:
            tables = self.db_class.list_tables(conn, self.db_schema)

        if self.table is not None:
            return {self.table: tables.get(self.table)}
        else:
            return tables

    def print_table_list(self, do_json=False):
        tables = self.get_tables()
        if do_json:
            dump_json_output({"kart.tables/v1": tables}, sys.stdout)
        else:
            click.secho("Tables found:", bold=True)
            for table_name, title in tables.items():
                if title:
                    click.echo(f"  {table_name} - {title}")
                else:
                    click.echo(f"  {table_name}")
        return tables

    def validate_table(self, table):
        """
        Find the db-schema and the table, given a table name that the user supplied.
        The table-name might be in the format "DBSCHEMA.TABLE" or it might just be the table name.
        OGR can find the table even if the db_schema is not specified, at least in certain circumstances,
        so we try to do that too.
        """

        all_tables = self.get_tables().keys()
        if table in all_tables:
            if (
                self.db_schema is None
                and "." in table
                and self.db_type is not DbType.GPKG
            ):
                db_schema, table = table.split(".", maxsplit=1)
                return db_schema, table
            else:
                return self.db_schema, table

        if self.db_schema is None and self.db_type is not DbType.GPKG:
            with self.engine.connect() as conn:
                db_schemas = self.db_class.db_schema_searchpath(conn)
            for db_schema in db_schemas:
                if f"{db_schema}.{table}" in all_tables:
                    return db_schema, table

        raise NotFound(
            f"Table '{table}' not found",
            exit_code=NO_TABLE,
        )

    def clone_for_table(
        self, table, *, dest_path=None, primary_key=None, meta_overrides={}
    ):
        meta_overrides = {**self.meta_overrides, **meta_overrides}
        db_schema, table = self.validate_table(table)

        result = SqlAlchemyTableImportSource(
            self.original_spec,
            db_type=self.db_type,
            engine=self.engine,
            db_schema=db_schema,
            table=table,
            dest_path=dest_path,
            meta_overrides=meta_overrides,
        )

        if primary_key is not None:
            result.override_primary_key(primary_key)
        return result

    def meta_items(self):
        return {**self.meta_items_from_db(), **self.meta_overrides}

    @functools.lru_cache(maxsize=1)
    def meta_items_from_db(self):
        id_salt = f"{self.engine.url} {self.db_schema} {self.table}"

        with sessionmaker(bind=self.engine)() as sess:
            return self.db_type.adapter.all_v2_meta_items(
                sess, self.db_schema, self.table, id_salt
            )

    def attachments(self):
        with sessionmaker(bind=self.engine)() as sess:
            metadata_xml = self.db_type.adapter.get_metadata_xml(
                sess, self.db_schema, self.table
            )

        if metadata_xml and not isinstance(metadata_xml, ListOfConflicts):
            yield "metadata.xml", ensure_bytes(metadata_xml)

    def align_schema_to_existing_schema(self, existing_schema):
        aligned_schema = existing_schema.align_to_self(self.schema)
        self.meta_overrides["schema.json"] = aligned_schema.to_column_dicts()
        assert self.schema == aligned_schema

    def override_primary_key(self, new_primary_key):
        """Modify the schema such that the given column is the primary key."""

        def _modify_col(col):
            pk_index = 0 if col["name"] == new_primary_key else None
            return {**col, **{"primaryKeyIndex": pk_index}}

        old_schema = self.get_meta_item("schema.json")
        new_schema = [_modify_col(c) for c in old_schema]
        self.meta_overrides["schema.json"] = new_schema

        if not self.schema.pk_columns:
            raise click.UsageError(
                f"Cannot use column '{new_primary_key}' as primary key - column not found"
            )
        assert self.schema.pk_columns[0].name == new_primary_key

    @functools.lru_cache(maxsize=1)
    @ungenerator(dict)
    def crs_definitions(self):
        for key, value in self.meta_items().items():
            if key.startswith("crs/") and key.endswith(".wkt"):
                yield key[4:-4], value

    def quote(self, ident):
        """Conditionally quote an identifier - eg if it is a reserved word or contains special characters."""
        return self.db_class.quote(ident)

    @property
    def table_identifier(self):
        return self.db_class.quote_table(
            db_schema=self.db_schema, table_name=self.table
        )

    @property
    def feature_count(self):
        with self.engine.connect() as conn:
            return conn.scalar(f"SELECT COUNT(*) FROM {self.table_identifier};")

    def features(self):
        # Make sure to use the raw schema from the db - self.schema can be modified.
        schema = Schema.from_column_dicts(self.meta_items_from_db().get("schema.json"))
        table_def = self.db_type.adapter.table_def_for_schema(
            schema, db_schema=self.db_schema, table_name=self.table
        )
        query = sqlalchemy.select(table_def.columns).select_from(table_def)
        with self.engine.connect() as conn:
            r = (
                conn.execution_options(stream_results=True)
                .execute(query)
                .yield_per(self.CURSOR_SIZE)
            )
            yield from self._resultset_as_dicts(r)

    def _resultset_as_dicts(self, resultset):
        for row in resultset:
            yield dict(zip(row.keys(), row))

    def _first_pk_values(self, row_pks):
        # (123,) --> 123. we only handle one pk field
        for x in row_pks:
            assert len(x) == 1
            yield x[0]

    def get_features(self, row_pks, *, ignore_missing=False):
        pk_names = [c.name for c in self.schema.pk_columns]
        if len(pk_names) != 1:
            raise NotYetImplemented(
                "Sorry, importing specific IDs is supported only when there is one primary key column:\n"
                + ", ".join(pk_names)
            )

        [pk_name] = pk_names

        table_def = self.db_type.adapter.table_def_for_schema(
            self.schema, db_schema=self.db_schema, table_name=self.table
        )

        with self.engine.connect() as conn:
            for pk_chunk in chunk(self._first_pk_values(row_pks), 10000):
                query = (
                    sqlalchemy.select(table_def.columns)
                    .select_from(table_def)
                    .where(table_def.c[pk_name].in_(pk_chunk))
                )
                r = conn.execution_options(stream_results=True).execute(query)
                yield from self._resultset_as_dicts(r)

    @property
    @functools.lru_cache(maxsize=1)
    def geometry_column_names(self):
        return [c.name for c in self.schema.geometry_columns]
