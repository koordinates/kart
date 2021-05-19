import functools
import os
import sys

import click
import sqlalchemy

from .exceptions import (
    NotFound,
    NotYetImplemented,
    NO_TABLE,
)
from .geometry import Geometry
from .import_source import ImportSource
from .sqlalchemy import DbType, separate_last_path_part
from .output_util import dump_json_output
from .utils import ungenerator, chunk


class SqlAlchemyImportSource(ImportSource):
    """
    ImportSource that uses SqlAlchemy directly to import into Kart.
    Currently only GPKG is supported, but in theory should work for Postgis, SQL Server, MySQL.
    """

    CURSOR_SIZE = 10000

    @classmethod
    def open(cls, spec):
        db_type = DbType.from_spec(spec)
        if db_type is None:
            raise cls._bad_import_source_spec(spec)

        # TODO - add support for other DB types.
        if db_type is not DbType.GPKG:
            raise NotYetImplemented(
                "Only GPKG is currently supported by the SqlAlchemyImportSource"
            )

        path_length = db_type.path_length(spec)
        longest_allowed_path_length = db_type.path_length_for_table
        shortest_allowed_path_length = max(
            db_type.path_length_for_table_container - 1, 0
        )

        if (
            path_length > longest_allowed_path_length
            or path_length < shortest_allowed_path_length
        ):
            raise cls._bad_import_source_spec(spec)

        connect_url = spec
        db_schema = None
        table = None

        # Handle the case where specification already points to a single table.
        if path_length == db_type.path_length_for_table:
            connect_url, table = separate_last_path_part(connect_url)
            path_length -= 1

        # Handle the case where specification points to a database schema (or similar).
        if path_length > shortest_allowed_path_length:
            connect_url, db_schema = separate_last_path_part(connect_url)

        engine = db_type.class_.create_engine(connect_url)
        return SqlAlchemyImportSource(spec, db_type, engine, db_schema, table)

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
        db_type,
        engine,
        db_schema,
        table,
        **meta_overrides,
    ):
        self.original_spec = original_spec
        self.db_type = db_type
        self.db_class = db_type.class_
        self.engine = engine
        self.preparer = self.db_class.create_preparer(engine)

        self.db_schema = db_schema
        self.table = table
        self.meta_overrides = {k: v for k, v in meta_overrides.items() if v is not None}

    @property
    def source_name(self):
        # TODO - this only works for GPKG.
        return os.path.basename(self.original_spec)

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

    @functools.lru_cache(maxsize=1)
    def get_tables(self):
        with self.engine.connect() as conn:
            # TODO - this only works for GPKG
            tables = self.db_class.list_tables(conn, self.db_schema, with_titles=True)

        if self.table is not None:
            return {self.table: self.tables.get(self.table)}
        else:
            return tables

    def print_table_list(self, do_json=False):
        tables = self.get_tables()
        if do_json:
            dump_json_output({"kart.tables/v1": tables}, sys.stdout)
        else:
            click.secho("Tables found:", bold=True)
            for table_name, title in tables.items():
                click.echo(f"  {table_name} - {title or ''}")
        return tables

    def check_table(self, table_name):
        if table_name not in self.get_tables():
            raise NotFound(
                f"Table '{table_name}' not found",
                exit_code=NO_TABLE,
            )

    def clone_for_table(self, table, primary_key=None, **meta_overrides):
        meta_overrides = {**self.meta_overrides, **meta_overrides}
        self.check_table(table)

        if primary_key is not None:
            raise NotYetImplemented(
                "Sorry, overriding the primary key is not yet supported"
            )

        return SqlAlchemyImportSource(
            self.original_spec,
            self.db_type,
            self.engine,
            self.db_schema,
            table,
            **meta_overrides,
        )

    def get_meta_item(self, name):
        if name in self.meta_overrides:
            return self.meta_overrides[name]
        elif name == "metadata.xml" and "xml_metadata" in self.meta_overrides:
            return self.meta_overrides["xml_metadata"]
        return self.meta_items.get(name)

    @property
    @functools.lru_cache(maxsize=1)
    def meta_items(self):
        # TODO - this only works for GPKG.
        from kart.working_copy import gpkg_adapter

        id_salt = f"{self.engine.url} {self.db_schema} {self.table}"
        with self.engine.connect() as conn:
            return dict(gpkg_adapter.all_v2_meta_items(conn, self.table, id_salt))

    def crs_definitions(self):
        for key, value in self.meta_items.items():
            if key.startswith("crs/") and key.endswith(".wkt"):
                yield key[4:-4], value

    @ungenerator(dict)
    def _sqlalchemy_row_to_kart_feature(self, sa_row):
        # TODO - this only works for GPKG. Use the adapter code from working copies.
        for key, value in sa_row.items():
            if key in self.geometry_column_names:
                yield (key, Geometry.of(value))
            else:
                yield key, value

    def _sqlalchemy_to_kart_features(self, resultset):
        for sa_row in resultset:
            yield self._sqlalchemy_row_to_kart_feature(sa_row)

    def quote(self, ident):
        """Conditionally quote an identifier - eg if it is a reserved word or contains special characters."""
        return self.preparer.quote(ident)

    def features(self):
        with self.engine.connect() as conn:
            r = (
                conn.execution_options(stream_results=True)
                .execute(f"SELECT * FROM {self.quote(self.table)};")
                .yield_per(self.CURSOR_SIZE)
            )
            yield from self._sqlalchemy_to_kart_features(r)

    def _first_pk_values(self, row_pks):
        # (123,) --> 123. we only handle one pk field
        for x in row_pks:
            assert len(x) == 1
            yield x[0]

    def get_features(self, row_pks, *, ignore_missing=False):
        with self.engine.connect() as conn:
            pk_field = self.primary_key
            batch_query = sqlalchemy.text(
                f"SELECT * FROM {self.quote(self.table)} "
                f"WHERE {self.quote(pk_field)} IN :pks ;"
            ).bindparams(sqlalchemy.bindparam("pks", expanding=True))

            for batch in chunk(self._first_pk_values(row_pks), 1000):
                r = conn.execute(batch_query, {"pks": batch})
                yield from self._sqlalchemy_to_kart_features(r)

    @property
    @functools.lru_cache(maxsize=1)
    def geometry_column_names(self):
        return [c.name for c in self.schema.geometry_columns]

    @property
    @functools.lru_cache(maxsize=1)
    def primary_key(self):
        # TODO - this only works for GPKG.
        from kart.working_copy import gpkg_adapter

        with self.engine.connect() as conn:
            return gpkg_adapter.pk(conn, self.table)
