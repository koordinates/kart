import functools
import re
from pathlib import PurePosixPath
from urllib.parse import urlsplit, urlunsplit

import click

from kart.exceptions import DbConnectionError, InvalidOperation
from kart.sqlalchemy import DbType, strip_password
from sqlalchemy.exc import DBAPIError

from . import WorkingCopyStatus
from .base import BaseWorkingCopy


class DatabaseServer_WorkingCopy(BaseWorkingCopy):
    """Functionality common to working copies that connect to a database server."""

    @property
    @classmethod
    def URI_SCHEME(cls):
        """The URI scheme to connect to this type of database, eg "postgresql"."""
        raise NotImplementedError()

    # The expected URI format, not including the scheme, as displayed to the user (not used for parsing URIs).
    URI_FORMAT = "//HOST[:PORT]/DBNAME/DBSCHEMA"
    # Message for when the URI path is not a valid length.
    INVALID_PATH_MESSAGE = (
        "URI path must have two parts: database name and database schema"
    )

    @classmethod
    def check_valid_creation_location(cls, wc_location, repo):
        cls.check_valid_location(wc_location, repo)

        working_copy = cls(repo, wc_location)
        status = working_copy.status()
        if status & WorkingCopyStatus.NON_EMPTY:
            db_schema = working_copy.db_schema
            container_text = f"schema '{db_schema}'" if db_schema else "working copy"
            raise InvalidOperation(
                f"Error creating {cls.WORKING_COPY_TYPE_NAME} working copy at {wc_location} - "
                f"non-empty {container_text} already exists"
            )

    @classmethod
    def clearly_doesnt_exist(cls, wc_location, repo):
        # As documented in base class - we can't connect to a database for this quick check, so have to return False.
        return False

    @classmethod
    def normalise_location(cls, wc_location, repo):
        return wc_location

    @classmethod
    def check_valid_location(cls, wc_location, repo):
        """
        For working copies that connect to a database - checks the given URI is in the required form -
        generally URI_SCHEME::HOST/DBNAME/DBSCHEMA but some implementations may have different path formats.
        """
        url = urlsplit(wc_location)

        assert url.scheme == cls.URI_SCHEME

        db_type = DbType.from_spec(wc_location)
        path_length = db_type.path_length(wc_location)
        required_path_length = db_type.path_length_for_table_container

        if path_length != required_path_length:
            if (path_length + 1) == required_path_length:
                suggested_path = PurePosixPath(url.path) / cls.default_db_schema(repo)
                suggested_uri = urlunsplit(
                    [url.scheme, url.netloc, str(suggested_path), url.query, ""]
                )
                suggestion_message = f"\nFor example: {suggested_uri}"
            else:
                suggestion_message = ""

            raise click.UsageError(
                f"Invalid {cls.WORKING_COPY_TYPE_NAME} URI - {cls.INVALID_PATH_MESSAGE}:\n"
                f"Expecting URI in form: {cls.URI_SCHEME}:{cls.URI_FORMAT}"
                + suggestion_message
            )

    @classmethod
    def default_db_schema(cls, repo):
        """Returns a suitable default database schema - named after the folder this repo is in."""
        stem = repo.workdir_path.stem
        suffix = "_kart" if repo.is_kart_branded else "_sno"
        schema = re.sub("[^a-z0-9]+", "_", stem.lower()) + suffix
        if schema[0].isdigit():
            schema = "_" + schema
        return schema

    @property
    def clean_location(self):
        return strip_password(self.uri)

    @property
    @functools.lru_cache(maxsize=1)
    def DB_SCHEMA(self):
        """Escaped, dialect-specific name of the database-schema owned by this working copy (if any)."""
        if self.db_schema is None:
            raise RuntimeError("No schema to escape.")
        return self.preparer.format_schema(self.db_schema)

    def _db_connection_error(self, causal_error):
        message = (
            f"Error connecting to {self.WORKING_COPY_TYPE_NAME} working copy at {self}"
        )
        return DbConnectionError(message, causal_error)

    def status(self, check_if_dirty=False, allow_unconnectable=False):
        result = 0
        try:
            with self.session() as sess:
                if not sess.scalar(
                    "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name=:schema_name;",
                    {"schema_name": self.db_schema},
                ):
                    return result

                result |= WorkingCopyStatus.DB_SCHEMA_EXISTS

                kart_table_count = sess.scalar(
                    """
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema=:table_schema AND table_name IN (:kart_state_name, :kart_track_name);
                    """,
                    {
                        "table_schema": self.db_schema,
                        "kart_state_name": self.KART_STATE_NAME,
                        "kart_track_name": self.KART_TRACK_NAME,
                    },
                )
                schema_table_count = sess.scalar(
                    """
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema=:table_schema;
                    """,
                    {"table_schema": self.db_schema},
                )
                if schema_table_count:
                    result |= WorkingCopyStatus.NON_EMPTY
                if kart_table_count == 2:
                    result |= WorkingCopyStatus.INITIALISED
                if schema_table_count > kart_table_count:
                    result |= WorkingCopyStatus.HAS_DATA

            if (
                (result & WorkingCopyStatus.INITIALISED)
                and check_if_dirty
                and self.is_dirty()
            ):
                result |= WorkingCopyStatus.DIRTY

        except DBAPIError as e:
            if allow_unconnectable:
                result |= WorkingCopyStatus.UNCONNECTABLE
            else:
                raise self._db_connection_error(e)

        return result

    def create_and_initialise(self):
        with self.session() as sess:
            self.create_schema(sess)
            self.kart_tables.create_all(sess)
            self.create_common_functions(sess)

    def create_schema(self, sess):
        """Creates the schema named by self.db_schema, if it doesn't exist."""
        # We have to check if the schema exists before creating it.
        # CREATE SCHEMA IF NOT EXISTS may not work at all, or may require CREATE permissions even if the schema exists.
        schema_exists = sess.scalar(
            "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name=:schema_name;",
            {"schema_name": self.db_schema},
        )
        if not schema_exists:
            sess.execute(f"CREATE SCHEMA {self.DB_SCHEMA}")

    def create_common_functions(self, sess):
        """
        Create any functions that are not specific to a particular user table, but are common to any/all of them,
        and so must be created during the initialisation step.
        """
        pass

    def delete(self, keep_db_schema_if_possible=False):
        # We don't use DROP SCHEMA CASCADE since that could possibly delete things outside the schema
        # if they've been linked to it using foreign keys, and we only want to delete the schema that we manage.
        with self.session() as sess:
            self.adapter.drop_all_in_schema(sess, self.db_schema)

        if not keep_db_schema_if_possible:
            with self.session() as sess:
                self._drop_schema(sess, treat_error_as_warning=True)

    def _drop_schema(self, sess, treat_error_as_warning=False):
        """Drops the schema self.db_schema"""
        try:
            sess.execute(f"DROP SCHEMA IF EXISTS {self.DB_SCHEMA};")
        except DBAPIError as e:
            if treat_error_as_warning:
                click.echo(
                    f"Couldn't delete schema {self.db_schema} at {self} due to the following error:\n{e}",
                    err=True,
                )
            else:
                raise e
