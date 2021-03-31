import functools
import re
from urllib.parse import urlsplit, urlunsplit

import click
from sqlalchemy.exc import DBAPIError

from .base import WorkingCopy, WorkingCopyStatus
from sno.exceptions import InvalidOperation, DbConnectionError


class DatabaseServer_WorkingCopy(WorkingCopy):
    """Functionality common to working copies that connect to a database server."""

    @property
    def URI_SCHEME(self):
        """The URI scheme to connect to this type of database, eg "postgresql"."""
        raise NotImplementedError()

    @classmethod
    def check_valid_creation_path(cls, wc_path, workdir_path=None):
        cls.check_valid_path(wc_path, workdir_path)

        working_copy = cls(None, wc_path)
        status = working_copy.status()
        if status & WorkingCopyStatus.NON_EMPTY:
            db_schema = working_copy.db_schema
            container_text = f"schema '{db_schema}'" if db_schema else "working copy"
            raise InvalidOperation(
                f"Error creating {cls.WORKING_COPY_TYPE_NAME} working copy at {wc_path} - "
                f"non-empty {container_text} already exists"
            )

    @classmethod
    def check_valid_path(cls, wc_path, workdir_path=None):
        cls.check_valid_db_uri(wc_path, workdir_path)

    @classmethod
    def normalise_path(cls, repo, wc_path):
        return wc_path

    @classmethod
    def check_valid_db_uri(cls, db_uri, workdir_path=None):
        """
        For working copies that connect to a database - checks the given URI is in the required form:
        >>> URI_SCHEME::[HOST]/DBNAME/DBSCHEMA
        """
        url = urlsplit(db_uri)

        if url.scheme != cls.URI_SCHEME:
            raise click.UsageError(
                f"Invalid {cls.WORKING_COPY_TYPE_NAME} URI - "
                f"Expecting URI in form: {cls.URI_SCHEME}://[HOST]/DBNAME/DBSCHEMA"
            )

        url_path = url.path
        path_parts = url_path[1:].split("/", 3) if url_path else []

        suggestion_message = ""
        if len(path_parts) == 1 and workdir_path is not None:
            suggested_path = f"/{path_parts[0]}/{cls.default_db_schema(workdir_path)}"
            suggested_uri = urlunsplit(
                [url.scheme, url.netloc, suggested_path, url.query, ""]
            )
            suggestion_message = f"\nFor example: {suggested_uri}"

        if len(path_parts) != 2:
            raise click.UsageError(
                f"Invalid {cls.WORKING_COPY_TYPE_NAME} URI - URI requires both database name and database schema:\n"
                f"Expecting URI in form: {cls.URI_SCHEME}://[HOST]/DBNAME/DBSCHEMA"
                + suggestion_message
            )

    @classmethod
    def _separate_db_schema(cls, db_uri):
        """
        Removes the DBSCHEMA part off the end of a uri in the form URI_SCHEME::[HOST]/DBNAME/DBSCHEMA -
        and returns the URI and the DBSCHEMA separately.
        Useful since generally, URI_SCHEME::[HOST]/DBNAME is what is needed to connect to the database,
        and then DBSCHEMA must be specified in each query.
        """
        url = urlsplit(db_uri)
        url_path = url.path
        path_parts = url_path[1:].split("/", 3) if url_path else []
        assert len(path_parts) == 2
        url_path = "/" + path_parts[0]
        db_schema = path_parts[1]
        return urlunsplit([url.scheme, url.netloc, url_path, url.query, ""]), db_schema

    @classmethod
    def default_db_schema(cls, workdir_path):
        """Returns a suitable default database schema - named after the folder this Sno repo is in."""
        stem = workdir_path.stem
        schema = re.sub("[^a-z0-9]+", "_", stem.lower()) + "_sno"
        if schema[0].isdigit():
            schema = "_" + schema
        return schema

    @property
    def clean_path(self):
        return self.strip_password(self.uri)

    @classmethod
    def strip_password(cls, uri):
        p = urlsplit(uri)
        if p.password is not None:
            nl = p.hostname
            if p.username is not None:
                nl = f"{p.username}@{nl}"
            if p.port is not None:
                nl += f":{p.port}"
            p = p._replace(netloc=nl)

        return p.geturl()

    @property
    @functools.lru_cache(maxsize=1)
    def DB_SCHEMA(self):
        """Escaped, dialect-specific name of the database-schema owned by this working copy (if any)."""
        if self.db_schema is None:
            raise RuntimeError("No schema to escape.")
        return self.preparer.format_schema(self.db_schema)

    def _db_connection_error(self, causal_error):
        message = f"Error connecting to {self.WORKING_COPY_TYPE_NAME} working copy at {self.clean_path}"
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

                sno_table_count = sess.scalar(
                    f"""
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema=:table_schema AND table_name IN ('{self.SNO_STATE_NAME}', '{self.SNO_TRACK_NAME}');
                    """,
                    {"table_schema": self.db_schema},
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
                if sno_table_count == 2:
                    result |= WorkingCopyStatus.INITIALISED
                if schema_table_count > sno_table_count:
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
            self.sno_tables.create_all(sess)
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
            self._drop_all_tables(sess)
            self._drop_all_functions(sess)

        if not keep_db_schema_if_possible:
            with self.session() as sess:
                self._drop_schema(sess, treat_error_as_warning=True)

    def _drop_all_tables(self, sess):
        """Drops all tables in schema self.db_schema"""
        r = sess.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema=:table_schema;",
            {"table_schema": self.db_schema},
        )
        table_identifiers = ", ".join((self.table_identifier(row[0]) for row in r))
        if table_identifiers:
            sess.execute(f"DROP TABLE IF EXISTS {table_identifiers};")

    def _drop_all_functions(self, sess):
        """Drops all functions in schema self.db_schema"""
        # Subclasses only need to override if they create functions in the working copy.
        pass

    def _drop_schema(self, sess, treat_error_as_warning=False):
        """Drops the schema self.db_schema"""
        try:
            sess.execute(f"DROP SCHEMA IF EXISTS {self.DB_SCHEMA};")
        except DBAPIError as e:
            if treat_error_as_warning:
                click.echo(
                    f"Couldn't delete schema {self.db_schema} at {self.clean_path} due to the following error:\n{e}",
                    err=True,
                )
            else:
                raise e
