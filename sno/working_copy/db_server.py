import functools
import re
from urllib.parse import urlsplit, urlunsplit

import click

from .base import WorkingCopy
from sno.exceptions import InvalidOperation


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
        if working_copy.has_data():
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
    @functools.lru_cache(maxsize=1)
    def DB_SCHEMA(self):
        """Escaped, dialect-specific name of the database-schema owned by this working copy (if any)."""
        if self.db_schema is None:
            raise RuntimeError("No schema to escape.")
        return self.preparer.format_schema(self.db_schema)

    def is_created(self):
        """
        Returns true if the DB schema referred to by this working copy exists and
        contains at least one table. If it exists but is empty, it is treated as uncreated.
        This is so the DB schema can be created ahead of time before a repo is created
        or configured, without it triggering code that checks for corrupted working copies.
        Note that it might not be initialised as a working copy - see self.is_initialised.
        """
        with self.session() as sess:
            count = sess.scalar(
                """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema=:table_schema;
                """,
                {"table_schema": self.db_schema},
            )
            return count > 0

    def is_initialised(self):
        """
        Returns true if the working copy is initialised -
        the db schema exists and has the necessary sno tables, _sno_state and _sno_track.
        """
        with self.session() as sess:
            count = sess.scalar(
                f"""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema=:table_schema AND table_name IN ('{self.SNO_STATE_NAME}', '{self.SNO_TRACK_NAME}');
                """,
                {"table_schema": self.db_schema},
            )
            return count == 2

    def has_data(self):
        """
        Returns true if the PostGIS working copy seems to have user-created content already.
        """
        with self.session() as sess:
            count = sess.scalar(
                f"""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema=:table_schema AND table_name NOT IN ('{self.SNO_STATE_NAME}', '{self.SNO_TRACK_NAME}');
                """,
                {"table_schema": self.db_schema},
            )
            return count > 0
