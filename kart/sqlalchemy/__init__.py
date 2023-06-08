from enum import Enum, auto
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit, urlunsplit

import sqlalchemy as sa
from sqlalchemy import MetaData


class DbType(Enum):
    """Different types of Database connection currently supported Kart."""

    GPKG = auto()
    POSTGIS = auto()
    SQL_SERVER = auto()
    MYSQL = auto()

    @classmethod
    def from_spec(cls, spec):
        spec = str(spec)
        if spec.startswith("postgresql:"):
            return DbType.POSTGIS
        elif spec.startswith("mssql:"):
            return DbType.SQL_SERVER
        elif spec.startswith("mysql:"):
            return DbType.MYSQL
        elif spec.lower().endswith(".gpkg"):
            return DbType.GPKG
        return None

    @property
    def class_(self):
        if self is DbType.GPKG:
            from .gpkg import Db_GPKG

            return Db_GPKG
        elif self is DbType.POSTGIS:
            from .postgis import Db_Postgis

            return Db_Postgis
        elif self is DbType.SQL_SERVER:
            from .sqlserver import Db_SqlServer

            return Db_SqlServer
        elif self is DbType.MYSQL:
            from .mysql import Db_MySql

            return Db_MySql
        raise RuntimeError("Invalid DbType")

    @property
    def adapter(self):
        if self is DbType.GPKG:
            from .adapter.gpkg import KartAdapter_GPKG

            return KartAdapter_GPKG
        elif self is DbType.POSTGIS:
            from .adapter.postgis import KartAdapter_Postgis

            return KartAdapter_Postgis
        elif self is DbType.SQL_SERVER:
            from .adapter.sqlserver import KartAdapter_SqlServer

            return KartAdapter_SqlServer
        elif self is DbType.MYSQL:
            from .adapter.mysql import KartAdapter_MySql

            return KartAdapter_MySql
        raise RuntimeError("Invalid DbType")

    @property
    def json_name(self):
        if self is DbType.GPKG:
            return "gpkg"
        elif self is DbType.POSTGIS:
            return "postgresql"
        elif self is DbType.SQL_SERVER:
            return "mssql"
        elif self is DbType.MYSQL:
            return "mysql"

    def path_length(self, spec):
        """
        Returns the number of identifiers included in the URI path that narrow down our focus to a particular
        DBNAME or DBSCHEMA contained within the server. Defined to be zero for a path to a GPKG file.
        """
        if self is self.GPKG:
            return 0
        url_path = urlsplit(spec).path
        return len(PurePosixPath(url_path).parents)

    @property
    def path_length_for_table(self):
        """Returns the number of identifiers (or path-parts) required to uniquely specify a table."""
        if self is self.GPKG:
            return 1  # TABLE
        elif self is self.MYSQL:
            return 2  # DBNAME.TABLE
        elif self in (self.POSTGIS, self.SQL_SERVER):
            return 3  # DBNAME.DBSCHEMA.TABLE

    @property
    def path_length_for_table_container(self):
        return self.path_length_for_table - 1

    def clearly_doesnt_exist(self, spec):
        if self is self.GPKG:
            return not Path(spec).expanduser().exists()
        # Can't easily check if other DB types exists - we just try to connect and report any errors that occur.
        return False


def strip_username_and_password(uri):
    """Removes username and password from URI."""
    p = urlsplit(uri)
    if p.username is not None or p.password is not None:
        nl = p.hostname
        if p.port is not None:
            nl += f":{p.port}"
        p = p._replace(netloc=nl)

    return p.geturl()


def strip_password(uri):
    """Removes password from URI but keeps username."""
    p = urlsplit(uri)
    if p.password is not None:
        nl = p.hostname
        if p.username is not None:
            nl = f"{p.username}@{nl}"
        if p.port is not None:
            nl += f":{p.port}"
        p = p._replace(netloc=nl)

    return p.geturl()


def strip_query(uri):
    """Removes query parameters from URI."""
    p = urlsplit(uri)
    if p.query is not None:
        p = p._replace(query=None)
    return p.geturl()


def separate_last_path_part(uri):
    """
    Removes the last part of the path from a URI and returns it separately.
    Generally useful for connecting to the URI but at a less specific level than the one given,
    Eg, when given a URI of the form SCHEME://HOST/DBNAME/DBSCHEMA, we want to connect to SCHEME://HOST/DBNAME
    and then specify the DBSCHEMA separately in each query.
    """
    url = urlsplit(uri)
    url_path = PurePosixPath(url.path)

    last_part = url_path.name
    modified_url_path = str(url_path.parent)
    modified_url = urlunsplit(
        [url.scheme, url.netloc, modified_url_path, url.query, ""]
    )
    return (modified_url, last_part)


def text_with_inlined_params(text, params):
    """
    Uses sqlalchemy feature bindparam(literal_execute=True)
    to ensure that the params are inlined as literals during execution (ie "LIMIT 5"),
    and not left as placeholders (ie "LIMIT :limit", {"limit": 5}).
    This is required when the DBA doesn't support placeholders in a particular context.
    See https://docs.sqlalchemy.org/en/14/core/sqlelement.html?highlight=execute#sqlalchemy.sql.expression.bindparam.params.literal_execute

    Note: this sqlalchemy feature is new and still a bit clunky.
    Each param can only be inlined once - to inline the same value twice, it must have two different names.
    """
    return sa.text(text).bindparams(
        *[
            sa.bindparam(key, value, literal_execute=True)
            for key, value in params.items()
        ]
    )


class TableSet:
    """
    A class that holds a set of table definitions that can be created and/or used for selects / inserts / updates.
    Generally, instances of this class are used (rather than the class itself), but for those subclasses which can
    only have one possible definition of each table, the tables definitions can be copied to the class for convenience.
    """

    def __init__(self):
        self.sqlalchemy_metadata = MetaData()

    def create_all(self, session):
        return self.sqlalchemy_metadata.create_all(session.connection())

    @classmethod
    def _create_all_classmethod(cls, session):
        return cls().sqlalchemy_metadata.create_all(session.connection())

    @classmethod
    def copy_tables_to_class(cls):
        for table_name, table in cls().sqlalchemy_metadata.tables.items():
            setattr(cls, table_name, table)
        cls.create_all = cls._create_all_classmethod
