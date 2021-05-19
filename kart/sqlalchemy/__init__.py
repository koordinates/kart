from enum import Enum, auto
from pathlib import PurePosixPath
from urllib.parse import urlsplit, urlunsplit

import sqlalchemy as sa


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


def separate_last_path_part(uri):
    """
    Removes the last part of the path from a URI and returns it separately.
    Generally useful for connecting to the URI but at a less specific level than the one given,
    Eg, when given a URI of the form SCHEME://HOST/DNAME/DBSCHEMA, we want to connect to SCHEME://HOST/DBNAME
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
