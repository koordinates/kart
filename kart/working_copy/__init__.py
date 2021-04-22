from enum import Enum, IntEnum, auto

import click


class WorkingCopyType(Enum):
    """Different types of working copy currently supported by Kart."""

    GPKG = auto()
    POSTGIS = auto()
    SQL_SERVER = auto()

    @classmethod
    def from_location(cls, location, allow_invalid=False):
        location = str(location)
        if location.startswith("postgresql:"):
            return WorkingCopyType.POSTGIS
        elif location.startswith("mssql:"):
            return WorkingCopyType.SQL_SERVER
        elif location.lower().endswith(".gpkg"):
            return WorkingCopyType.GPKG
        elif allow_invalid:
            return None
        else:
            raise click.UsageError(
                f"Unrecognised working copy type: {location}\n"
                "Try one of:\n"
                "  PATH.gpkg\n"
                "  postgresql://[HOST]/DBNAME/DBSCHEMA\n"
                "  mssql://[HOST]/DBNAME/DBSCHEMA"
            )

    @property
    def class_(self):
        if self is WorkingCopyType.GPKG:
            from .gpkg import WorkingCopy_GPKG

            return WorkingCopy_GPKG
        elif self is WorkingCopyType.POSTGIS:
            from .postgis import WorkingCopy_Postgis

            return WorkingCopy_Postgis
        elif self is WorkingCopyType.SQL_SERVER:
            from .sqlserver import WorkingCopy_SqlServer

            return WorkingCopy_SqlServer
        raise RuntimeError("Invalid WorkingCopyType")


class WorkingCopyStatus(IntEnum):
    """
    Different status that a working copy can have.
    A working copy can have more than one status at a time, eg:
    >>> FILE_EXISTS | INITIALISED.
    """

    DB_SCHEMA_EXISTS = 0x1  # The database schema for this working copy exists.
    FILE_EXISTS = 0x2  # The file (eg GPKG file) for this working copy exists.
    NON_EMPTY = 0x4  # At least one table of any sort exists in this working copy.
    INITIALISED = 0x8  # All required Kart tables exist.
    HAS_DATA = 0x10  # At least one table that is not a Kart table exists.
    DIRTY = 0x20  # Working copy has uncommitted changes.

    # A working copy "exists" if it is a file that exists, or it is a non-empty db schema.
    # An empty db schema is a working copy in a valid state of non-existance.
    WC_EXISTS = FILE_EXISTS | NON_EMPTY

    UNCONNECTABLE = 0x1000  # Couldn't connect to this working copy.
