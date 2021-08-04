import logging

import sqlalchemy
from sqlalchemy.types import UserDefinedType

L = logging.getLogger("kart.sqlalchemy.adapter.base")


class BaseKartAdapter:
    """
    A KartAdapter adapts the Kart model (currently Datasets V2) - to or from a table in a sqlalchemy database.
    Adapts not just the features / table rows, but also other metadata such as title, description,
    CRS definitions and XML metadata (if the storage of this metadata is supported by the sqlalchemy
    database in a standardised way).
    """

    # Certain types have a small set of subtypes that can be distinguished between by checking the extra type info.
    # For instance, integers and floats have subtypes that have different "size" attribues.
    SUBTYPE_KEYS = {
        "integer": "size",
        "float": "size",
        "timestamp": "timezone",
    }
    DEFAULT_SUBTYPE_VALUES = {"size": 0}

    @classmethod
    def v2_schema_to_sql_spec(cls, schema, v2_obj=None):
        """
        Given a V2 schema object, returns a SQL specification that can be used with CREATE TABLE.
        For example: 'fid INTEGER, geom GEOMETRY(POINT,2136), desc VARCHAR(128), PRIMARY KEY(fid)'
        The SQL dialect and types will be conformant to the sqlalchemy database that this adapter supports.
        Some type information will be approximated if it is not fully supported by the database.

        schema - a kart.schema.Schema object.
        v2_obj - the V2 object (eg a dataset) with this schema - used for looking up CRS definitions (if needed).
        """
        has_int_pk = cls._schema_has_int_pk(schema)
        result = [
            cls.v2_column_schema_to_sql_spec(col, v2_obj, has_int_pk=has_int_pk)
            for col in schema
        ]

        if schema.pk_columns:
            pk_col_names = ", ".join((cls.quote(col.name) for col in schema.pk_columns))
            result.append(f"PRIMARY KEY({pk_col_names})")

        return ", ".join(result)

    @classmethod
    def _schema_has_int_pk(cls, schema):
        return (
            len(schema.pk_columns) == 1 and schema.pk_columns[0].data_type == "integer"
        )

    @classmethod
    def v2_column_schema_to_sql_spec(cls, col, v2_obj=None, has_int_pk=False):
        """
        Given a V2 column schema object, returns a SQL specification that can be used with CREATE TABLE.
        Can include extra constraints (eg non-null, unique) if they are required for some reason.
        For example: 'fid INTEGER NOT NULL' or 'geom GEOMETRY(POINT,2136)'.
        Doesn't include the primary key specification - this is handled by v2_schema_to_sql_spec.

        schema - a kart.schema.ColumnSchema object.
        v2_obj - the V2 object (eg a dataset) with this schema - used for looking up CRS definitions (if needed).
        """
        col_name = cls.quote(col.name)
        sql_type = cls.v2_type_to_sql_type(col, v2_obj)
        return f"{col_name} {sql_type}"

    @classmethod
    def v2_type_to_sql_type(cls, col, v2_obj=None):
        """
        Given a V2 column schema object, returns a SQL type specificier that can be used with CREATE TABLE.
        For example: "INTEGER" or "GEOMETRY(POINT,2136)".
        Doesn't include column name or any other constraints eg non-null, unique.

        schema - a kart.schema.ColumnSchema object.
        v2_obj - the V2 object (eg a dataset) with this schema - used for looking up CRS definitions (if needed).
        """
        v2_type = col.data_type

        # This implementation just looks up V2_TYPE_TO_SQL_TYPE.
        # Any extra work to be done (eg handling of extra_type_info) must be performed by the subclass.

        subtype_key = cls.SUBTYPE_KEYS.get(v2_type)
        if subtype_key:
            sql_type_options = cls.V2_TYPE_TO_SQL_TYPE.get(v2_type)
            if not sql_type_options:
                raise ValueError(f"Unrecognised V2 data type: {v2_type}")

            v2_subtype_value = col.extra_type_info.get(
                subtype_key, cls.DEFAULT_SUBTYPE_VALUES.get(subtype_key)
            )
            sql_type = sql_type_options.get(v2_subtype_value)
            if not sql_type:
                raise ValueError(f"Invalid {subtype_key} value: {v2_subtype_value}")
            return sql_type

        else:
            sql_type = cls.V2_TYPE_TO_SQL_TYPE.get(v2_type)
            if not sql_type:
                raise ValueError(f"Unrecognised V2 data type: {v2_type}")
            return sql_type

    @classmethod
    def sql_type_to_v2_type(cls, sql_type):
        """
        Given the name of a SQL type, returns the equivalent V2 type as a tuple (data_type, extra_type_info).
        For example: ("integer", {"size": 32}).
        Note that the sql_type on its own may or may not contain all needed information to specify a V2 type.
        Subclasses can use or augment this method as they see fit - what is required is that the result of
        all_v2_meta_items includes a schema.json with the right data types.
        """

        # This implementation just looks up SQL_TYPE_TO_V2_TYPE.
        # Any extra work to be done (eg handling of extra_type_info) must be performed by the subclass.
        sql_type = sql_type.upper()
        v2_type_info = cls.SQL_TYPE_TO_V2_TYPE.get(sql_type)
        if v2_type_info is None:
            L.warn(f"SQL type {sql_type} not fully supported - importing as text")
            return "text", {}

        if isinstance(v2_type_info, tuple):
            v2_type, v2_subtype_value = v2_type_info
            subtype_key = cls.SUBTYPE_KEYS.get(v2_type)
            return v2_type, {subtype_key: v2_subtype_value}
        else:
            v2_type = v2_type_info
            return v2_type, {}

    @classmethod
    def all_v2_meta_items(
        cls, sess, db_schema, table_name, id_salt, include_legacy_items=False
    ):
        """
        Returns a dict all V2 meta items for the specified table.
        Guaranteed to at least generate the table's V2 schema with key "schema.json", if the table exists at all.
        Possibly returns any or all of the title, description, xml metadata, and attached CRS definitions.
        Varying the id_salt varies the column ids that are generated for the schema.json item -
        these are generated deterministically so that running the same command twice in a row produces the same output.
        But if the user does something different later, a different salt should be provided.

        sess - an open sqlalchemy session.
        db_schema - the db schema (or similar) that contains the table, if any.
        table_name - the table to generate meta items for.
        id_salt - a string based on the current state that should change when the circumstances change.
        """

        return cls.remove_empty_values(
            cls.all_v2_meta_items_including_empty(
                sess,
                db_schema,
                table_name,
                id_salt,
                include_legacy_items=include_legacy_items,
            )
        )

    @classmethod
    def remove_empty_values(cls, meta_items):
        """
        Given a dict of V2 meta item key-value pairs, remove empty ones which contain no data.
        This normalises three different ways of showing no data - either key omitted, {key: None}, or {key: ""} -
        into the first way: key ommitted.
        """
        result = {}
        for key, value in meta_items.items():
            # Don't skip over CRS entries even if they are empty - the name of the CRS could be informative,
            # even if we can't find the definition.
            if key.startswith("crs/") or value:
                result[key] = value
        return result

    @classmethod
    def table_def_for_schema(cls, schema, table_name, db_schema=None, dataset=None):
        """
        Returns a sqlalchemy table definition with conversion-logic for reading or writing data with the given schema
        to or from the given table.

        schema - a kart.schema.Schema
        table_name - the name of the table.
        db_schema - the database schema containing the table, if any.
        dataset - this is used to look up CRS definitions referred to by the schema (if  needed for type conversion).
        """
        return sqlalchemy.Table(
            table_name,
            sqlalchemy.MetaData(),
            *[cls._column_def_for_column_schema(c, dataset) for c in schema],
            schema=db_schema,
        )

    @classmethod
    def _column_def_for_column_schema(cls, col, dataset=None):
        """
        Returns a sqlalchemy column definition with conversion-logic for reading or writing data with the given
        column-schema to or from the given dataset.

        col - a kart.schema.ColumnSchema
        dataset - this is used to look up CRS definitions referred to by the schema (if  needed for type conversion).
        """
        return sqlalchemy.Column(
            col.name,
            cls._type_def_for_column_schema(col, dataset),
            primary_key=col.pk_index is not None,
        )

    def _type_def_for_column_schema(cls, col, dataset=None):
        """
        Returns a ConverterType suitable for converting Kart values of type `col.data_type` to or from the equivalent
        SQL type for this type of database.
        Can simply return None if no type conversion is required - for instance the Kart value read for an "integer"
        should be int, and most DB-API drivers will return an int when an integral type is read, so no conversion needed.
        If a value read from the DB cannot be converted to the equivalent Kart type, it can be left as-is - this will
        be uncommittable, but the resulting error message gives the user a chance to find and fix the schema-violation.

        col - a kart.schema.ColumnSchema
        dataset - this is used to look up CRS definitions referred to by the schema (if  needed for type conversion).
        """
        raise NotImplementedError()

    # TODO - move other common functions - or at least declare their signatures - in BaseKartAdapter.


class ConverterType(UserDefinedType):
    """
    A User-defined-type that automatically converts values when reading and writing to the database.
    In SQLAlchemy, the most straight-forward way to create a type-converter is to define a user-defined-type that has
    extra logic when reading or writing - hence the name "converter-type".
    After each conversion step, the type of the resulting data is declared to be `self` - the user-defined-type - this
    is so that if there are more conversion steps at a different layer, they will still be run too.

    Subclasses should override some or all of the following:

    1. Called in Python layer before writing:
    def bind_processor(self, dialect):
        # Returns a converter function for pre-processing python values.

    2. Called in SQL layer during writing:
    def bind_expression(self, bindvalue):
        # Returns a SQL expression for writing the bindvalue to the database.

    At this point the data is at rest in the database. But, to continue the round-trip:

    3. Called in SQL layer during reading:
    def column_expression(self, column):
        # Returns a SQL expression for reading the column from the database.

    4. Called in Python layer after reading:
    def result_processor(self, dialect, coltype):
        # Returns a converter function for post-processing python values.
    """


def aliased_converter_type(cls):
    """
    A decorator that renames the functions in a ConverterType, so that the following methods definitions can
    be used instead of the sqlalchemy ones. This avoids overriding methods that are not needed since sqlalchemy
    tries to optimise by detecting which methods have been overridden and which are not.

    An @aliased_converter_type ConverterType should override some or all of the following:

    1. Called in Python layer before writing:
    def python_prewrite(self, value):
        # Pre-process value before writing to the database.

    2. Called in SQL layer during writing:
    def sql_write(self, bindvalue):
        # Returns a SQL expression for writing the bindvalue to the database.

    At this point the data is at rest in the database. But, to continue the round-trip:

    3. Called in SQL layer during reading:
    def sql_read(self, column):
        # Returns a SQL expression for reading the column from the database.

    4. Called in Python layer after reading:
    def python_postread(self, value):
        # Post-process value after reading from the database.
    """
    if hasattr(cls, "python_prewrite"):

        def bind_processor(self, dialect):
            return lambda v: self.python_prewrite(v)

        cls.bind_processor = bind_processor

    if hasattr(cls, "sql_write"):
        cls.bind_expression = cls.sql_write

    if hasattr(cls, "sql_read"):
        cls.column_expression = cls.sql_read

    if hasattr(cls, "python_postread"):

        def result_processor(self, dialect, coltype):
            return lambda v: self.python_postread(v)

        cls.result_processor = result_processor

    return cls
