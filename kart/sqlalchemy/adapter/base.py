class BaseKartAdapter:
    """
    A KartAdapter adapts the Kart model (currently Datasets V2) - to or from a table in a sqlalchemy database.
    Adapts not just the features / table rows, but also other metadata such as title, description,
    CRS definitions and XML metadata (if the storage of this metadata is supported by the sqlalchemy
    database in a standardised way).
    """

    @classmethod
    def v2_schema_to_sql_spec(cls, schema):
        """
        Given a V2 schema object, returns a SQL specification that can be used with CREATE TABLE:
        For example: 'fid INTEGER, geom GEOMETRY(POINT,2136), desc VARCHAR(128), PRIMARY KEY(fid)'
        The SQL dialect and types will be conformant to the sqlalchemy database that this adapter supports.
        Some type information will be approximated if it is not fully supported by the database.

        schema - a kart.schema.Schema object.
        """

        raise NotImplementedError()

    @classmethod
    def all_v2_meta_items(cls, sess, db_schema, table_name, id_salt):
        """
        Generate all V2 meta items for the specified table, yielded as key-value pairs.
        Guaranteed to at least generate the table's V2 schema with key "schema.json".
        Possibly returns any or all of the title, description, xml metadata, and attached CRS definitions.
        Varying the id_salt varies the column ids that are generated for the schema.json item -
        these are generated deterministically so that running the same command twice in a row produces the same output.
        But if the user does something different later, a different salt should be provided.

        sess - an open sqlalchemy session.
        db_schema - the db schema (or similar) that contains the table, if any.
        table_name - the table to generate meta items for.
        id_salt - a string based on the current state that should change when the circumstances change.
        """

        raise NotImplementedError()

    # TODO - move other common functions - or at least declare their signatures - in BaseKartAdapter.
