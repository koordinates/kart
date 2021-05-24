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
        """

        raise NotImplementedError()

    # TODO - move other common functions - or at least declare their signatures - in BaseKartAdapter.
