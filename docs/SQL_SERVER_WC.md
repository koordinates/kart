SQL Server Working Copy
--------------------

In order to use a [Microsoft SQL Server](https://docs.microsoft.com/sql/sql-server/) working copy, you need to have a SQL Server running. SQL Server 2016 and later is officially supported by Sno (SQL Server 2008 and later are largely compatible but not officially supported).

You also need to have the [Microsoft ODBC Driver for SQL Server](https://docs.microsoft.com/sql/connect/odbc/microsoft-odbc-driver-for-sql-server) installed on your system.

### SQL Server partitioning

SQL Server databases are designed so that they can be used for multiple tasks simultaneously without those tasks interfering with each other - they have multiple levels of data separation.

* A SQL Server contains one or more named databases. When a user connects to the server, they must specify up front which database they need, and then they can only access data in this database.
* A single database contains one or more named schemas, which in turn contain tables. A user connected to the database can query tables in any schema they have access-rights to without starting a new connection. Two tables can have the same name, as long as they are in different schemas.

So SQL Server has a partition called a "schema" - the name can be confusing as "schema" can also have other meanings, but in this case it means a namespace. A Sno SQL Server working copy is fine to share a server or a database with any other task, but it expects to be given its own schema to manage (just as Sno expects to manage its own GPKG working copy, not share it with other data). Managing the schema means that Sno is responsible for initialising that schema and importing the data in its initial state, then keeping track of any edits made to that data so that they can be committed. Sno expects that the user will use some other application to modify the data in that schema as part of making edits to a Sno working copy.

### SQL Server Connection URI

A Sno repository with a SQL Server working copy needs to be configured with a `mssql://` connection URI. This URI contains how to connect to the server, the name of the database to connect to (which can be shared with other tasks), and the name of the schema that should be managed as a working copy by this Sno repository.

A connection URL would generally have the following format:

`mssql://[user[:password]@][host][:port][/dbname]`

Since Sno also requires the schema to be specified up front, Sno needs a connection URL in the following format:

`mssql://[user[:password]@][host][:port]/dbname/dbschema`

For example, a Sno repo called `airport` might have a URL like the following:

`mssql://sno_user:password@localhost:1433/gis/airport_sno`

To configure a Sno repository to use a particular SQL Server schema as its working copy, specify the `--workingcopy` flag when creating the repository, for example:

`sno init --workingcopy=mssql://... --import=...`

The schema that Sno is given to manage should be either non-existent or empty at the time Sno is configured, but the server and database should already exist.

The database user needs to have full rights to modify objects in the specified schema. (eg: via `GRANT CONTROL ON SCHEMA airport_sno TO sno_user;`).

### SQL Server limitations

Almost all geospatial data can be converted to SQL Server format without losing any fidelity, but it does have the following limitations.

#### Approximated types

There is one type that Sno supports that has no SQL Server equivalent - the `interval`. This type is "approximated" as a string (or more precisely, as an `NVARCHAR`) in the SQL Server working copy while keeping its original type in the Sno dataset. Sno creates a column of type `NVARCHAR` in the appropriate place in the database table, and fills it with with the intervals formatted as [ISO8601 durations](https://en.wikipedia.org/wiki/ISO_8601#Durations). When the working copy is committed, Sno converts the contents of those columns back to `interval`, instead of their actual type, `NVARCHAR`. Since the change from `interval` to `NVARCHAR` is just a limitation of the working copy, this apparent change in type will not show up as a change in the commit log, nor will it show up as an uncommitted change if you run `sno status` to see what local changes you have made but not yet committed.

#### Geometry types

Sno lets you define a column as containing only a particular type of geometry, eg only `POINT` or only `MULTIPOLYGON` types. By contrast, SQL Server lets you put any type of geometry into a geometry column.

This mismatch has the following consequence: If Sno is managing a geometry column with a particular geometry type such as `POINT`, and you check it out in a SQL Server working copy, you will be able to insert other types of geometry into it, but Sno will prevent you from committing it. You still need to follow the constraint put in place when the dataset was created, and only insert new geometries of the appropriate type.

If you need decide that a certain dataset should contain more types of geometries than its constraint currently allows, it is possible to change a columns geometry type to be broader and allow more types. This cannot be done by editing the SQL Server working copy, since as noted it doesn't store this type information - instead it must be done using either a different type of working copy, or the Sno command line tool. To use the command line, take the following steps:

1. View all the metadata for your dataset:
   `sno meta get DATASET`
2. Copy the JSON from under the heading `schema.json` and save it to a file of the same name in your current working directory.
3. Modify the JSON so that the `geometryType` property is broader. For example:
   - Old line: `  "geometryType": "POINT",`
   - New line: `  "geometryType": "GEOMETRY",`
4. Commit this change to the schema:
   `sno meta set DATASET schema.json=@schema.json`

#### CRS definitions

Sno lets you define arbitrary CRS definitions and attach them to your dataset. By contrast, SQL Server comes pre-installed with hundreds of the standard EPSG & ESRI coordinate reference system definitions. However, these cannot be modified, and custom CRS cannot be added.

This mismatch has the following consequence: the only part of the CRS that Sno is tracking that can be written to a SQL Server working copy is the numeric part of the CRS authority code (referred to in [SQL Server documentation](https://docs.microsoft.com/sql/relational-databases/system-catalog-views/sys-spatial-reference-systems-transact-sql) as the `spatial_reference_id` or `SRID`). This code will be embedded in each geometry.

Since SQL Server has support for a limited number of CRS, it is possible that the SRID associated with your geometry will not be one that SQL Server recognizes. However, this is of very little consequence since SQL Server doesn't make much use of the SRID for the Geometry type (as opposed to Geography type), and Sno working copies currently only contain Geometry types. See the [SQL Server documentation](https://docs.microsoft.com/sql/relational-databases/spatial/spatial-data-types-overview). It is much more important to make sure that the application you use to view and edit your SQL Server working copy is able to extract and understand the CRS code and so display the data correctly.

Since the CRS is not stored as part of the geometry column's type information in SQL Server, it is also not possible to change which CRS is applied to a geometry column by editing the SQL Server working copy - instead it must be done using either a different type of working copy, or the Sno command line tool. More documentation will be added here when this change is better supported.
