SQL Server Working Copy
-----------------------

In order to use a `Microsoft SQL Server <sql_server_docs_>`_ working copy, you
need to have a SQL Server running. SQL Server 2016 and later is
officially supported by Kart (SQL Server 2012 and later are largely
compatible but not officially supported).

You also need to have the `Microsoft ODBC Driver for SQL Server <sql_server_odbc_docs_>`_
installed on your system.

SQL Server partitioning
~~~~~~~~~~~~~~~~~~~~~~~

SQL Server databases are designed so that they can be used for multiple
apps simultaneously without those apps interfering with each other -
they have multiple levels of data separation.

-  A SQL Server contains one or more named databases. When a user
   connects to the server, they must specify up front which database
   they need, and then they can only access data in this database.
-  A single database contains one or more named schemas, which in turn
   contain tables. A user connected to the database can query tables in
   any schema they have access-rights to without starting a new
   connection. Two tables can have the same name, as long as they are in
   different schemas.

So SQL Server has a partition called a "schema" - the name can be
confusing as "schema" can also have other meanings, but in this case it
means a namespace. A Kart SQL Server working copy can share a server or
a database with any other app, but it expects to be given its own schema
to manage (just as Kart expects to manage its own GPKG working copy, not
share it with data from other apps). Managing the schema means that Kart
is responsible for initialising that schema and importing the data in
its initial state, then keeping track of any edits made to that data so
that they can be committed. Kart expects that the user will use some
other application to modify the data in that schema as part of making
edits to a Kart working copy.

SQL Server Connection URI
~~~~~~~~~~~~~~~~~~~~~~~~~

A Kart repository with a SQL Server working copy needs to be configured
with a ``mssql://`` connection URI. This URI contains how to connect to
the server, the name of the database to connect to (which can be shared
with other apps), and the name of the schema that should be managed as a
working copy by this Kart repository.

Kart needs a connection URL in the following format:

``mssql://[user[:password]@]host[:port]/dbname/dbschema``

For example, a Kart repo called ``airport`` might have a URL like the
following:

``mssql://kart_user:password@localhost:1433/gis/airport_kart``

To configure a Kart repository to use a particular SQL Server schema as
its working copy, specify the ``--workingcopy`` flag when creating the
repository, for example:

``kart init --workingcopy=mssql://... --import=...``

The schema that Kart is given to manage should be either non-existent or
empty at the time Kart is configured, but the server and database should
already exist.

The database user needs to have full rights to modify objects in the
specified schema. (eg: via
``GRANT CONTROL ON SCHEMA airport_kart TO kart_user;``).

Kart limitations - Geometry and Geography types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

SQL Server has two different spatial data types, called ``Geometry`` and
``Geography``. See the SQL Server `spatial data types
documentation <sql_server_spatial_data_types_>`_.
Kart only has one spatial data type, called ``geography``. At present,
Kart ``geography`` data is always written to the working copy in a SQL
Server ``Geography`` column, but a future release of Kart will give the
user the option to configure if they want ``Geometry`` or ``Geography``.
The SQL Server ``Geometry`` type treats geometries as if they lie on a
flat plane (rather than on the ellipsoidal surface of the Earth), so
using SQL Server functions such as ``STDistance`` or ``STArea`` that
calculate distance or area of the geometries will not give the
real-world answer if the geometries describe features on the surface of
the Earth. Although the appropriate CRS ID remains attached to each
``Geometry`` instance, SQL Server doesn't use it at all to do these
flat-plane geometry calculations.

If you need to use SQL Server's ``Geography`` functions on data in your
Kart working copy so that the calculations give the correct answers as
modeled on the ellipsoidal surface of the Earth, you can convert the
``Geometry`` instances to ``Geography`` instance before doing the
calculations. For instance, instead of executing the following query to
find the area of features in column ``geom``:

.. code:: sql

   SELECT geom.STArea() FROM my_table;`

you would instead execute a query that first converts to ``Geography``:

.. code:: sql

   SELECT geography::STGeomFromWKB(geom.STAsBinary(), 4326).STArea() FROM my_table;`

SQL Server limitations
~~~~~~~~~~~~~~~~~~~~~~

Almost all geospatial data can be converted to SQL Server format without
losing any fidelity, but it does have the following limitations.

Approximated types
^^^^^^^^^^^^^^^^^^

There is one type that Kart supports that has no SQL Server equivalent -
the ``interval``. This type is approximated as ``NVARCHAR`` in the SQL
Server working copy. See :ref:`Approximated Types`
for more information.

Unconstrained geometry types
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Kart lets you define a column as containing only a particular type of
geometry, eg only ``POINT`` or only ``MULTIPOLYGON`` types. By contrast,
SQL Server lets you put any type of geometry into a geometry column.

This mismatch has the following consequence: If Kart is managing a
geometry column with a particular geometry type such as ``POINT``, and
you check it out in a SQL Server working copy, you will be able to
insert other types of geometry into it, but Kart will prevent you from
committing it. You still need to follow the constraint put in place when
the dataset was created, and only insert new geometries of the
appropriate type.

If you need decide that a certain dataset should contain more types of
geometries than its constraint currently allows, it is possible to
change a columns geometry type to be broader and allow more types. This
cannot be done by editing the SQL Server working copy, since as noted it
doesn't store this type information - instead it must be done using
either a different type of working copy, or the Kart command line tool.
To use the command line, take the following steps:

1. View all the metadata for your dataset: ``kart meta get DATASET``
2. Copy the JSON from under the heading ``schema.json`` and save it to a
   file of the same name in your current working directory.
3. Modify the JSON so that the ``geometryType`` property is broader. For
   example:

   -  Old line: ``"geometryType": "POINT",``
   -  New line: ``"geometryType": "GEOMETRY",``

4. Commit this change to the schema:
   ``kart meta set DATASET schema.json=@schema.json``

CRS definitions
^^^^^^^^^^^^^^^

Kart lets you define arbitrary CRS definitions and attach them to your
dataset. By contrast, SQL Server comes pre-installed with hundreds of
standard EPSG coordinate reference system definitions. However, these
cannot be modified, and custom CRS cannot be added.

This mismatch has the following consequence: the only part of the CRS
that Kart is tracking that can be written to a SQL Server working copy
is the numeric part of the CRS authority code (referred to in `SQL
Server
documentation <sys_spatial_reference_systems_transact_sql_>`_
as the ``spatial_reference_id`` or ``SRID``). This code will be embedded
in each geometry.

Since SQL Server has support for a limited number of CRS, it is possible
that the SRID associated with your geometry will not be one that SQL
Server recognizes. However, this is of very little consequence since SQL
Server doesn't make much use of the SRID for the Geometry type (as
opposed to Geography type), and Kart working copies currently only
contain Geometry types. See the `SQL Server
documentation <sql_server_spatial_data_types_>`_.
It is much more important to make sure that the application you use to
view and edit your SQL Server working copy is able to extract and
understand the CRS code and so display the data correctly.

It is possible to modify the CRS definition attached to a particular
geometry column by editing the code embedded in every geometry in that
column. This change can be committed as long as the new CRS you have
chosen is one that is built into SQL Server.
