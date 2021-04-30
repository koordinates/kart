MySQL Working Copy
-----------------------

In order to use a [MySQL](https://www.mysql.com/) working copy, you need to have a server running MySQL 8.0 or later. (MySQL 5.6 and later are largely compatible but not officially supported).

### MySQL partitioning

MySQL servers are designed so that they can be used for multiple apps simultaneously without those apps interfering with each other. This is usually achieved by storing data from different apps in different databases.

* A MySQL server contains one or more named databases, which in turn contain tables. A user connected to the server can query tables in any database they have access-rights to without starting a new connection. Two tables can have the same name, as long as they are in different databases.

MySQL has only a single layer of data separation - the *database*. (Contrast to [PostgreSQL](POSTGIS_WC.md) and [Microsoft SQL Server](SQL_SERVER_WC.md) which have two layers, *database* and *schema*). A Kart MySQL working copy can share a server with any other app, but it expects to be given its own database to manage (just as Kart expects to manage its own GPKG working copy, not share it with data from other apps). Managing the database means that Kart is responsible for initialising that database and importing the data in its initial state, then keeping track of any edits made to that data so that they can be committed. Kart expects that the user will use some other application to modify the data in that database as part of making edits to a Kart working copy.

This approach differs from other working copy types that only manage a single *schema* within a database.

### MySQL Connection URI

A Kart repository with a MySQL working copy needs to be configured with a `mysql://` connection URI. This URI contains how to connect to the server, and the name of the database that should be managed as a working copy by this Kart repository.

Kart needs a connection URL in the following format:

`mysql://[user[:password]@][host][:port]/dbname`

For example, a Kart repo called `airport` might have a URL like the following:

`mysql://kart_user:password@localhost:1433/airport_kart`

To configure a Kart repository to use a particular MySQL database as its working copy, specify the `--workingcopy` flag when creating the repository, for example:

`kart init --workingcopy=mysql://... --import=...`

The database that Kart is given to manage should be either non-existent or empty at the time Kart is configured, but the server should already be running.

The database user needs to have full rights to modify objects in the specified database. (eg: via `GRANT ALL PRIVILEGES ON airport_kart.* TO kart_user; FLUSH PRIVILEGES;`).

### MySQL limitations

Most geospatial data can be converted to MySQL format without losing any fidelity, but it does have the following limitations.

#### Three and four dimensional geometries

Geometries in MySQL are always two-dimensional (meaning they have an X and a Y co-ordinate, or a longitude and a latitude co-ordinate). Three- or four-dimensional geometries, with Z (altitude) or M (measure) co-ordinates, are not supported in MySQL. As a result, Kart datasets containing three- and four-dimensional geometries cannot currently be checked out as MySQL working copies.

#### Approximated types

There is one type that Kart supports that has no MySQL equivalent - the `interval`. This type is approximated as `TEXT` in the MySQL working copy. See [APPROXIMATED_TYPES](APPROXIMATED_TYPES.md) for more information.

#### CRS definitions

MySQL comes pre-installed with thousands of standard EPSG coordinate reference system definitions. Currently, only the CRS definitions that are already in your MySQL installation are supported - Kart will not create definitions in MySQL to match the custom definitions attached to your Kart datasets. More documentation will be added here when this is supported.
