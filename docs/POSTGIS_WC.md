PostGIS Working Copy
--------------------

In order to use a [PostGIS](https://postgis.net/) working copy, you need to have a PostgreSQL database server with the PostGIS extension installed. PostGIS 3.0 and later is officially supported by Sno (versions 2.0 and later are largely compatible but not officially supported)

### PostgreSQL partitioning

PostgreSQL databases are designed so that they can be used for multiple tasks simultaneously without those tasks interfering with each other - they have multiple levels of data separation.

* A single server hosts a PostgreSQL database cluster.
* A database cluster contains one or more named databases. When a user connects to the server, they must specify up front which database they need, and then they can only access data in this database.
* A single database contains one or more named schemas, which in turn contain tables. A user connected to the database can query tables in any schema they have access-rights to without starting a new connection. Two tables can have the same name, as long as they are in different schemas.

So PostgreSQL has a partition called a "schema" - the name can be confusing as "schema" can also have other meanings, but in this case it means a namespace. A Sno PostGIS working copy is fine to share a database cluster or a database with any other task, but it expects to be given its own schema to manage (just as Sno expects to manage its own GPKG working copy, not share it with other data). Managing the schema means that Sno is responsible for initialising that schema and importing the data in its initial state, then keeping track of any edits made to that data so that they can be committed. Sno expects that the user will use some other application to modify the data in that schema as part of making edits to a Sno working copy.

### PostgreSQL Connection URI

A Sno repository with a PostGIS working copy needs to be configured with a `postgresql://` connection URI. This URI contains how to connect to the database cluster, the name of the database to connect to (which can be shared with other tasks), and the name of the schema that should be managed as a working copy by this Sno repository.

From the [PostgreSQL documentation](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING), a connection URL has the following format:

`postgresql://[user[:password]@][host][:port][/dbname]`

Since Sno also requires the schema to be specified up front, Sno needs a connection URL in the following format:

`postgresql://[user[:password]@][host][:port]/dbname/dbschema`

For example, a Sno repo called `airport` might have a URL like the following:

`postgresql://sno_user:password@localhost:5432/gis/airport_sno`

To configure a Sno repository to use a particular PostGIS schema as its working copy, specify the `--workingcopy` flag when creating the repository, for example:

`sno init --workingcopy=postgresql://... --import=...`

The schema that Sno is given to manage should be either non-existent or empty at the time Sno is configured, but the database cluster and database should already exist.

The database user needs to have full rights to modify objects in the specified schema. (eg: via `GRANT ALL ON SCHEMA airport_sno TO sno_user;`). As with `psql`, if no user or password is explicitly specified in the URL, the `PGUSER` and `PGPASSWORD` environment variables are consulted.

### PostGIS limitations

Almost all geospatial data can be converted to PostGIS format without losing any fidelity, but it does have the following limitations.

#### Approximated types

There is one type that Sno supports that has no PostGIS equivalent - an 8-bit integer. This type is "approximated" as a `SMALLINT` (which has 16 bits) in the PostGIS working copy. See [APPROXIMATED_TYPES](APPROXIMATED_TYPES.md] for more information.

#### CRS definitions

The PostGIS extension comes pre-installed with thousands of the standard EPSG & ESRI coordinate reference system definitions. Although these are generally produced from official sources, unfortunately different vendors or products might have slightly different variations of them with respect to axis ordering, naming, authority codes, or other differences.

Sno has some design goals that make CRS management slightly more complicated in a PostGIS working copy:

* Sno doesn't want to interfere with the CRS definitions that come pre-installed in PostGIS, since these are shared by all database users - it would be unhelpful if they were forever being modified in minor ways by different users, instead software should try and use the standard.
For this reason, Sno doesn't take the CRS from the dataset and overwrite the pre-installed CRS in the PostGIS database.
* Sno doesn't want commit changes that only exist due to working copy limitations, as opposed to changes the user has made explicitly. A user might create a PostGIS working copy just to change one piece of data - they shouldn't accidentally end up committing the PostGIS version of any CRS definitions that the data is using. It would be unhelpful if every type of working copy that was used to make a commit, caused the dataset CRS definitions to be modified to a different version of the standard. For this reason, Sno doesn't take the CRS from the working copy and overwrite the CRS in the dataset.

The end result is that the standard CRS definitions are "approximated" - just as 8-bit integers in the sno dataset are approximated by 16-bit integers in the PostGIS working copy, standard CRS definitions are approximated too - for instance `EPSG:4326` as it is defined in the dataset, is approximated by `EPSG:4326` however it is defined in the working copy. These may differ slightly, but because it is an officially defined CRS, they shouldn't differ in any meaningful way. The difference between these two definitions is not shown when running `sno status` to see uncommitted changes, and the changed definition will not be committed.

In the case that you want to replace the working copy definition with the one from the dataset, manually delete the appropriate definition from the working copy and then run `sno reset` to rewrite the relevant part of your working copy.

For CRS definitions that are not considered standard, Sno works exactly as it does with a GPKG working copy - checkout of a working copy will write the relevant CRS definitions from the dataset to the working copy, and if those CRS definitions are then changed locally, these changes will show up in `sno status` and can be committed back to the dataset.

CRS definitions are considered standard if they have an authority of "EPSG" or "ESRI".
