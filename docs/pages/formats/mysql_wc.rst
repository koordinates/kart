MySQL Working Copy
------------------

In order to use a `MySQL <mysql_>`_ working copy, you
need to have a server running MySQL 8.0 or later.

MySQL partitioning
~~~~~~~~~~~~~~~~~~

MySQL servers are designed so that they can be used for multiple apps
simultaneously without those apps interfering with each other. This is
usually achieved by storing data from different apps in different
databases.

-  A MySQL server contains one or more named databases, which in turn
   contain tables. A user connected to the server can query tables in
   any database they have access-rights to without starting a new
   connection. Two tables can have the same name, as long as they are in
   different databases.

MySQL has only a single layer of data separation - the *database*.
(Contrast to :doc:`PostgreSQL </pages/formats/postgis_wc>` and
:doc:`Microsoft SQLServer </pages/formats/mysql_wc>` which have two layers,
*database* and *schema*). A Kart MySQL working copy can share a server with any other
app, but it expects to be given its own database to manage (just as Kart
expects to manage its own GPKG working copy, not share it with data from
other apps). Managing the database means that Kart is responsible for
initialising that database and importing the data in its initial state,
then keeping track of any edits made to that data so that they can be
committed. Kart expects that the user will use some other application to
modify the data in that database as part of making edits to a Kart
working copy.

This approach differs from other working copy types that only manage a
single *schema* within a database.

MySQL Connection URI
~~~~~~~~~~~~~~~~~~~~

A Kart repository with a MySQL working copy needs to be configured with
a ``mysql://`` connection URI. This URI contains how to connect to the
server, and the name of the database that should be managed as a working
copy by this Kart repository.

Kart needs a connection URL in the following format:

``mysql://[user[:password]@]host[:port]/dbname``

For example, a Kart repo called ``airport`` might have a URL like the
following:

``mysql://kart_user:password@localhost:1433/airport_kart``

To configure a Kart repository to use a particular MySQL database as its
working copy, specify the ``--workingcopy`` flag when creating the
repository, for example:

``kart init --workingcopy=mysql://... --import=...``

The database that Kart is given to manage should be either non-existent
or empty at the time Kart is configured, but the server should already
be running.

The database user needs to have full rights to modify objects in the
specified database. (eg: via
``GRANT ALL PRIVILEGES ON airport_kart.* TO kart_user; FLUSH PRIVILEGES;``).

MySQL limitations
~~~~~~~~~~~~~~~~~

Most geospatial data can be converted to MySQL format without losing any
fidelity, but it does have the following limitations.

Three and four dimensional geometries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Geometries in MySQL are always two-dimensional (meaning they have an X
and a Y co-ordinate, or a longitude and a latitude co-ordinate). Three-
or four-dimensional geometries, with Z (altitude) or M (measure)
co-ordinates, are not supported in MySQL. As a result, Kart datasets
containing three- and four-dimensional geometries cannot currently be
checked out into MySQL working copies.

Approximated types
^^^^^^^^^^^^^^^^^^

There is one type that Kart supports that has no MySQL equivalent - the
``interval``. This type is approximated as ``TEXT`` in the MySQL working
copy. See :ref:`Approximated Types`
for more information.

CRS definitions
^^^^^^^^^^^^^^^

MySQL comes pre-installed with thousands of standard EPSG coordinate
reference system definitions. Although these are generally produced from
official sources, unfortunately different vendors or products might have
slightly different variations of them with respect to axis ordering,
naming, authority codes, or other differences.

Kart has some design goals that make CRS management slightly more
complicated in a MySQL working copy:

-  Kart doesn't want to interfere with the CRS definitions that come
   pre-installed in MySQL, since these are shared by all database users
   - it would be unhelpful if they were forever being modified in minor
   ways by different users, instead software should try and use the
   standard. For this reason, Kart doesn't take the CRS from the dataset
   and overwrite the pre-installed CRS in MySQL.
-  Kart doesn't want commit changes that only exist due to working copy
   limitations, as opposed to changes the user has made explicitly. A
   user might create a MySQL working copy just to change one piece of
   data - they shouldn't accidentally end up committing the MySQL
   version of any CRS definitions that the data is using. It would be
   unhelpful if every type of working copy that was used to make a
   commit, caused the dataset CRS definitions to be modified to a
   different version of the standard. For this reason, Kart doesn't take
   the CRS from the working copy and overwrite the CRS in the dataset.

The end result is that the standard CRS definitions are "approximated" -
for instance ``EPSG:4326`` as it is defined in the dataset, is
approximated by ``EPSG:4326`` however it is defined in the working copy.
These may differ slightly, but because it is an officially defined CRS,
they shouldn't differ in any meaningful way. The difference between
these two definitions is not shown when running ``kart status`` to see
uncommitted changes, and the changed definition will not be committed.

In the case that you want to replace the working copy definition with
the one from the dataset, manually delete the appropriate definition
from the working copy and then run ``kart reset`` to rewrite the
relevant part of your working copy.

For CRS definitions that are not considered standard, Kart works exactly
as it does with a GPKG working copy - checkout of a working copy will
write the relevant CRS definitions from the dataset to the working copy,
and if those CRS definitions are then changed locally, these changes
will show up in ``kart status`` and can be committed back to the
dataset.

CRS definitions are considered standard in MySQL if they have an
authority of "EPSG".
