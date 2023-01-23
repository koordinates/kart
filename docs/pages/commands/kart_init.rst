Creating a repository
=====================

A repository is a version controlled data store. It exists as a
filesystem directory, which contains the versioned data, the current
revision, a log of changes, etc. It is highly recommended that you do
not manually edit the contents of the repository directory.

Create a Repository from a GeoPackage or Postgres Database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``kart init --import <source> [<repository>]``

This command creates a new repository and imports all tables from the
given database. (For more fine grained control, use ``kart init`` to
create an empty repository, and then use ``kart import``.)

-  ``<source>``: Path to the
   `GeoPackage <http://www.geopackage.org>`_, or `PostgreSQL Connection URI <https://www.postgresql.org/docs/current/libpq-connect.html#id-1.7.3.8.3.6>`_
   to be imported.
-  ``<repository>`` Path to the directory where the repository will be
   created. If not specified, defaults to the current directory.

.. code:: bash

   kart init   # init empty repository
   kart init --import my-data-store.gpkg
   kart init --import my-data-store.gpkg ./my-new-repository/
   kart init --import postgresql://username:password@hostname/databasename
