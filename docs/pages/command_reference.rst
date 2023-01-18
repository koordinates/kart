Command Reference
==================

Creating Repositories
---------------------

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
   `GeoPackage <gpkg_>`_, or `PostgreSQL Connection URI <postgres_conn_>`_
   to be imported.
-  ``<repository>`` Path to the directory where the repository will be
   created. If not specified, defaults to the current directory.

.. code:: bash

   kart init   # init empty repository
   kart init --import my-data-store.gpkg
   kart init --import my-data-store.gpkg ./my-new-repository/
   kart init --import postgresql://username:password@hostname/databasename

Import into Existing Repository
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``kart import <source> [<table>] [<table>]``

This command imports one or more tables into the kart repository in the
working directory. If no tables are provided at the command line, the
command will prompt the user to select one from the source database. If
tables should be imported with a different name, this is done by
providing a table specification like so:
``<table_to_import>:<new_name_for_table>``

.. code:: bash

   kart import my-data-store.gpkg
   kart import my-data-store.gpkg 2019_08_06_median_waterlevel
   kart import my-data-store.gpkg 2019_08_06_median_waterlevel:waterlevel

Data can be imported from any of the following types of databases:

- `GeoPackage <gpkg_>`_
- `PostGIS <postgis_>`_
- `SQLServer <sql_server_>`_
- `MySQL <mysql_>`_
- `Shapefiles <shapefiles_>`_

The following syntax examples show how to import from each type of
database.

.. code:: bash

   kart import PATH-TO-FILE.gpkg table_1 table_2 table_3
   kart import postgresql://USERNAME:PASSWORD@HOST/DBNAME/DBSCHEMA table_1 table_2 table_3
   kart import mssql://USERNAME:PASSWORD@HOST/DBNAME/DBSCHEMA table_1 table_2 table_3
   kart import mysql://USERNAME:PASSWORD@HOST/DBNAME/DBSCHEMA table_1 table_2 table_3
   kart import PATH-TO-FILE.shp

You can also specify ``--all-tables`` to import all tables from a
particular datasource.

Clone Existing Repositories
~~~~~~~~~~~~~~~~~~~~~~~~~~~

``kart clone <repository> [<directory>]``

Copies a repository into a newly created directory and automatically
adds remotes to the source repository.

-  ``<repository>`` The remote repository to clone from.
-  ``<directory>`` The directory for the new repository. Defaults to the
   last path component of the cloned repository.



Managing Working Copies
-----------------------

A working copyis a snapshot of the data contained in the
repository. The working copy of the data is the data you can view & edit
in your GIS tool of choice. By default, Kart creates a GeoPackage
working copy in your Kart repository as soon as there is some data in
your repository.

Kart supports different types of working copy, but a Kart repository can
only have one type of working copy at a time. The supported types are
currently:

- `GeoPackage <gpkg_>`_
- `PostGIS <postgis_>`_
- `SQLServer <sql_server_>`_
- `MySQL <mysql_>`_

Creating different types of Working Copy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can choose the type of working copy you want during an init or clone
operation, eg:

-  ``kart init --workingcopy=PATH.gpkg``
-  ``kart init --workingcopy=postgresql://USERNAME:PASSWORD@HOST/DBNAME/DBSCHEMA``
-  ``kart init --workingcopy=mssql://USERNAME:PASSWORD@HOST/DBNAME/DBSCHEMA``
-  ``kart init --workingcopy=mysql://USERNAME:PASSWORD@HOST/DBNAME``

Or you can change working copy type in an existing repository using
``kart create-workingcopy``:

-  ``kart create-workingcopy PATH.gpkg``

… and so on.

Query the State of a Working Copy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``kart status``

Displays the current branch state and lists the number of uncommitted
additions, modifications and deletions in the current working copy.

Example
^^^^^^^

.. code:: bash

   $ kart status
   On branch master

   Changes in working copy:
     (use "kart commit" to commit)
     (use "kart reset" to discard changes)

     my_layer/
       modified:   4 features
       new:        8 features
       deleted:    11 features

Viewing Changes
~~~~~~~~~~~~~~~

``kart diff [<commit_spec>] [<dataset>[:<pk>]]...``

Shows the diff between two commits, or between one commit and the
working copy.

-  ``<commit_spec>``:

   -  If not supplied, this defaults to ``HEAD``, so that this command
      shows the diff between ``HEAD`` and the working copy.
   -  If supplied with a single commit, ie ``<commit-A>`` then this
      command shows the diff between ``commit-A`` and the working copy.
   -  If supplied with the form ``<commit-A>...<commit-B>`` then this
      command shows the diff between ``commit-A`` and ``commit-B``.
   -  If supplied with the form ``<commit-A>..<commit-B>`` then this
      command shows the diff between
      ``the common ancestor of commit-A and commit-B``, and
      ``commit-B``.

-  ``<dataset>`` or ``<dataset>:<pk>`` Only show changes in this
   dataset, or to this feature. This argument can be supplied multiple
   times to view multiple changes at once - if it is not supplied, all
   changes in the repository are shown.

Commit Changes to a Working Copy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``kart commit [-m "<message>"] [<dataset>[:<pk>]]...``

Commits any changes that have been made to the working copy that have
not yet been committed.

-  ``-m <message>`` ``--message <message>`` The text message to
   associate with this commit.

-  ``<dataset>`` or ``<dataset>:<pk>`` Only commit changes from this
   dataset, or only commit changes of this feature. This argument can be
   supplied multiple times to commit multiple changes at once - if it is
   not supplied, all changes in the repository are committed.

Roll Back Changes to a Working Copy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``kart reset HEAD``

Resets any changes (additions, deletions, modifications) in the working
copy to the state of the last commit (HEAD).

Branching and Merging
---------------------

Create a Branch
~~~~~~~~~~~~~~~

``kart checkout -b <branch>``

Creates a new branch, and switches the current working copy to the the
new branch.

-  ``-b <branch>`` Specifies the name of the new branch.

Delete a Branch
~~~~~~~~~~~~~~~

``kart branch -d <branch>``

Deletes the branch.

Merge a Branch
~~~~~~~~~~~~~~

Merging two branches combines the changes from both branches to produce
a new state and history.

``kart merge <branch>``

-  ``<branch>`` The branch to merge into the current branch.

Resolving Conflicts
-------------------

Sometimes a merge cannot be performed automatically because changes in
one branch conflict with changes in the other branch. These conflicts
must instead be manually resolved. Tools to make this process easier are
still in development.

.. code:: bash

   $ kart merge my_work
   Merging branch "my_work" into master
   Conflicts found:

   my_layer:
       my_layer:feature: 3 conflicts

   Repository is now in "merging" state.
   View conflicts with `kart conflicts` and resolve them with `kart resolve`.
   Once no conflicts remain, complete this merge with `kart merge --continue`.
   Or use `kart merge --abort` to return to the previous state.

When a merge cannot be applied cleanly, the repository is moved into a
merging state. The merge must be completed by resolving all conflicts
before any other work can be done in the repository - if now is not a
good time to work on this, then the merge can be abandoned for the time
being with ``kart merge --abort``.

Useful commands during merging state:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

   kart status            # Shows whether the repository is in merging state
   kart merge --abort     # Abandons the merge, returns to the previous state
   kart conflicts         # Shows conflicts that must be resolved to complete the merge
   kart resolve ...       # Resolves one conflict
   kart merge --continue  # Completes the merge once no more conflicts remain

Viewing Conflicts
~~~~~~~~~~~~~~~~~

``kart conflicts [<dataset>[:<pk>]]...``

-  ``<dataset>`` or ``<dataset>:<pk>`` Only view conflicts involving
   this dataset, or only view conflicts involving this feature. This
   argument can be supplied multiple times to view multiple conflicts at
   once - if it is not supplied, all conflicts in the repository are
   shown.

Resolving a Conflict
~~~~~~~~~~~~~~~~~~~~

``kart resolve <conflict> --with=<resolution>``

-  ``<conflict>`` The name of the the conflict to resolve - generally
   takes the form ``<layer>:feature:<feature_id>``, but more complicated
   conflicts can have more complicated names. The names of all conflicts
   are available by running ``kart conflicts``

-  ``--with=<resolution>`` The resolution here must be one of
   ``ancestor``, ``ours``, ``theirs``, or ``delete``, which resolve the
   conflict in the following manner:

   -  ``ancestor`` resolve the conflict by accepting the ancestor
      version, essentially undoing both changes that conflict
   -  ``ours`` resolve the conflict by accepting our version, keeping
      our change but discarding their change
   -  ``theirs`` resolve the conflict by accepting their version,
      keeping their change but discarding our change
   -  ``delete`` resolve the conflict by deleting the item which has
      conflicting changes

These resolutions are the only resolutions that can be selected by name.
Other resolutions are also possible, but must be supplied in a file.

``kart resolve <conflict> --with-file=<resolution.geojson>``

-  ``<resolution.geojson>`` This must be a geojson file containing one
   or more features. The order of the features in the file is
   unimportant, as is any "id" field on the GEOJson feature object
   itself - all relevant information is read from the "geometry" and
   "properties" fields, including primary keys.

.. code:: bash

   $ kart resolve my_layer:feature:15 --with-file=15_merged.geojson
   Resolved 1 conflict. 0 conflicts to go.

   $ kart merge --continue
   Merging branch "my_work" into master
   No conflicts!
   Merge committed as 2a645ba3987625b723f0f4bc406e7da877bd30c2
