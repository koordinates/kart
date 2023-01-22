Geopackage Working Copy
-----------------------

The default format for the tabular part of the working copy of a Kart
repository is a `GeoPackage <gpkg_>`_, or GPKG. Creating a Kart
repository and importing data will generally cause a GPKG working copy
to be created automatically in the same directory as the Kart
repository, unless you specify a different working copy configuration.
To explicitly choose where a GPKG working copy is created or what it is
called, specify the ``--workingcopy`` flag when creating the repository,
for example:

``kart init --workingcopy=example.gpkg --import=...``

GPKG limitations
~~~~~~~~~~~~~~~~

Most geospatial data can be converted to GPKG format without losing any
fidelity, but it does have the following limitations.

Primary keys
^^^^^^^^^^^^

GPKG requires integer primary keys for tables that have a geometry
column. Other tables do not have this constraint `[GPKG
spec] <gpkg_user_tables_>`_. Kart
follows these requirements as described below:

-  | **Integer primary keys**
   | Integer primary keys are preserved fully and function normally in a
     GPKG working copy.

-  | **String primary keys** (or any other non-integer type)
   | If the dataset **does not** have a geometry column, then these
     primary keys are checked out without modification.
   | If the dataset **does** have a geometry column, then the primary
     key column is demoted to a regular column (but constrained to be
     unique and not-null), and another column is added to the table that
     is an integer primary key column, for the sole purpose of making
     the table conform to the GPKG specification. The additional integer
     column is called ``auto_int_pk``. It is not part of the dataset,
     its contents are arbitrary (except that they conform to the GPKG
     requirements), and it is not tracked by Kart - any edits
     specifically to this column will not be committed.

-  | **No primary key column**
   | Importing data without primary keys is supported, a primary key
     will be added to each row automatically as it is imported. More
     documentation on importing data without primary keys will be added
     soon.

-  | **Composite primary keys** (multiple primary key columns)
   | This is not yet supported.

No type safety
^^^^^^^^^^^^^^

A GPKG file is implemented as a `SQLite <sqlite_>`_ database. SQLite is
extremely lax about types, treating all of them as suggestions rather
than rules to be enforced - for example, it is possible to store a
string in a field of type integer. Because of this, it is possible to
create a GPKG working copy that contains data that doesn't conform to
it's schema. Kart tries to prevent you from committing changes that
don't conform to the schema, because Kart is interoperable with other
data formats where there is stricter type checking. If you were able to
commit strings in an integer field, that would prevent another
contributor from checking out the same dataset in, for instance, a
PostGIS working copy. So, when editing data, be mindful that you keep it
as the right type, and be aware that if you fail to do so you will get a
``Schema violation`` error when you try to commit.

Approximated types
^^^^^^^^^^^^^^^^^^

According to the `GPKG Specification <gpkg_>`_, a valid GPKG must
contain only the following types:

-  ``BOOLEAN``
-  ``BLOB``
-  ``DATE``
-  ``DATETIME``
-  ``DOUBLE``
-  ``FLOAT``
-  ``INTEGER`` (int64)
-  ``MEDIUMINT`` (int32)
-  ``REAL``
-  ``SMALLINT`` (int16)
-  ``TEXT``
-  ``TINYINT`` (int8)

There are three types that Kart supports that do not have an equivalent
on this list: ``interval``, ``numeric``, and ``time``. These types are
"approximated" as ``TEXT`` in the GPKG working copy. See
:doc:`Approximated Types </pages/development/approximated_types>` for more information.
SQLite has a series of `date and time
functions <sqlite_date_and_time_>`_ available which
work with ISO8601 date and time strings.
