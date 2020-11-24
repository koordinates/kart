Geopackage Working Copy
-----------------------

The default format for the working copy of a Sno repository is a [Geopackage](http://www.geopackage.org/), or GPKG. Creating a Sno repository and importing data will generally cause a GPKG working copy to be created automatically in the same directory as the Sno repository, unless you specify a different working copy configuration. To explicitly choose where a GPKG working copy is created or what it is called, specify the `--workingcopy` flag when creating the repository, for example:

`sno init --workingcopy=example.gpkg --import=...`

### GPKG limitations

Most geospatial data can be converted to GPKG format without losing any fidelity, but it does have the following limitations.

#### Primary keys

GPKG requires integer primary keys for tables that have a geometry column. Other tables do not have this constraint [[GPKG spec]](http://www.geopackage.org/spec120/#feature_user_tables). Sno follows these requirements as described below:

 * **Integer primary keys** \
     Integer primary keys are preserved fully and function normally in a GPKG working copy.

 * **String primary keys** (or any other non-integer type) \
     If the dataset **does not** have a geometry column, then these primary keys are checked out without modification. \
     If the dataset **does** have a geometry column, then the primary key column is demoted to a regular column (but constrained to be unique and not-null), and another column is added to the table that is an integer primary key column, for the sole purpose of making the table conform to the GPKG specification. The additional integer column is called `.sno-auto-pk`. It is not part of the dataset, its contents are arbitrary (except that they conform to the GPKG requirements), and it is not tracked by Sno - any edits specifically to this column will not be committed.

 * **No primary key column** \
   This is not yet supported.

 * **Composite primary keys** (multiple primary key columns) \
   This is not yet supported.

Work in this area is ongoing, expect to see support for data without primary keys as Sno development continues.

Tracking bug for data without primary keys: [#212](https://github.com/koordinates/sno/issues/212)

#### No type safety

A GPKG file is implemented as a [SQLite](https://www.sqlite.org/index.html) database. SQLite is extremely lax about types, treating all of them as suggestions rather than rules to be enforced - for example, it is possible to store a string in a field of type integer. Because of this, it is possible to create a GPKG working copy that contains data that doesn't conform to it's schema. Sno tries to prevent you from committing changes that don't conform to the schema, because Sno is interoperable with other data formats where there is stricter type checking. If you were able to commit strings in an integer field, that would prevent another contributor from checking out the same dataset in, for instance, a PostGIS working copy. So, when editing data, be mindful that you keep it as the right type, and be aware that if you fail to do so you will get a `Schema violation` error when you try to commit.

#### Approximated types

According to the [GPKG specification](http://www.geopackage.org/spec/), a valid GPKG must contain only the following types:

* `BOOLEAN`
* `BLOB`
* `DATE`
* `DATETIME`
* `DOUBLE`
* `FLOAT`
* `INTEGER` (int64)
* `MEDIUMINT` (int32)
* `REAL`
* `SMALLINT` (int16)
* `TEXT`
* `TINYINT` (int8)

There are three types that Sno supports that do not have an equivalent on this list: `interval`, `numeric`, and `time`. These types are "approximated" as `TEXT` in the GPKG working copy while keeping their original type in the Sno dataset. Sno creates a column of type `TEXT` in the appropriate place in the GPKG, and when the working copy is committed, Sno acts as if those columns have the original type (eg `interval`) instead of their actual type, `TEXT`. Since the change from (eg) `interval` to `TEXT` is just a limitation of the working copy, this apparent change in type will not show up as a change in the commit log, nor will it show up as an uncommitted change if you run `sno status` to see what local changes you have made but not yet committed.
