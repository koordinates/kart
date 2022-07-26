# Kart changelog

Please note that compatibility for 0.x releases (software or repositories) isn't guaranteed. Kart is evolving quickly and things will change. However, we aim to provide the means to upgrade existing repositories between 0.x versions and to 1.0.

_When adding new entries to the changelog, please include issue/PR numbers wherever possible._

## 0.11.4 (UNRELEASED)

- Add man-style pagers instead of click's default help page on `kart <command> --help`. [#643](https://github.com/koordinates/kart/issues/643)
- Support tab completion in powershell for kart. [#643](https://github.com/koordinates/kart/issues/643)
- Add `--install-tab-completion` option for installing shell completion. [#643](https://github.com/koordinates/kart/issues/643)
- Add tab completion for kart commands and user-specific data. [#643](https://github.com/koordinates/kart/issues/643)
- Changed format of feature IDs in GeoJSON output to be more informative and consistent. [#135](https://github.com/koordinates/kart/issues/135)
- Fixed primary key issues for shapefile import - now generates an `auto_pk` by default, but uses an existing field if specified (doesn't use the FID). [#646](https://github.com/koordinates/kart/pull/646)
- Add `--with-dataset-types` option to `kart meta get` which is informative now that there is more than one type of dataset. [#649](https://github.com/koordinates/kart/pull/649)
- Support `kart diff COMMIT1 COMMIT2` as an alternative to typing `kart diff COMMIT1...COMMIT2` [#666](https://github.com/koordinates/kart/issues/666)
- Add `kart helper` which starts a long running process to reduce the overhead of Python startup.
- the `--num-processes` option to `init` and `import` commands is now deprecated and does nothing. In most situations it offered no performance gain. [#692](https://github.com/koordinates/kart/issues/692)
- Honour symlinks for shared libraries rather than including copies created by PyInstaller. [#691](https://github.com/koordinates/kart/issues/691)
- Strip shared libraries on Linux to reduce package size. [#691](https://github.com/koordinates/kart/issues/691)

## 0.11.3

- Added support for `--output` option to `kart conflicts`. [#135](https://github.com/koordinates/kart/issues/135)
- Bugfix: Better error message on using `kart conflicts -ogeojson` for `meta-item` conflicts. [#515](https://github.com/koordinates/kart/issues/515)
- Removed the older `upgrade-to-tidy` and `upgrade-to-kart` features which were only relevant to Sno (predecessor of Kart). [#585](https://github.com/koordinates/kart/issues/585)
- Added support for `--decorate` and `--no-decorate` in `kart log`. [#586](https://github.com/koordinates/kart/issues/586)
- Bugfix: Fixed a bug where creating a MSSQL working copy fails when there are large (~10KB) geometries. [#617](https://github.com/koordinates/kart/issues/617)
- Bugfix: Fixed `kart diff <commit-id>` for a commit containing a dataset that has since been deleted using `kart data rm`. [#611](https://github.com/koordinates/kart/issues/611)
- Add `ext-run` to provide an execution environment for prototyping ideas/extensions.
- Added more context about datasets to JSONL diffs. [#624](https://github.com/koordinates/kart/pull/624)
- Fix `kart branch -o json` when branch HEAD is unborn or repo is empty - this would either fail outright or report that HEAD is on branch "null" which is not exactly true. [#637](https://github.com/koordinates/kart/issues/637)

## 0.11.1

- Improve performance when creating a working copy in a spatially filtered repository (this was previously slower than in a non-filtered repository; now it is much faster) [#561](https://github.com/koordinates/kart/issues/561)
- Added Sphinx [documentation](https://docs.kartproject.org/en/latest/), built from the `docs` directory. [#220](https://github.com/koordinates/kart/issues/220)

## 0.11.0

### Major changes

Support for spatial filters - the spatial filter can be updated during an `init`, `clone` or `checkout` by supplying the option `--spatial-filter=CRS;GEOMETRY` where CRS is a string such as `EPSG:4326` and GEOMETRY is a polygon or multigon specified using WKT or hex-encoded WKB. When a spatial filter is set, the working copy will only contain features that intersect the spatial filter, and changes that happened outside the working copy are not shown to the user unless specifically required. Starting with Kart 0.11.0, only the features that are inside the specified spatial filter are downloaded during a clone. [Spatial filter docs](docs/SPATIAL_FILTERING.md) | [#456](https://github.com/koordinates/kart/issues/456)

### Other changes

- Expanded `--output-format`/`-o` to accept format specifiers; e.g. `-o json:compact`. `kart log` now accepts text formatstrings, e.g. `-o text:%H` [#544](https://github.com/koordinates/kart/issues/544)
- Deprecated `--json-style` in favour of `-o json:{style}`
- diff: Added `--add-feature-count-estimate=<accuracy>` to `json-lines` diffs. This lazily inserts an estimate of the total number of features into the output stream. [#543](https://github.com/koordinates/kart/issues/543)
- Bugfix: fixed errors with Postgres working copies when one or more datasets have no CRS defined. [#529](https://github.com/koordinates/kart/issues/529)
- Bugfix: better error message when `kart import` fails due to multiple XML metadata files for a single dataset, which Kart does not support [#547](https://github.com/koordinates/kart/issues/547)
- When there are two pieces of XML metadata for a single dataset, but one is simply a `GDALMultiDomainMetadata` wrapping the other, the wrapped version is ignored. [#547](https://github.com/koordinates/kart/issues/547)
- Bugfix: fixed a bug preventing `checkout -b NEW_BRANCH HEAD^` and similar commands from working
- Bugfix: fixed a bug where `kart merge` would fail in a shallow clone, in certain circumstances. [#555](https://github.com/koordinates/kart/issues/555)

## 0.10.8

- More advanced filters for `log`, `diff` and `commit`: All of these now work:
  - Wildcard (`*`) filters for dataset names, e.g. `kart diff -- *parcel*:meta:schema.json` will show only schema changes for all datasets with `parcel` in their names. `*` by itself matches all datasets. [#532](https://github.com/koordinates/kart/issues/532)
  - You can now output the history of individual features: `kart log -- <dataset-name>:feature:<feature-primary-key>`. [#496](https://github.com/koordinates/kart/issues/496)
- `kart clone` now strips `.git` off the end of automatically-generated repo paths, if `--bare` is not specified. [#540](https://github.com/koordinates/kart/issues/540)
- Simpler developer builds using CMake, see the [contributing notes](./CONTRIBUTING.md).
- Bugfix: fixed certificate verification errors when cloning HTTPS repositories on some Linux distributions. [#541](https://github.com/koordinates/kart/pull/541)
- Bugfix: fixed the error when merging a commit where every feature in a dataset is deleted. [#506](https://github.com/koordinates/kart/pull/506)
- Bugfix: Don't allow `--replace-ids` to be specified during an import where the primary key is changing. [#521](https://github.com/koordinates/kart/issues/521)

## 0.10.7

### Bugs fixed

- `log`: Fixed some regressions in 0.10.6 involving argument parsing:
  - range arguments (`x..y` or `x...y`) were handled incorrectly. [#504](https://github.com/koordinates/kart/pull/504)
  - `-n INTEGER` caused an error [#507](https://github.com/koordinates/kart/pull/507)
  - `kart log @` was empty (`@` is supposed to be handled as a synonym for `HEAD`) [#510](https://github.com/koordinates/kart/pull/510)
- Auto-incrementing PK sequences now work in PostGIS working copies for table names containing the `.` character. [#468](https://github.com/koordinates/kart/pull/468)

## 0.10.6

- Information about the current spatial filter is now shown in `status`. [#456](https://github.com/koordinates/kart/issues/456)
- Added a specification for allowed characters & path components in dataset names - see [Valid Dataset Names](https://github.com/koordinates/kart/blob/master/docs/DATASETS_v3.md#valid-dataset-names).
- New `kart data rm` command to simply delete datasets and commit the result [#490](https://github.com/koordinates/kart/issues/491)
- Fix for [#491](https://github.com/koordinates/kart/issues/491) - make Kart more robust to manual edits to the GPKG working copy that don't leave the metadata exactly as Kart would leave it (such as by leaving unneeded table rows in `gpkg_contents`)
- Added minimal patches:
  - `kart create-patch` now supports `--patch-type minimal`, which creates a much-smaller patch; relying on the patch recipient having the HEAD commit in their repository [#482](https://github.com/koordinates/kart/issues/482)
  - `kart apply` now applies both types of patch.
- `kart log` now accepts a `--` marker to signal that all remaining arguments are dataset names. [#498](https://github.com/koordinates/kart/issues/498)
- `import` from a Postgres or MSSQL source will no longer prepend the database schema name to the imported dataset path.
- Bugfix: Diffing between an old commit and the current working copy no longer fails when datasets have been deleted in the intervening commits.
- Bugfix: Existing auto-incrementing integer PK sequences are now overwritten properly in GPKG working copies. [#468](https://github.com/koordinates/kart/pull/468)

## 0.10.5

- Fixed regressions in `diff -o geojson` since Kart 0.10.1 [#487](https://github.com/koordinates/kart/issues/487)
- Removed `kart show -o geojson` [#487](https://github.com/koordinates/kart/issues/487#issuecomment-933561924)
- Fix for [#478](https://github.com/koordinates/kart/issues/478) `merge --dry-run` raises error
- Fix for [#483](https://github.com/koordinates/kart/issues/483) `diff` error with Z/M geometries

## 0.10.4

### Major changes

- Added basic support for spatial filters - the spatial filter can be updated during an `init`, `clone` or `checkout` by supplying the option `--spatial-filter=CRS;GEOMETRY` where CRS is a string such as `EPSG:4326` and GEOMETRY is a polygon or multigon specified using WKT or hex-encoded WKB. When a spatial filter is set, the working copy will only contain features that intersect the spatial filter, and changes that happened outside the working copy are not shown to the user unless specifically required. [#456](https://github.com/koordinates/kart/issues/456)

### Other changes

- Auto-incrementing integer PKs: When the working copy is written, Kart now sets up a sequence which supplies the next unassigned PK value and sets it as the default value for the PK column. This helps the user find the next unassigned PK, which can be non-obvious in particular when a spatial filter has been applied and not all features are present in the working copy. [#468](https://github.com/koordinates/kart/pull/468)
- Bugfix: Set GDAL and PROJ environment variables on startup, which fixes an issue where Kart may or may not work properly depending on whether GDAL and PROJ are appropriately configured in the user's environment
- Bugfix: `kart restore` now simply discards all working copy changes, as it is intended to - previously it would complain if there were "structural" schema differences between the working copy and HEAD.
- Bugfix: MySQL working copy now works without a timezone database - previously it required that at least `UTC` was defined in such a database.
- Feature-count estimates are now more accurate and generally also faster [#467](https://github.com/koordinates/kart/issues/467)
- `kart log` now supports output in JSON-lines format, so that large logs can be streamed before being entirely generated.

## 0.10.2

- Added support for the geometry `POINT EMPTY` in SQL Server working copy.
- Bugfix: fixed the error when writing diff output to a file. [#453](https://github.com/koordinates/kart/issues/453)
- Bugfix: when checking out a dataset that has an integer primary key as a GPKG working copy, Kart should continue to use the actual primary key instead of overriding it, even if the primary key column isn't the first column. [#455](https://github.com/koordinates/kart/issues/455)

## 0.10.1

#### Fix for `kart upgrade`

Fixed `kart upgrade` so that it preserves more complicated (or yet-to-be-released) features of V2 repos as they are upgraded to V3. [#448](https://github.com/koordinates/kart/issues/448)

Specifically:

- `generated-pks.json` metadata, extra metadata found in datasets that have an automatically generated primary key and which are maintained by repeatedly importing from a primary-key-less datasource
- attachments (which are not yet fully supported by Kart) - arbitrary files kept alongside datasets, such as license or readme files.

#### Other changes

- `kart show` now supports all the same options as `kart diff`. Both `kart diff` and `kart show` now both support output in JSON-lines format, so that large diffs can be processed as the diff is generated.
- Bugfix: diffs containing a mixture of primary key types can now be shown (necessary in the case where the primary key type has changed).
- Some performance improvements - less startup overhead.

## 0.10.0

Kart v0.10.0 introduces a new repository structure, which is the default, dubbed 'Datasets V3'. Datasets V2 continues to be supported, but all newly created repos are V3 going forward.

### Datasets V3

- Entire repositories can be upgraded from V2 to V3 using `kart upgrade EXISTING_REPO NEW_REPO`.
- Anything which works in a V2 repo should work in a V3 repo and vice versa.
- V3 repos are more performant for large datasets - compared to V2 repos where size-on-disk climbs quickly once dataset size exceeds 16 million features.

### Other major changes in this release

- The working copy can now be a MySQL database (previously only GPKG, PostGIS and SQL Server working copies were supported). The commands `init`, `clone` and `create-workingcopy` now all accept working copy paths in the form `mysql://HOST/DBNAME` [#399](https://github.com/koordinates/kart/pull/399)
  - Read the documentation at [docs/MYSQL_WC.md](docs/MYSQL_WC.md)
- Import of tables using `kart import` is now supported from any type of database that Kart also supports writing to as a working copy - namely, GPKG, PostGIS, SQL Server and MySQL.
- Support for rapidly calculating or estimating feature-counts - see below.

### Other minor changes

- Change to `kart data ls` JSON output, now includes whether repo is Kart or Sno branded.
- Importing from a datasource now samples the first geometry to check the number of dimensions - in case the datasource actually has 3 or 4 dimensions but this fact is not stored in the column metadata (which is not necessarily required by all source types). [#337](https://github.com/koordinates/kart/issues/337)
- Bugfix: Creating a working copy while switching branch now creates a working copy with the post-switch branch checked out, not the pre-switch branch.
- Bugfix: GPKG spatial indexes are now created and deleted properly regardless of the case (upper-case or lower-case) of the table name and geometry column.
- A few bugfixes involving accurately roundtripping boolean and blob types through different working copy types.
- Bugfix: 3D and 4D geometries are now properly roundtripped through SQL Server working copy.
- Fix help text for discarding changes to refer to `kart restore` instead of `kart reset`, as `kart restore` is now the simplest way to discard changes. [#426](https://github.com/koordinates/kart/issues/426)
- `import`: PostGIS internal views/tables are no longer listed by `--list` or imported by `--all-tables`, and can't be imported by name either. [#439](https://github.com/koordinates/kart/issues/439)
- `upgrade` no longer adds a `main` or `master` branch to upgraded repos.

### Calculating feature counts for diffs

Kart now includes ways to calculate or estimate feature counts for diffs. This encompasses the following changes:

- `diff` now accepts `--only-feature-count=<ACCURACY>`, which produces a feature count for the diff.
- `log` now accepts `--with-feature-count=<ACCURACY>` which adds a feature count to each commit when used with `-o json`.
- All calculated feature counts are stored in a SQLite database in the repo's `.kart` directory.
- Feature counts for commit diffs can be populated in bulk with the new `build-annotations` command

## 0.9.0 (First "Kart" release)

### Major changes in this release

- First and foremost, the name — we're now called Kart!

### Other changes

- Various local config and structure which was named after `sno` is now named after `kart` - for instance, a Kart repo's objects are now hidden inside a `.kart` folder. Sno repos with the older names will continue to be supported going forward. To modify a repo in place to use the `kart` based names instead of the `sno` ones, use `kart upgrade-to-kart PATH`.
- `import` & `init` are often much faster now because they do imports in parallel subprocesses. Use `--num-processes` to control this behaviour. [#408](https://github.com/koordinates/kart/pull/408)
- `status -o json` now shows which branch you are on, even if that branch doesn't yet have any commits yet.

## 0.8.0 (Last "Sno" release)

### Breaking changes

- Internally, Sno now stores XML metadata in an XML file, instead of nested inside a JSON file. This is part of a longer term plan to make it easier to attach metadata or other files to a repository in a straight-forward way, without having to understand JSON internals. Unfortunately, diffing commits where the XML metadata has been written by Sno 0.8.0 won't work in Sno 0.7.1 or earlier - it will fail with `binascii.Error`
- Backwards compatibility with Datasets V1 ends at Sno 0.8.0 - all Sno commands except `sno upgrade` will no longer work in a V1 repository. Since Datasets V2 has been the default since Sno 0.5.0, most users will be unaffected. Remaining V1 repositories can be upgraded to V2 using `sno upgrade EXISTING_REPO NEW_REPO`, and the ability to upgrade from V1 to V2 continues to be supported indefinitely. [#342](https://github.com/koordinates/kart/pull/342)
- `sno init` now sets the head branch to `main` by default, instead of `master`. To override this, add `--initial-branch=master`
- `reset` now behaves more like `git reset` - specifically, `sno reset COMMIT` stays on the same branch but sets the branch tip to be `COMMIT`. [#60](https://github.com/koordinates/kart/issues/60)
- `import` now accepts a `--replace-ids` argument for much faster importing of small changesets from large sources. [#378](https://github.com/koordinates/kart/issues/378)

### Other changes

- The working copy can now be a SQL Server database (previously only GPKG and PostGIS working copies were supported). The commands `init`, `clone` and `create-workingcopy` now all accept working copy paths in the form `mssql://HOST/DBNAME/DBSCHEMA` [#362](https://github.com/koordinates/kart/issues/362)
  - Currently requires that the ODBC driver for SQL Server is installed.
  - Read the documentation at [docs/SQL_SERVER_WC.md](docs/SQL_SERVER_WC.md)
- Support for detecting features which have changed slightly during a re-import from a data source without a primary key, and reimporting them with the same primary key as last time so they show as edits as opposed to inserts. [#212](https://github.com/koordinates/kart/issues/212)
- Optimised GPKG working copies for better performance for large datasets.
- Bugfix - fixed issues roundtripping certain type metadata in the PostGIS working copy: specifically geometry types with 3 or more dimensions (Z/M values) and numeric types with scale.
- Bugfix - if a database schema already exists, Sno shouldn't try to create it, and it shouldn't matter if Sno lacks permission to do so [#391](https://github.com/koordinates/kart/issues/391)
- Internal dependency change - Sno no longer depends on [apsw](https://pypi.org/project/apsw/), instead it depends on [SQLAlchemy](https://www.sqlalchemy.org/).
- `init` now accepts a `--initial-branch` option
- `clone` now accepts a `--filter` option (advanced users only)
- `show -o json` now includes the commit hash in the output
- `import` from Postgres now uses a server-side cursor, which means sno uses less memory
- Improved log formatting at higher verbosity levels
- `sno -vvv` will log SQL queries to the console for debugging

## 0.7.1

#### JSON syntax-highlighting fix

- Any command which outputs JSON would fail in 0.7.0 when run in a terminal unless a JSON style other than `--pretty` was explicitly specified, due to a change in the pygments library which Sno's JSON syntax-highlighting code failed to accomodate. This is fixed in the 0.7.1 release. [#335](https://github.com/koordinates/kart/pull/335)

## 0.7.0

### Major changes in this release

- Support for importing data without a primary key. Since the Sno model requires that every feature has a primary key, primary keys are assigned during import. [#212](https://github.com/koordinates/kart/issues/212)
- Support for checking out a dataset with a string primary key (or other non-integer primary key) as a GPKG working copy. [#307](https://github.com/koordinates/kart/issues/307)

### Minor features / fixes:

- Improved error recovery: Sno commands now write to the working copy within a single transaction, which is rolled back if the command fails. [#281](https://github.com/koordinates/kart/pull/281)
- Dependency upgrades (GDAL; Git; Pygit2; Libgit2; Spatialite; GEOS) [#327](https://github.com/koordinates/kart/pull/327)
- Bugfixes:
  - `sno meta set` didn't allow updates to `schema.json`
  - Fixed a potential `KeyError` in `Schema._try_align`
  - Fixed a potential `unexpected NoneType` in `WorkingCopy.is_dirty`
  - Imports now preserve fixed-precision numeric types in most situations.
  - Imports now preserve length of text/string fields.
  - Imported fields of type `numeric` now stored internally as strings, as required by datasets V2 spec. [#325](https://github.com/koordinates/kart/pull/325)

## 0.6.0

### Major changes in this release

- Newly created Sno repositories no longer have git internals visible in the main folder - they are hidden away in a '.sno' folder. [#147](https://github.com/koordinates/kart/issues/147)
- The working copy can now be a PostgreSQL / PostGIS database (previously only GPKG working copies were supported). The commands `init`, `clone` and `create-workingcopy` now all accept working copy paths in the form `postgresql://HOST/DBNAME/DBSCHEMA` [#267](https://github.com/koordinates/kart/issues/267)
  - Read the documentation at [docs/POSTGIS_WC.md](docs/POSTGIS_WC.md)
- Patches that create or delete datasets are now supported in Datasets V2 [#239](https://github.com/koordinates/kart/issues/239)

### Minor features / fixes:

- `apply` and `import` no longer create empty commits unless you specify `--allow-empty` [#243](https://github.com/koordinates/kart/issues/243), [#245](https://github.com/koordinates/kart/issues/245)
- `apply` can now apply patches to branches other than `HEAD` [#294](https://github.com/koordinates/kart/issues/294)
- `apply`, `commit` and `merge` commands now optimise repositories after committing, to avoid poor repo performance. [#250](https://github.com/koordinates/kart/issues/250)
- `commit` now checks that the diff to be committed matches the schema, and rejects diffs that do not - this is possible in working copy formats that have relatively lax type enforcement, ie GPKG [#300](https://github.com/koordinates/kart/pull/300)
- Added GPKG support for Sno types that GPKG doesn't support - they are approximated as strings. [#304](https://github.com/koordinates/kart/pull/304)
- `schema.json` no longer stores attributes that are null - a missing attribute has the same meaning as that attribute being present and null. [#304](https://github.com/koordinates/kart/pull/304)
- `data ls` now accepts an optional ref argument
- `meta get` now accepts a `--ref=REF` option
- `clone` now accepts a `--branch` option to clone a specific branch.
- `switch BRANCH` now switches to a newly created local branch that tracks `BRANCH`, if `BRANCH` is a remote branch and not a local branch [#259](https://github.com/koordinates/kart/issues/259)
- `gc` command added (delegates to `git gc`)
- Bugfix - don't drop the user-supplied authority from the supplied CRS and generate a new unrelated one. [#278](https://github.com/koordinates/kart/issues/278)
- Bugfix - generated CRS numbers are now within the user range: 200000 to 209199 [#296](https://github.com/koordinates/kart/issues/296)

## 0.5.0

Sno v0.5 introduces a new repo layout, which is the default, dubbed 'Datasets V2'

Existing commands are backward compatible with V1 datasets, however some new functionality is only supported in repositories upgraded to the new layout.

### Datasets V2

- Entire repositories can be upgraded from V1 to V2 using `sno upgrade EXISTING_REPO NEW_REPO`.
- V2 should support everything V1 supports
- All new repositories use the new layout by default. To opt out, use the `--repo-version=1` flag for `sno init`
- A future release will drop support for v1 repositories

#### New features for V2 repositories only

- Most schema changes now work
  - this includes column adds, drops, renames and reordering.
  - Notably, changing the primary key field of a dataset are not yet supported.
- Meta changes are now supported (title, description and XML metadata for each dataset)
- `import` now has a `--replace-existing` flag to replace existing dataset(s).

#### Missing functionality in Datasets V2

- String primary keys and tables without primary keys are not yet supported. [#212](https://github.com/koordinates/kart/issues/212)
- Changing the primary key column is not yet supported. [#238](https://github.com/koordinates/kart/issues/238)
- Patches which create or delete datasets are not supported. [#239](https://github.com/koordinates/kart/issues/239)
- Schema changes might not be correctly interpreted if too many changes are made at once (eg adding a new column with the same name as a deleted column - sno may incorrectly assume it is the same column).
  - It is safest to commit schema changes to any existing columns, then commit schema changes adding any new columns, then commit any feature changes.

### Breaking changes in this release

- New structure to `sno diff` output:
  - Text output: Features are now labelled as `<dataset>:feature:<primary_key>`, consistent with meta items that are labelled as `<dataset>:meta:<meta_item_name>`
  - JSON output also uses "feature" and "meta" as keys for the different types of changes, instead of "featureChanges" and "metaChanges".
- `sno show -o json` header key changed to `sno.show/v1`, which is not an applyable patch. Use `sno create-patch` to create a patch.
- `sno upgrade` now only takes two arguments: `sno upgrade EXISTING_REPO NEW_REPO`. No other arguments are required or accepted, exactly how to upgrade the repository is detected automatically.

### Other changes in this release

- Added `sno create-patch <refish>` - creates a JSON patch file, which can be applied using `sno apply` [#210](https://github.com/koordinates/kart/issues/210)
- Added `sno data ls` - shows a list of datasets in the sno repository [#203](https://github.com/koordinates/kart/issues/203)
- `sno help [command]` is a synonym for `sno [subcommand] --help` [#221](https://github.com/koordinates/kart/issues/221)
- `sno clone` now support shallow clones (`--depth N`) to avoid cloning a repo's entire history [#174](https://github.com/koordinates/kart/issues/174)
- `sno log` now supports JSON output with `--output-format json` [#170](https://github.com/koordinates/kart/issues/170)
- `sno meta get` now prints text items as text (not encoded as JSON) [#211](https://github.com/koordinates/kart/issues/211)
- `sno meta get` without arguments now outputs multiple datasets [#217](https://github.com/koordinates/kart/issues/217)
- `sno diff` and `sno show` now accept a `--crs` parameter to reproject output [#213](https://github.com/koordinates/kart/issues/213)
- Streaming diffs: less time until first change is shown when diffing large changes. [#156](https://github.com/koordinates/kart/issues/156)
- Working copies are now created automatically. [#192](https://github.com/koordinates/kart/issues/192)
- Commands which are misspelled now suggest the correct spelling [#199](https://github.com/koordinates/kart/issues/199)
- Bugfix: operations that should immediately fail due to dirty working copy no longer partially succeed. [#181](https://github.com/koordinates/kart/issues/181)
- Bugfix: some column datatype conversion issues during import and checkout.
- Linux: Add openssh client dependency into rpm & deb packages. [#121](https://github.com/koordinates/kart/issues/121)
- Windows: Fix missing PROJ data files in packages. [#235](https://github.com/koordinates/kart/issues/235)

## 0.4.1

### Packaging fix:

- packaging: Fix issue with broken git component paths in packages on macOS and Linux ([#143](https://github.com/koordinates/kart/issues/143))
- packaging: Exclude dev dependency in macOS package

### Minor features / fixes:

- Added a `sno meta get` command for viewing dataset metadata ([#136](https://github.com/koordinates/kart/issues/136))
- `merge`, `commit`, `init`, `import` commands can now take commit messages as files with `--message=@filename.txt`. This replaces the `sno commit -F` option ([#138](https://github.com/koordinates/kart/issues/138))
- `import`: Added `--table-info` option to set dataset metadata, when it can't be autodetected from the source database ([#139](https://github.com/koordinates/kart/issues/139))
- `pull`, `push`, `fetch`, `clone` commands now show progress - disabled with `--quiet` ([#144](https://github.com/koordinates/kart/issues/144))
- `import` now works while on an empty branch ([#149](https://github.com/koordinates/kart/issues/149))

## 0.4.0

### Major changes in this release

- Basic conflict resolution:
  - `sno merge` now puts the repo in a merging state when there are conflicts ([#80](https://github.com/koordinates/kart/issues/80))
  - Added `sno conflicts` to list conflicts ([#84](https://github.com/koordinates/kart/issues/84))
  - Added `sno resolve` ([#101](https://github.com/koordinates/kart/issues/101))
  - Added `sno merge --continue` ([#94](https://github.com/koordinates/kart/issues/94))
- Major improvements to `sno import` and `sno init --import`:
  - Can now import from postgres databases ([#90](https://github.com/koordinates/kart/issues/90))
  - Multiple tables can be imported at once ([#118](https://github.com/koordinates/kart/issues/118))
- Added `sno show`: shows a commit. With `-o json` generates a patch ([#48](https://github.com/koordinates/kart/issues/48))
- Added `sno apply` to apply the patches generated by `sno show -o json` ([#61](https://github.com/koordinates/kart/issues/61))

### Minor features / fixes:

- add a changelog (here!)
- `sno import` enhancements (in addition to major changes above):
  - GPKG database paths no longer need `GPKG:` prefix
  - now takes table names as separate arguments
  - Added `--primary-key=FIELD` to override primary key field name
  - Added `--message` to customize the commit message
  - `--list` no longer requires a repository
- `sno init --import` enhancements:
  - imports are much faster ([#55](https://github.com/koordinates/kart/issues/55))
  - now imports _all_ tables from database, doesn't allow table to be specified
- Many JSON output improvements:
  - JSON output is specified with `-o json` instead of `--json` ([#98](https://github.com/koordinates/kart/issues/98))
  - Added syntax highlighting to JSON output when viewed in a terminal ([#54](https://github.com/koordinates/kart/issues/54))
  - `sno diff` JSON output layout has changed - features are now flat objects instead of GeoJSON objects. This is much more compact ([#71](https://github.com/koordinates/kart/issues/71))
  - Added JSON output option for most commands
  - Added `--json-style` option to several commands to control JSON formatting ([#70](https://github.com/koordinates/kart/issues/70))
- `sno diff`:
  - `a..b` now refers to the same changes as `sno log a..b` ([#116](https://github.com/koordinates/kart/issues/116))
  - can now diff against tree objects, particularly the empty tree ([#53](https://github.com/koordinates/kart/issues/53))
  - can now view some subset of the changes by supplying filter args, ie `[dataset[:pk]]`
- `sno commit`:
  - can now commit some subset of the changes by supplying filter args, ie `[dataset[:pk]]` ([#69](https://github.com/koordinates/kart/issues/69))
- removed `import-gpkg` command; use `import` instead ([#85](https://github.com/koordinates/kart/issues/85))
- Error messages now go to stderr instead of stdout ([#57](https://github.com/koordinates/kart/issues/57))
- Error conditions now use exit codes to indicate different types of errors ([#46](https://github.com/koordinates/kart/issues/46))

## 0.3.1

- Sno is now available on Windows 🎉 (Windows 8.1+ / Server 2016+ (64-bit))
- Updates to continuous integration — installers/archives are now built and tested with every commit for every platform.
- For macOS users, a homebrew "tap" is now available: `brew cask install koordinates/sno/sno`
- Several bug fixes

## 0.3.0

### Major changes in this release

- License: sno is now publicly available under the GPL open source license.
- Sno now has a website at [**sno.earth**](https://sno.earth)
- Standalone builds and packaging for Linux and macOS. Windows won't be far behind
- Refactoring to support alternative database working copies
- Dependency upgrades (GDAL; Git; Pygit2; Proj; Libgit2; Sqlite; and others)
- Several bug fixes

### Upgrading

If you were running a preview release, remove it before installing the new release:

```console
$ brew uninstall sno
$ brew untap koordinates/sno
```

### Repository Hosting

We have an initial preview available of our Sno repository hosting. This allows you & your team to push and pull Sno repositories. Please contact support@koordinates.com with your Github username and we can get you set up. There is no cost for this service.

### Compatibility

Repositories created with Sno v0.2 are compatible with v0.3. For assistance upgrading any v0.1 repositories, please read our [upgrade guide](https://docs.kartproject.org/en/latest/pages/upgrading.html).

## 0.2.0

### Major changes in this release

- First and foremost, the name — we're now called Sno!
- A new repository structure/layout, which has better performance and a smaller on-disk size
- Data imports are now orders of magnitude faster
- Support for multiple datasets in a single Sno repository
- Support for non-spatial datasets
- Increased test coverage including end-to-end tests
- Improved macOS Homebrew packaging with CI testing.
- Prototype support for spatial-indexing and a sno query command for spatial lookups.
- Diffs across branches/commits, and a GeoJSON diff format.
- Numerous bug fixes
