# Kart changelog

Please note that compatibility for 0.x releases (software or repositories) isn't guaranteed. Kart is evolving quickly and things will change. However, we aim to provide the means to upgrade existing repositories between 0.x versions and to 1.0.

_When adding new entries to the changelog, please include issue/PR numbers wherever possible._


## 0.10.0 (UNRELEASED)

* Change to `kart data ls` JSON output, now includes whether repo is Kart or Sno branded.

## 0.9.0 (First "Kart" release)

### Major changes in this release
* First and foremost, the name — we're now called Kart!

### Other changes
* Various local config and structure which was named after `sno` is now named after `kart` - for instance, a Kart repo's objects are now hidden inside a `.kart` folder. Sno repos with the older names will continue to be supported going forward. To modify a repo in place to use the `kart` based names instead of the `sno` ones, use `kart upgrade-to-kart PATH`.
* `import` & `init` are often much faster now because they do imports in parallel subprocesses. Use `--num-processes` to control this behaviour. [#408](https://github.com/koordinates/kart/pull/408)
* `status -o json` now shows which branch you are on, even if that branch doesn't yet have any commits yet.

## 0.8.0 (Last "Sno" release)

### Breaking changes

 * Internally, Sno now stores XML metadata in an XML file, instead of nested inside a JSON file. This is part of a longer term plan to make it easier to attach metadata or other files to a repository in a straight-forward way, without having to understand JSON internals. Unfortunately, diffing commits where the XML metadata has been written by Sno 0.8.0 won't work in Sno 0.7.1 or earlier - it will fail with `binascii.Error`
 * Backwards compatibility with Datasets V1 ends at Sno 0.8.0 - all Sno commands except `sno upgrade` will no longer work in a V1 repository. Since Datasets V2 has been the default since Sno 0.5.0, most users will be unaffected. Remaining V1 repositories can be upgraded to V2 using `sno upgrade EXISTING_REPO NEW_REPO`, and the ability to upgrade from V1 to V2 continues to be supported indefinitely. [#342](https://github.com/koordinates/kart/pull/342)
 * `sno init` now sets the head branch to `main` by default, instead of `master`. To override this, add `--initial-branch=master`
 * `reset` now behaves more like `git reset` - specifically, `sno reset COMMIT` stays on the same branch but sets the branch tip to be `COMMIT`. [#60](https://github.com/koordinates/kart/issues/60)
 * `import` now accepts a `--replace-ids` argument for much faster importing of small changesets from large sources. [#378](https://github.com/koordinates/kart/issues/378)

### Other changes

 * The working copy can now be a SQL Server database (previously only GPKG and PostGIS working copies were supported). The commands `init`, `clone` and `create-workingcopy` now all accept working copy paths in the form `mssql://[HOST]/DBNAME/SCHEMA` [#362](https://github.com/koordinates/kart/issues/362)
     - Currently requires that the ODBC driver for SQL Server is installed.
     - Read the documentation at `docs/SQL_SERVER_WC.md`
 * Support for detecting features which have changed slightly during a re-import from a data source without a primary key, and reimporting them with the same primary key as last time so they show as edits as opposed to inserts. [#212](https://github.com/koordinates/kart/issues/212)
 * Optimised GPKG working copies for better performance for large datasets.
 * Bugfix - fixed issues roundtripping certain type metadata in the PostGIS working copy: specifically geometry types with 3 or more dimensions (Z/M values) and numeric types with scale.
 * Bugfix - if a database schema already exists, Sno shouldn't try to create it, and it shouldn't matter if Sno lacks permission to do so [#391](https://github.com/koordinates/kart/issues/391)
 * Internal dependency change - Sno no longer depends on [apsw](https://pypi.org/project/apsw/), instead it depends on [SQLAlchemy](https://www.sqlalchemy.org/).
 * `init` now accepts a `--initial-branch` option
 * `clone` now accepts a `--filter` option (advanced users only)
 * `show -o json` now includes the commit hash in the output
 * `import` from Postgres now uses a server-side cursor, which means sno uses less memory
 * Improved log formatting at higher verbosity levels
 * `sno -vvv` will log SQL queries to the console for debugging

## 0.7.1

#### JSON syntax-highlighting fix

* Any command which outputs JSON would fail in 0.7.0 when run in a terminal unless a JSON style other than `--pretty` was explicitly specified, due to a change in the pygments library which Sno's JSON syntax-highlighting code failed to accomodate. This is fixed in the 0.7.1 release. [#335](https://github.com/koordinates/kart/pull/335)

## 0.7.0

### Major changes in this release

 * Support for importing data without a primary key. Since the Sno model requires that every feature has a primary key, primary keys are assigned during import. [#212](https://github.com/koordinates/kart/issues/212)
 * Support for checking out a dataset with a string primary key (or other non-integer primary key) as a GPKG working copy. [#307](https://github.com/koordinates/kart/issues/307)

### Minor features / fixes:

 * Improved error recovery: Sno commands now write to the working copy within a single transaction, which is rolled back if the command fails. [#281](https://github.com/koordinates/kart/pull/281)
 * Dependency upgrades (GDAL; Git; Pygit2; Libgit2; Spatialite; GEOS) [#327](https://github.com/koordinates/kart/pull/327)
 * Bugfixes:
   - `sno meta set` didn't allow updates to `schema.json`
   - Fixed a potential `KeyError` in `Schema._try_align`
   - Fixed a potential `unexpected NoneType` in `WorkingCopy.is_dirty`
   - Imports now preserve fixed-precision numeric types in most situations.
   - Imports now preserve length of text/string fields.
   - Imported fields of type `numeric` now stored internally as strings, as required by datasets V2 spec. [#325](https://github.com/koordinates/kart/pull/325)


## 0.6.0

### Major changes in this release

 * Newly created Sno repositories no longer have git internals visible in the main folder - they are hidden away in a '.sno' folder. [#147](https://github.com/koordinates/kart/issues/147)
 * The working copy can now be a PostgreSQL / PostGIS database (previously only GPKG working copies were supported). The commands `init`, `clone` and `create-workingcopy` now all accept working copy paths in the form `postgresql://[HOST]/DBNAME/SCHEMA` [#267](https://github.com/koordinates/kart/issues/267)
     - Read the documentation at `docs/POSTGIS_WC.md`
 * Patches that create or delete datasets are now supported in Datasets V2 [#239](https://github.com/koordinates/kart/issues/239)

### Minor features / fixes:

 * `apply` and `import` no longer create empty commits unless you specify `--allow-empty` [#243](https://github.com/koordinates/kart/issues/243), [#245](https://github.com/koordinates/kart/issues/245)
 * `apply` can now apply patches to branches other than `HEAD` [#294](https://github.com/koordinates/kart/issues/294)
 * `apply`, `commit` and `merge` commands now optimise repositories after committing, to avoid poor repo performance. [#250](https://github.com/koordinates/kart/issues/250)
 * `commit` now checks that the diff to be committed matches the schema, and rejects diffs that do not - this is possible in working copy formats that have relatively lax type enforcement, ie GPKG [#300](https://github.com/koordinates/kart/pull/300)
 *  Added GPKG support for Sno types that GPKG doesn't support - they are approximated as strings. [#304](https://github.com/koordinates/kart/pull/304)
 *  `schema.json` no longer stores attributes that are null - a missing attribute has the same meaning as that attribute being present and null. [#304](https://github.com/koordinates/kart/pull/304)
 * `data ls` now accepts an optional ref argument
 * `meta get` now accepts a `--ref=REF` option
 * `clone` now accepts a `--branch` option to clone a specific branch.
 * `switch BRANCH` now switches to a newly created local branch that tracks `BRANCH`, if `BRANCH` is a remote branch and not a local branch [#259](https://github.com/koordinates/kart/issues/259)
 * `gc` command added (delegates to `git gc`)
 * Bugfix - don't drop the user-supplied authority from the supplied CRS and generate a new unrelated one. [#278](https://github.com/koordinates/kart/issues/278)
 * Bugfix - generated CRS numbers are now within the user range: 200000 to 209199 [#296](https://github.com/koordinates/kart/issues/296)

## 0.5.0

Sno v0.5 introduces a new repo layout, which is the default, dubbed 'Datasets V2'

Existing commands are backward compatible with V1 datasets, however some new functionality is only supported in repositories upgraded to the new layout.


### Datasets V2

 * Entire repositories can be upgraded from V1 to V2 using `sno upgrade EXISTING_REPO NEW_REPO`.
 * V2 should support everything V1 supports
 * All new repositories use the new layout by default. To opt out, use the `--repo-version=1` flag for `sno init`
 * A future release will drop support for v1 repositories

#### New features for V2 repositories only

 * Most schema changes now work
     - this includes column adds, drops, renames and reordering.
     - Notably, changing the primary key field of a dataset are not yet supported.
 * Meta changes are now supported (title, description and XML metadata for each dataset)
 * `import` now has a `--replace-existing` flag to replace existing dataset(s).

#### Missing functionality in Datasets V2

 * String primary keys and tables without primary keys are not yet supported. [#212](https://github.com/koordinates/kart/issues/212)
 * Changing the primary key column is not yet supported. [#238](https://github.com/koordinates/kart/issues/238)
 * Patches which create or delete datasets are not supported. [#239](https://github.com/koordinates/kart/issues/239)
 * Schema changes might not be correctly interpreted if too many changes are made at once (eg adding a new column with the same name as a deleted column - sno may incorrectly assume it is the same column).
    - It is safest to commit schema changes to any existing columns, then commit schema changes adding any new columns, then commit any feature changes.

### Breaking changes in this release

 * New structure to `sno diff` output:
    - Text output: Features are now labelled as `<dataset>:feature:<primary_key>`, consistent with meta items that are labelled as `<dataset>:meta:<meta_item_name>`
    - JSON output also uses "feature" and "meta" as keys for the different types of changes, instead of "featureChanges" and "metaChanges".
 * `sno show -o json` header key changed to `sno.show/v1`, which is not an applyable patch. Use `sno create-patch` to create a patch.
 * `sno upgrade` now only takes two arguments: `sno upgrade EXISTING_REPO NEW_REPO`. No other arguments are required or accepted, exactly how to upgrade the repository is detected automatically.

### Other changes in this release

 * Added `sno create-patch <refish>` - creates a JSON patch file, which can be applied using `sno apply` [#210](https://github.com/koordinates/kart/issues/210)
 * Added `sno data ls` - shows a list of datasets in the sno repository [#203](https://github.com/koordinates/kart/issues/203)
 * `sno help [command]` is a synonym for `sno [subcommand] --help` [#221](https://github.com/koordinates/kart/issues/221)
 * `sno clone` now support shallow clones (`--depth N`) to avoid cloning a repo's entire history [#174](https://github.com/koordinates/kart/issues/174)
 * `sno log` now supports JSON output with `--output-format json` [#170](https://github.com/koordinates/kart/issues/170)
 * `sno meta get` now prints text items as text (not encoded as JSON) [#211](https://github.com/koordinates/kart/issues/211)
 * `sno meta get` without arguments now outputs multiple datasets [#217](https://github.com/koordinates/kart/issues/217)
 * `sno diff` and `sno show` now accept a `--crs` parameter to reproject output [#213](https://github.com/koordinates/kart/issues/213)
 * Streaming diffs: less time until first change is shown when diffing large changes. [#156](https://github.com/koordinates/kart/issues/156)
 * Working copies are now created automatically. [#192](https://github.com/koordinates/kart/issues/192)
 * Commands which are misspelled now suggest the correct spelling [#199](https://github.com/koordinates/kart/issues/199)
 * Bugfix: operations that should immediately fail due to dirty working copy no longer partially succeed. [#181](https://github.com/koordinates/kart/issues/181)
 * Bugfix: some column datatype conversion issues during import and checkout.
 * Linux: Add openssh client dependency into rpm & deb packages. [#121](https://github.com/koordinates/kart/issues/121)
 * Windows: Fix missing PROJ data files in packages. [#235](https://github.com/koordinates/kart/issues/235)

## 0.4.1

### Packaging fix:

* packaging: Fix issue with broken git component paths in packages on macOS and Linux ([#143](https://github.com/koordinates/kart/issues/143))
* packaging: Exclude dev dependency in macOS package

### Minor features / fixes:

* Added a `sno meta get` command for viewing dataset metadata ([#136](https://github.com/koordinates/kart/issues/136))
* `merge`, `commit`, `init`, `import` commands can now take commit messages as files with `--message=@filename.txt`. This replaces the `sno commit -F` option ([#138](https://github.com/koordinates/kart/issues/138))
* `import`: Added `--table-info` option to set dataset metadata, when it can't be autodetected from the source database ([#139](https://github.com/koordinates/kart/issues/139))
* `pull`, `push`, `fetch`, `clone` commands now show progress - disabled with `--quiet` ([#144](https://github.com/koordinates/kart/issues/144))
* `import` now works while on an empty branch ([#149](https://github.com/koordinates/kart/issues/149))

## 0.4.0

### Major changes in this release

* Basic conflict resolution:
    - `sno merge` now puts the repo in a merging state when there are conflicts ([#80](https://github.com/koordinates/kart/issues/80))
    - Added `sno conflicts` to list conflicts ([#84](https://github.com/koordinates/kart/issues/84))
    - Added `sno resolve` ([#101](https://github.com/koordinates/kart/issues/101))
    - Added `sno merge --continue`  ([#94](https://github.com/koordinates/kart/issues/94))
* Major improvements to `sno import` and `sno init --import`:
    - Can now import from postgres databases ([#90](https://github.com/koordinates/kart/issues/90))
    - Multiple tables can be imported at once ([#118](https://github.com/koordinates/kart/issues/118))
* Added `sno show`: shows a commit. With `-o json` generates a patch ([#48](https://github.com/koordinates/kart/issues/48))
* Added `sno apply` to apply the patches generated by `sno show -o json` ([#61](https://github.com/koordinates/kart/issues/61))

### Minor features / fixes:

* add a changelog (here!)
* `sno import` enhancements (in addition to major changes above):
    - GPKG database paths no longer need `GPKG:` prefix
    - now takes table names as separate arguments
    - Added `--primary-key=FIELD` to override primary key field name
    - Added `--message` to customize the commit message
    - `--list` no longer requires a repository
* `sno init --import` enhancements:
    - imports are much faster ([#55](https://github.com/koordinates/kart/issues/55))
    - now imports _all_ tables from database, doesn't allow table to be specified
* Many JSON output improvements:
    - JSON output is specified with `-o json` instead of `--json` ([#98](https://github.com/koordinates/kart/issues/98))
    - Added syntax highlighting to JSON output when viewed in a terminal ([#54](https://github.com/koordinates/kart/issues/54))
    - `sno diff` JSON output layout has changed - features are now flat objects instead of GeoJSON objects. This is much more compact ([#71](https://github.com/koordinates/kart/issues/71))
    - Added JSON output option for most commands
    - Added `--json-style` option to several commands to control JSON formatting ([#70](https://github.com/koordinates/kart/issues/70))
* `sno diff`:
    - `a..b` now refers to the same changes as `sno log a..b` ([#116](https://github.com/koordinates/kart/issues/116))
    - can now diff against tree objects, particularly the empty tree ([#53](https://github.com/koordinates/kart/issues/53))
    - can now view some subset of the changes by supplying filter args, ie `[dataset[:pk]]`
* `sno commit`:
    - can now commit some subset of the changes by supplying filter args, ie `[dataset[:pk]]` ([#69](https://github.com/koordinates/kart/issues/69))
* removed `import-gpkg` command; use `import` instead ([#85](https://github.com/koordinates/kart/issues/85))
* Error messages now go to stderr instead of stdout ([#57](https://github.com/koordinates/kart/issues/57))
* Error conditions now use exit codes to indicate different types of errors ([#46](https://github.com/koordinates/kart/issues/46))

## 0.3.1

* Sno is now available on Windows 🎉 (Windows 8.1+ / Server 2016+ (64-bit))
* Updates to continuous integration — installers/archives are now built and tested with every commit for every platform.
* For macOS users, a homebrew "tap" is now available: `brew cask install koordinates/sno/sno`
* Several bug fixes

## 0.3.0

### Major changes in this release

* License: sno is now publicly available under the GPL open source license.
* Sno now has a website at [**sno.earth**](https://sno.earth)
* Standalone builds and packaging for Linux and macOS. Windows won't be far behind
* Refactoring to support alternative database working copies
* Dependency upgrades (GDAL; Git; Pygit2; Proj; Libgit2; Sqlite; and others)
* Several bug fixes

### Upgrading

If you were running a preview release, remove it before installing the new release:

```console
$ brew uninstall sno
$ brew untap koordinates/sno
```

### Repository Hosting

We have an initial preview available of our Sno repository hosting. This allows you & your team to push and pull Sno repositories. Please contact support@koordinates.com with your Github username and we can get you set up. There is no cost for this service.

### Compatibility


Repositories created with Sno v0.2 are compatible with v0.3. For assistance upgrading any v0.1 repositories, please read our [upgrade guide](https://github.com/koordinates/kart/wiki/Upgrading).


## 0.2.0

### Major changes in this release
* First and foremost, the name — we're now called Sno!
* A new repository structure/layout, which has better performance and a smaller on-disk size
* Data imports are now orders of magnitude faster
* Support for multiple datasets in a single Sno repository
* Support for non-spatial datasets
* Increased test coverage including end-to-end tests
* Improved macOS Homebrew packaging with CI testing.
* Prototype support for spatial-indexing and a sno query command for spatial lookups.
* Diffs across branches/commits, and a GeoJSON diff format.
* Numerous bug fixes
