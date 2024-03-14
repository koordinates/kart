GDAL/OGR Plugins
================

Experimental [GDAL/OGR](https://gdal.org) plugin drivers for Kart repositories. These are mostly to provide a proof of concept for discussion and to support future feature development in Kart.

They are implemented using the [OGR Python Drivers](https://gdal.org/development/rfc/rfc76_ogrpythondrivers.html#rfc-76-ogr-python-drivers) mechanism in GDAL which is fairly rough & inefficient. It also means the drivers are read-only.

> **ℹ️** These plugins require GDAL v3.8.0 or newer.

CLI
---

The CLI plugin calls Kart CLI commands to find and expose features/layers, and diffs between revisions. Since Kart _uses_ GDAL this provides a relatively clean process boundary, similar to how VSCode, Github Desktop, and other IDEs interact with Git — and how it's used server-side for many forge operations at the likes of Github & Gitlab.

As long as the interprocess communication is efficient using this mechanism should be a relatively smooth path to implement a fully-featured GDAL/OGR driver in the future.

### Usage

```console
# Tell GDAL where to find the Kart driver
$ export GDAL_PYTHON_DRIVER_PATH=/path/to/kart/contrib/gdal/cli

# KART should appear at the bottom of the list
$ ogrinfo --formats

# list datasets
$ ogrinfo /path/to/kart/repo

# list information about a dataset
$ ogrinfo -so -al /path/to/kart/repo mydataset

# convert the dataset to another OGR format
$ ogr2ogr -f SHP mydataset.shp /path/to/kart/repo mydataset
```

You can pass the dataset open option `GEOMTYPE=MULTILINESTRING`/etc to override the layer geometry type for operations that require a single geometry type: Kart datasets can support generic geometry types.

> **ℹ️** If you run into odd behaviour, set the environment variable `CPL_DEBUG=ON` and debug messages will be printed to stderr.

> **ℹ️** If you get an error `ERROR 1: Cannot find python/libpython. You can set the PYTHONSO configuration option to point to the a python .so/.dll/.dylib` then you need to set the `PYTHONSO` environment variable:
> ```console
> # Linux: find path to the appropriate libpython3.*.so
> $ export PYTHONSO=$(python3 -c 'from sysconfig import get_config_var; print("%s/%s" % (get_config_var("LIBDIR"), get_config_var("INSTSONAME")))')
>
> # macOS: the interpreter works as libpython
> $ export PYTHONSO=$(command -v python3)
> ```

### Referring to specific revisions

You can use the `/path/to/repo@COMMITISH` syntax, where commitish is one of the options from [Revision Selection](https://git-scm.com/book/en/v2/Git-Tools-Revision-Selection#_revision_selection). By default the commit used is the `HEAD` of the current branch (or the default branch for a bare repository).

* `/path/to/repo@main` tip of the `main` branch
* `/path/to/repo@v1.2` the `v1.2` tag
* `/path/to/repo@c0ffeebde6b7a4f2e39bd23ffb6b70637b5b3db8` a full commit hash
* `/path/to/repo@c0ffee` an abbreviated commit hash
* `/path/to/repo@main~3` the 3rd ancestor of main

### Changesets as OGR layers

You can use the `/path/to/repo@RANGE` syntax, where range is a double-dot or triple-dot [commit range](https://git-scm.com/book/en/v2/Git-Tools-Revision-Selection#_commit_ranges):

* `/path/to/repo@main..new` Changes in the `new` branch that aren't in `main`.
* `/path/to/repo@new..main` Changes in the `main` branch that aren't in `new`.
* `/path/to/repo@main...new` Changes that occurred on either branch since when the branches diverged.
* `/path/to/repo@HEAD^..HEAD` Changes between the previous and current commit.

You can use any commitish on either side of the range to refer to a commit. If ommitted, a commitish defaults to the `HEAD` of the current branch (or the default branch for a bare repository).


In-Process
----------

The in-process plugin uses Kart internal Python APIs to find and expose features/layers. Since Kart uses GDAL and GDAL is calling Kart, you can only use the GDAL library, tools, and associated drivers bundled with Kart.

### Usage

This currently only works from a local build tree in `/path/to/kart/build/`. See [CONTRIBUTING](../../CONTRIBUTING.md) for details. GDAL v3.8.0 isn't currently built with Kart currently, so this will require some changes in `vcpkg-vendor/` to make it work.

```console
# Configure paths
$ source /path/to/kart/contrib/gdal/in_process/devenv.sh

# KART should appear at the bottom of the list
$ ogrinfo --formats

# list datasets
$ ogrinfo /path/to/kart/repo

# list information about a dataset
$ ogrinfo -so -al /path/to/kart/repo mydataset

# convert the dataset to another OGR format
$ ogr2ogr -f SHP mydataset.shp /path/to/kart/repo mydataset
```

Changesets as OGR layers aren't available for the in-process plugin, though the same approach as in the CLI plugin is possible to implement.
