GDAL/OGR Plugin
===============

Experimental [GDAL/OGR](https://gdal.org) plugin driver for Kart repositories. This is mostly to provide a proof of concept for discussion and to support future feature development in Kart.

This is implemented using the [OGR Python Drivers](https://gdal.org/development/rfc/rfc76_ogrpythondrivers.html#rfc-76-ogr-python-drivers) mechanism in GDAL which is fairly rough & inefficient. It also means the driver is read-only.

> **ℹ️** This plugin requires GDAL v3.8.0 or newer.

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

There was previously an example of a Kart plugin that runs in the same process as GDAL, ie, the GDAL python driver imports and runs Kart python code rather that running a separate Kart subprocess. This worked if the calling GDAL was the same as the GDAL bundled with Kart - however it has been removed since the GDAL bundled with Kart is currently built with `GDAL_AUTOLOAD_PLUGINS=OFF` which prevents the loading of any python drivers.
