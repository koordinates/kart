# Contributing

We welcome all contributions, bug reports, and suggestions!

* Ask support and usage questions in [Discussions](https://github.com/koordinates/kart/discussions)
* Read and submit bug reports or feature requests at [Issues](https://github.com/koordinates/kart/issues)

We're moving to CMake as a better system for building Kart and its
dependencies. Currently CMake is still a work in progress, only supports macOS
and Linux, and doesn't yet create packages suitable for distribution.

## Installing the development version with CMake

Requirements:
* CMake >= v3.21
* GDAL >= v3.3.2
* Git >= v2.31
* LibGit2 >= v1.1.0
* OpenSSL >= v1.1
* PostgreSQL client library (libpq)
* Python 3.7
* Spatialindex >= v1.9.3
* SpatiaLite >= v5.0.1
* SQLite3 >= v3.31.1
* SWIG
* unixODBC >= v2.3.9 (macOS/Linux only)

### Installing dependencies on macOS

If you're a Homebrew user, you can get all of those via:

```console
$ brew install --upgrade cmake gdal git openssl@1.1 libpq python@3.7 \
    spatialindex libspatialite sqlite3 swig unixodbc
```

Then configure Kart:
```
$ cmake -B build -S .
```

### Installing dependencies on Linux

Ubuntu Focal using [UbuntuGIS](https://wiki.ubuntu.com/UbuntuGIS):
```console
$ sudo apt-get install software-properties-common
$ wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | gpg --dearmor - \
    | sudo tee /etc/apt/trusted.gpg.d/kitware.gpg >/dev/null
$ sudo apt-add-repository "deb https://apt.kitware.com/ubuntu/ $(lsb_release -cs) main"
$ sudo add-apt-repository ppa:ubuntugis/ubuntugis-unstable
$ sudo add-apt-repository ppa:git-core/ppa
$ sudo apt-get install build-essential cmake ccache libgdal-dev gdal-data git \
    libssl-dev libpq-dev python3.8-dev python3.8-venv \
    libspatialindex-dev libsqlite3-mod-spatialite sqlite3 swig4.0 unixodbc
```

Then [build and install libgit2](https://libgit2.org/docs/guides/build-and-link/#basic-build) v1.3
from the [Koordinates `kx-latest` branch](https://github.com/koordinates/libgit2/tree/kx-latest):

```console
$ git clone --branch=kx-latest https://github.com/koordinates/libgit2.git
$ cd libgit2
$ cmake -B build -S . -DBUILD_CLAR=OFF
$ cmake --build build
$ cmake --install build
```

Then configure Kart:
```console
$ cmake -B build -S . \
    -DSpatiaLite_EXTENSION=/usr/lib/x86_64-linux-gnu/mod_spatialite.so \
    -DPROJ_DATADIR=/usr/share/proj
```

### Building

```console
$ cd build
$ make
$ ./kart --version
```

### Downloading vendor dependencies from CI

If you're having issues with the above, you can download a [recent master-branch
vendor CI artifact](https://github.com/koordinates/kart/actions/workflows/build.yml?query=branch%3Amaster+is%3Asuccess) for your platform (`vendor-Darwin` for macOS,
or `vendor-Linux` for Linux). Then:

```console
$ cmake -B build -S . -DVENDOR_ARCHIVE=/path/to/downloaded/vendor-Darwin.zip
$ cd build
$ make
$ ./kart --version
```

Note you'll need to have (and configure) the same version of Python that CI
currently uses (3.7).

### Running the tests

```console
$ ctest -V  # run the tests
```

If you don't use the CI vendor dependencies archive, currently a few test failures are expected.
This will be cleaned up soon.

* macOS

    ```
    tests/test_core.py::test_proj_transformation_grid
    tests/test_spatial_tree.py::test_index_points_all
    tests/test_spatial_tree.py::test_index_points_commit_by_commit
    tests/test_spatial_tree.py::test_index_points_idempotent
    tests/test_spatial_tree.py::test_index_polygons_all
    tests/test_spatial_filters.py::test_git_spatial_filter_extension
    ```

* Linux

    ```
    tests/test_annotations.py::test_diff_feature_count_with_readonly_repo_dir
    tests/test_annotations.py::test_diff_feature_count_with_readonly_annotations
    tests/test_spatial_filters.py::test_git_spatial_filter_extension
    ```

## Installing the development version the legacy way

By default, vendored dependencies are downloaded from recent CI artifacts to save you a lot of time and effort building them.

If for some reason you do need to build them locally, `cd vendor` and run:

* MacOS/Linux: `make "build-$(uname -s)"`
* Windows: `nmake /f makefile.vc`

### macOS

Requirements (install via Homebrew/somehow):
* Python 3.7
* wget
* jq

```console
$ git clone git@github.com:koordinates/kart.git
$ cd kart
$ make

# check it's working
$ venv/bin/kart --version
Kart v0.9.1.dev0, Copyright (c) Kart Contributors
» GDAL v3.0.4
» PyGit2 v1.1.0; Libgit2 v0.99.0; Git v2.25.1.windows.1
» APSW v3.30.1-r3; SQLite v3.30.1; SpatiaLite v5.0.0-beta0
» SpatialIndex v1.8.5
```

### Linux

Requirements:
* Python 3.7
* wget

```console
$ git clone git@github.com:koordinates/kart.git
$ cd kart
$ make

# check it's working
$ venv/bin/kart --version
Kart v0.9.1.dev0, Copyright (c) Kart Contributors
» GDAL v3.0.4
» PyGit2 v1.1.0; Libgit2 v0.99.0; Git v2.25.1.windows.1
» APSW v3.30.1-r3; SQLite v3.30.1; SpatiaLite v5.0.0-beta0
» SpatialIndex v1.8.5
```

### Windows

Requirements:
* Windows 64-bit 8.1 / Windows Server 64-bit 2016; or newer
* MS Visual Studio 2017 or newer, with C++ tools installed
* Python 3.7 from [Python.org](https://python.org)
* cmake 3.15+
* 7-Zip
* Git

Run the following from the "x64 Native Tools Command Prompt for VS 2019":

```console
> git clone git@github.com:koordinates/kart.git
> cd kart
> nmake /F makefile.vc

# check it's working
> venv\Scripts\kart --version
Kart v0.9.1.dev0, Copyright (c) Kart Contributors
» GDAL v3.0.4
» PyGit2 v1.1.0; Libgit2 v0.99.0; Git v2.25.1.windows.1
» APSW v3.30.1-r3; SQLite v3.30.1; SpatiaLite v5.0.0-beta0
» SpatialIndex v1.8.5
```

## CI

Continuous integration builds apps, tests, and installers for every commit on supported platforms. Artifacts are published to Github Actions, including vendor library bundles, test results, and unsigned installers.

To only run CI for a particular platform (ie. when debugging CI), add `[ci only posix]` (for macOS + Linux) or `[ci only windows]` to commit messages.

## Code formatting

We use [Black](https://github.com/psf/black) to ensure consistent code formatting. We recommend integrating black with your editor:

* Sublime Text: install [sublack](https://packagecontrol.io/packages/sublack) via Package Control
* VSCode [instructions](https://code.visualstudio.com/docs/python/editing#_formatting)

We use the default settings, and target python 3.7+.

One easy solution is to install [pre-commit](https://pre-commit.com), run `pre-commit install --install-hooks` and it'll automatically validate your changes code as a git pre-commit hook.
