Sno: Distributed version-control for datasets
---------------------------------------------

[![Docker/Linux](https://badge.buildkite.com/621292fbfad27fe132e84c142ad0618d2a50375c29266d83a1.svg)](https://buildkite.com/koordinates/sno)
[![Homebrew/macOS](https://github.com/koordinates/sno/workflows/Homebrew/badge.svg)](https://github.com/koordinates/sno/actions?query=workflow%3AHomebrew)


## Installing

> ##### ℹ️ If you're new to git
> You need to have [an SSH key added in Github and available on your computer](https://help.github.com/en/github/authenticating-to-github/connecting-to-github-with-ssh) to be able to install Sno during the preview period.

### macOS

You need [Homebrew](https://brew.sh/) installed.

#### For just general running/updating
```console
$ brew tap --force-auto-update koordinates/sno git@github.com:koordinates/sno.git
$ brew install --HEAD sno

# check it's working
$ sno --version
Project Sno v0.2.0
GDAL v2.4.2
PyGit2 v0.28.2; Libgit2 v0.28.2
```

#### For developing Sno
```
$ git clone git@github.com:koordinates/sno.git
$ cd sno
$ brew install --only-dependencies --HEAD HomebrewFormula/sno.rb

# create our virtualenv
$ python3 -m venv --clear ./venv
$ source venv/bin/activate

# install python dependencies
$ pip install pygdal=="$(gdal-config --version).*"
$ pip install -r requirements-dev.txt

# get libgit2/pygit2 stuff
$ mkdir vendor
$ git clone --branch=kx-0.28 git@github.com:koordinates/libgit2.git vendor/libgit2
$ git clone --branch=kx-0.28 git@github.com:koordinates/pygit2.git vendor/pygit2

# build libgit2
$ pushd vendor/libgit2
$ export LIBGIT2=$VIRTUAL_ENV
$ cmake . -DCMAKE_INSTALL_PREFIX=$LIBGIT2
$ make
$ make install
$ popd

# build pygit2
$ pushd vendor/pygit2
$ export LIBGIT2=$VIRTUAL_ENV
$ export LDFLAGS="-Wl,-rpath,'$LIBGIT2/lib' $LDFLAGS"
$ pip install .
$ popd

# install sno
$ pip install -e .
# make sno globally accessible
$ ln -sf $(pwd)/venv/bin/sno /usr/local/bin/sno

# quit the virtualenv
$ deactivate

# check it's working
$ sno --version
Sno v0.2.0
GDAL v2.4.2
PyGit2 v0.28.2; Libgit2 v0.28.2
```

Sources:
* [pygit2: libgit2 within a virtual environment](https://www.pygit2.org/install.html#libgit2-within-a-virtual-environment)

### Docker

```console
$ docker build -t sno .
# in your repository directory
$ /path/to/sno/sno-docker.sh sno --version
Sno v0.2.0
GDAL v2.4.2
PyGit2 v0.28.2; Libgit2 v0.28.2
```

## Usage

See the [documentation](https://github.com/koordinates/sno/wiki) for tutorials and reference.

> ##### ℹ️ If you're new to git
> Configure the identity you will use for Sno commits with:
> ```console
> $ git config --global user.email "you@example.com"
> $ git config --global user.name "Your Name"
> ```

## Quick Start

1. Export a GeoPackage from [Koordinates](https://koordinates.com/) with any combination of vector layers and tables.
2. Create a new Sno repository and import the GeoPackage (eg. `kx-foo-layer.gpkg`).
   ```console
   $ mkdir myproject
   $ cd myproject
   $ sno init --import GPKG:kx-foo-layer.gpkg
   ```
   Use this repository as the directory to run all the other commands in.
   This will also create a working copy as `myproject/myproject.gpkg` to edit.
4. Editing the working copy in QGIS/etc:
   * will track changes in the internal `.sno-*` tables
   * additions/edits/deletes of features are supported
   * changing feature PKs is supported
   * schema changes should be detected, but aren't supported yet (will error).
   * Use F5 to refresh your QGIS map after changing the underlying working-copy data using `sno`.
5. With your working copy, `sno` commands should work if run from the `myproject/` folder. Check `--help` for options, the most important ones are supported. In some cases options are passed straight through to an underlying git command:
    * `sno diff` diff the working copy against the repository (no index!)
    * `sno commit -m {message}` commit outstanding changes from the working copy
    * `sno log` review commit history
    * `sno branch` & `sno checkout -b` branch management
    * `sno fetch` fetch upstream changes.
    * `sno status` show working copy state.
    * `sno merge` merge. Supports `--ff`/`--no-ff`/`--ff-only` from one merge source.
    * `sno switch` switch to existing or new branches.
    * `sno reset` & `sno restore` discard changes in the working copy.
    * `sno tag ...`
    * `sno remote ...`. Remember simple remotes can just be another local directory.
    * `sno push` / `sno pull`
    * `sno clone` initialise a new repository from a remote URL,
