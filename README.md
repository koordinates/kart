Sno: Distributed version-control for datasets
---------------------------------------------

[![Build status](https://badge.buildkite.com/621292fbfad27fe132e84c142ad0618d2a50375c29266d83a1.svg)](https://buildkite.com/koordinates/sno)

## Installing

### macOS

You need [Homebrew](https://brew.sh/) installed.

#### For just general running/updating
```console
$ brew tap --force-auto-update koordinates/sno git@github.com:koordinates/sno.git
$ brew install --HEAD sno

# check it's working
$ sno --version
Project Sno v0.1
GDAL v2.4.1
PyGit2 v0.28.2; Libgit2 v0.28.2
```

#### For developing Sno
```
$ git clone git@github.com:koordinates/sno.git
$ cd sno
$ brew install --only-dependencies --HEAD HomebrewFormula/sno.rb

# create our virtualenv
$ virtualenv --python=python3.7 ./venv
$ source venv/bin/activate

# get libgit2/pygit2 stuff
$ mkdir vendor
$ git clone --branch=kx-0.28 git@github.com:koordinates/libgit2.git vendor/libgit2
$ git clone --branch=kx-0.28 git@github.com:koordinates/pygit2.git vendor/pygit2

# build libgit2
$ cd vendor/libgit2
$ export LIBGIT2=$VIRTUAL_ENV
$ cmake . -DCMAKE_INSTALL_PREFIX=$LIBGIT2
$ make
$ make install

# build pygit2
$ cd ../../vendor/pygit2
$ export LIBGIT2=$VIRTUAL_ENV
$ export LDFLAGS="-Wl,-rpath,'$LIBGIT2/lib' $LDFLAGS"
$ pip install .

# install other dependencies
$ cd ../..
$ pip install pygdal=="$(gdal-config --version).*"
$ pip install -r requirements-dev.txt
$ rm venv/lib/python*/no-global-site-packages.txt

# install sno
$ pip install -e .
# make sno globally accessible
$ ln -sf $(pwd)/venv/bin/sno /usr/local/bin/sno

# quit the virtualenv
$ deactivate

# check it's working
$ sno --version
Sno v0.1
GDAL v2.4.1
PyGit2 v0.28.2; Libgit2 v0.28.2
```

Sources:
* [pygit2: libgit2 within a virtual environment](https://www.pygit2.org/install.html#libgit2-within-a-virtual-environment)

### Docker

```console
$ docker build -t sno .
# in repository/data directory
$ /path/to/sno/sno-docker.sh sno ...
```

## Usage

1. Export a GeoPackage from Koordinates
   * With a single vector layer
   * Which has a primary key
   * Get the whole layer
2. Create a new Sno repository and import the GeoPackage (eg. `kx-foo-layer.gpkg`).
   ```console
   $ mkdir myproject
   $ cd myproject
   $ sno init --import GPKG:kx-foo-layer.gpkg
   ```
   Use this repository as the directory to run all the other commands in.
   This will also create a working copy as `myproject/myproject.gpkg` to edit.
4. Editing in QGIS/etc:
   * will track changes in the `.sno-*` tables
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
    * `sno reset` discard changes in the working copy.
    * `sno merge` merge. Supports `--ff`/`--no-ff`/`--ff-only` from one merge source.
    * `sno tag ...`
    * `sno remote ...`. Remember simple remotes can just be another local directory.
    * `sno push` / `sno pull`
    * `sno clone` initialise a new repository from a remote URL,
6. Other git commands will _possibly_ work if run from the `myproject/` folder. eg:
    * `git reset --soft {commitish}`
7. If you need a remote, head to https://kxgit-gitea.kx.gd and create a repository. Add it as a remote via:
   ```console
   $ git remote add origin https://kxgit-gitea.kx.gd/myuser/myrepo.git
   # enter your gitea username/password when prompted
   $ sno push --all --set-upstream origin
   ```
