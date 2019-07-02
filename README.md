Project Snowdrop
----------------
```
      @@@
         @
          @
           @
           @@
         @@@@@
       @@@@@@@@@
      @@@@@@@@@@@@
     @@@@@@@@@@ @@@@
    @@@@@ @@@@@ @@@@
    @@@@  @@@@@@ @@@@
    @@@  @@@@@@  @@@@
    @@@@@@@@@@@   @@@
    @@   @@@@@@ @  @@
    @   @@@@@@
         @@@@
          @@
          @
```

### Distributed version-control for datasets

## Installing

### macOS

You need [Homebrew](https://brew.sh/) installed.

#### For just general running/updating
```console
$ brew tap --force-auto-update koordinates/snowdrop git@github.com:koordinates/snowdrop.git
$ brew install --HEAD snowdrop

# check it's working
$ snow --version
Project Snowdrop v0.1
GDAL v2.4.1
PyGit2 v0.28.2; Libgit2 v0.28.2
```

#### For developing snowdrop
```
$ git clone git@github.com:koordinates/snowdrop.git
$ cd snowdrop
$ brew install --only-dependencies --HEAD HomebrewFormula/snowdrop.rb

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
$ rm venv/lib/python*/site-packages/no-global-site-packages.txt

# install snowdrop
$ pip install -e .
# make snowdrop globally accessible
$ ln -sf $(pwd)/venv/bin/snow /usr/local/bin/snow
$ ln -sf $(pwd)/venv/bin/snowdrop /usr/local/bin/snowdrop

# quit the virtualenv
$ deactivate

# check it's working
$ snow --version
Project Snowdrop v0.1
GDAL v2.4.1
PyGit2 v0.28.2; Libgit2 v0.28.2
```

Sources:
* [pygit2: libgit2 within a virtual environment](https://www.pygit2.org/install.html#libgit2-within-a-virtual-environment)

### Docker

```console
$ docker build -t snowdrop .
# in repository/data directory
$ /path/to/snowdrop/snowdrop-docker.sh snow ...
```

## Usage

1. Export a GeoPackage from Koordinates
   * With a single vector layer
   * Which has a primary key
   * Get the whole layer
2. Import the GeoPackage (eg. `kx-foo-layer.gpkg`) into a Snowdrop repository.
   ```console
   # find the table name in the GeoPackage
   $ snow import-gpkg --list-tables kx-foo-layer.gpkg
   # import the layer (`foo_layer`) — the repo directory will be created:
   $ snow --repo=/path/to/kx-foo-layer.snow import-gpkg kx-foo-layer.gpkg foo_layer
   $ cd /path/to/kx-foo-layer.snow
   ```
   This will create a _bare_ git repository at `/path/to/kx-foo-layer.snow`.

   Use this repository as the directory to run all the other commands in.
3. Checkout a working copy to edit in eg. QGIS
   ```console
   # find/check the table name in the geopackage
   $ snow checkout --layer=foo_layer --working-copy=/path/to/foo.gpkg
   ```
4. Editing in QGIS/etc:
   * will track changes in the `__kxg_*` tables
   * additions/edits/deletes of features are supported
   * changing feature PKs is supported
   * schema changes should be detected, but aren't supported yet (will error).
   * Use F5 to refresh your QGIS map after changing the underlying working-copy data using `snow`.
6. With your working copy, `snow` commands should work if run from the `kx-foo-layer.snow/` folder. Check `--help` for options, the most important ones are supported. In some cases options are passed straight through to an underlying git command:
    * `snow diff` diff the working copy against the repository (no index!)
    * `snow commit -m {message}` commit outstanding changes from the working copy
    * `snow log` review commit history
    * `snow branch` & `snow checkout -b` branch management
    * `snow fetch` fetch upstream changes.
    * `snow status` show working copy state.
    * `snow reset` discard changes in the working copy.
    * `snow merge` merge. Supports `--ff`/`--no-ff`/`--ff-only` from one merge source.
    * `snow tag ...`
    * `snow remote ...`. Remember simple remotes can just be another local directory.
    * `snow push` / `snow pull`
7. Other git commands will _possibly_ work if run from the `kx-foo-layer.snow/` folder. eg:
    * `git reset --soft {commitish}`
8. If you need a remote, head to https://kxgit-gitea.kx.gd and create a repository. Add it as a remote via:
   ```console
   $ git remote add origin https://kxgit-gitea.kx.gd/myuser/myrepo.git
   # enter your gitea username/password when prompted
   $ snow push --all --set-upstream origin
   ```
