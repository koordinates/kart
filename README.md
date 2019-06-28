SnowDrop
--------
```
      @@@
         @
          @
           @
           @@
          @@@@
         @@@@@
       @@@@@@@@@
      @@@@@@@@@@@@
     @@@@@@@@@@@@@@
    @@@@@@@@@@@ @@@@
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

### Koordinates git-like Sync Client (experiments)

## Installing

### macOS

You need [Homebrew](https://brew.sh/) installed.

#### For just general running/updating
```console
$ brew tap --force-auto-update koordinates/snowdrop git@github.com:koordinates/snowdrop.git
$ brew install --HEAD kxgit

# check it's working (these should be the version numbers)
$ kxgit --version
kxgit proof of concept
GDAL v2.4.1
PyGit2 v0.28.2; Libgit2 v0.28.0
```

#### For developing snowdrop
```
$ git clone git@github.com:koordinates/snowdrop.git --branch=gitlike-2019
$ cd snowdrop
$ brew install --only-dependencies --HEAD HomebrewFormula/kxgit.rb

# create our virtualenv
$ virtualenv --python=python3.7 ./venv
$ source venv/bin/activate

# get libgit2/pygit2 stuff
$ mkdir vendor
$ git clone --branch=kx-0.28 git@github.com:rcoup/libgit2.git vendor/libgit2
$ git clone --branch=kx-0.28 git@github.com:rcoup/pygit2.git vendor/pygit2

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

# install kxgit
$ pip install -e .
# make kxgit globally accessible
$ ln -sf $(pwd)/venv/bin/kxgit /usr/local/bin/kxgit

# quit the virtualenv
$ deactivate

# check it's working (these should be the version numbers)
$ kxgit --version
kxgit proof of concept
GDAL v2.4.1
PyGit2 v0.28.2; Libgit2 v0.28.0
```

Sources:
* [pygit2: libgit2 within a virtual environment](https://www.pygit2.org/install.html#libgit2-within-a-virtual-environment)

### Docker

```console
$ docker build -t snowdrop .
# in repository/data directory
$ /path/to/snowdrop/kxgit-docker.sh kxgit ...
```

## Usage

1. Export a GeoPackage from Koordinates
   * With a single vector layer
   * Which has a primary key
   * Get the whole layer
2. Import the GeoPackage (eg. `kx-foo-layer.gpkg`) into a kxgit repository.
   ```console
   # find/check the table name in the Kx GeoPackage
   $ sqlite3 kx-foo-layer.gpkg 'SELECT table_name FROM gpkg_contents;'
   # repo directory will be created
   $ kxgit --repo=/path/to/kx-foo-layer.git import-gpkg kx-foo-layer.gpkg foo_layer
   $ cd /path/to/kx-foo-layer.git
   ```
   This will create a _bare_ git repository at `/path/to/kx-foo-layer.git`. Normal git tools & commands will work in there (mostly).

   Use this repository as the directory to run all the other commands in.
3. Checkout a working copy to edit in eg. QGIS
   ```console
   # find/check the table name in the geopackage
   $ kxgit checkout --layer=foo_layer --working-copy=/path/to/foo.gpkg
   ```
4. Editing in QGIS/etc:
   * will track changes in the `__kxg_*` tables
   * additions/edits/deletes of features are supported
   * changing feature PKs is supported
   * schema changes should be detected, but aren't supported yet (will error).
   * Use F5 to refresh your QGIS map after changing the underlying working-copy data using `kxgit`.
6. With your working copy, `kxgit` commands should work if run from the `kx-foo-layer.git/` folder. Check `--help` for options, the most important ones are supported, and in some cases options are passed straight through to the underlying git command:
    * `kxgit diff` diff the working copy against the repository (no index!)
    * `kxgit commit -m {message}` commit outstanding changes from the working copy
    * `kxgit log` review commit history
    * `kxgit branch` & `kxgit checkout -b` branch management
    * `kxgit fetch` fetch upstream changes.
    * `kxgit merge` merge. Supports `--ff`/`--no-ff`/`--ff-only` from one merge source.
    * `kxgit push`
7. Other git commands will _probably_ work if run from the `kx-foo-layer.git/` folder. eg:
    * `git remote ...`. Remember simple remotes can just be another local directory.
    * `git reset --soft {commitish}`
    * `git tag ...`
8. If you need a remote, head to https://kxgit-gitea.kx.gd and create a repository. Add it as a remote via:
   ```console
   $ git remote add origin https://kxgit-gitea.kx.gd/myuser/myrepo.git
   # enter your gitea username/password when prompted
   $ kxgit push --all --set-upstream origin
   ```
