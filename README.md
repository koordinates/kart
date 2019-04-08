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

### Max OS X

```console
$ git clone git@github.com:koordinates/snowdrop.git
$ cd snowdrop
$ brew install sqlite3

# make our virtualenv
$ virtualenv --python=python3.7 ./venv
$ source venv/bin/activate

# get libgit2/pygit2 stuff
$ brew install --only-dependencies libgit2
$ mkdir vendor
$ git clone git@github.com:libgit2/libgit2.git vendor/libgit2
$ git clone git@github.com:libgit2/pygit2.git vendor/pygit2

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
$ pip install -r requirements.txt
```

Sources:
* [pygit2: libgit2 within a virtual environment](https://www.pygit2.org/install.html#libgit2-within-a-virtual-environment)

## Usage

1. Export a GeoPackage from Koordinates
   * With a single vector layer
   * Which has a primary key
   * Get the whole layer
2. Import the GeoPackage into a kxgit repository
   ```console
   # find/check the table name in the Kx GeoPackage
   $ sqlite3 kx-foo-layer.gpkg 'SELECT table_name FROM gpkg_contents;'
   $ kxgit --repo=/path/to/kx-foo-layer.git import-gpkg kx-foo-layer.gpkg foo_layer
   $ cd /path/to/kx-foo-layer.git
   ```
   This will create a _bare_ git repository at `/path/to/kx-foo-layer.git`. Normal git tools & commands will work in there (mostly).
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
