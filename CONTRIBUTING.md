# Contributing

We welcome all contributions, bug reports, and suggestions!

## Installing the development version

### macOS

```console
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
Sno v0.3.0.dev0
GDAL v2.4.2
PyGit2 v0.28.2; Libgit2 v0.28.2
```

Sources:
* [pygit2: libgit2 within a virtual environment](https://www.pygit2.org/install.html#libgit2-within-a-virtual-environment)

### Docker

```console
$ git clone git@github.com:koordinates/sno.git
$ cd sno
$ docker build -t sno .
# in your repository directory
$ /path/to/sno/sno-docker.sh sno --version
Sno v0.3.0.dev0
GDAL v2.4.2
PyGit2 v0.28.2; Libgit2 v0.28.2
```

### Installing a development branch using Homebrew

```console
$ git clone git@github.com:koordinates/sno.git
$ cd sno
# use git to checkout whatever branch/tag you want
$ git checkout -b somebranch origin/somebranch

# unlink any existing Homebrew version
$ brew unlink sno

$ brew install --devel HomebrewFormula/sno.rb
```

To swap back to stable:
```console
# get available versions
$ brew switch sno 0
Error: sno does not have a version "0" in the Cellar.
sno installed versions: 0.0.0+git.d6797cf, 0.2.0-rc.2

$ brew switch sno 0.2.0
```
