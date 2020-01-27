# Contributing

We welcome all contributions, bug reports, and suggestions!

## Installing the development version

### macOS

```console
$ git clone git@github.com:koordinates/sno.git
$ cd sno
$ make install

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
$ make docker
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
