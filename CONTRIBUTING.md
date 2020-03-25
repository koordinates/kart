# Contributing

We welcome all contributions, bug reports, and suggestions!

## Installing the development version

### macOS

Requirements (install via Homebrew/somehow):
* Python 3.7
* wget
* cmake

```console
$ git clone git@github.com:koordinates/sno.git
$ cd sno
$ make

# check it's working
$ sno --version
Sno v0.2.1.dev0
GDAL v3.0.3
PyGit2 v0.28.2; Libgit2 v0.28.2
```

### Docker

```console
$ git clone git@github.com:koordinates/sno.git
$ cd sno
$ make docker
# in your repository directory
$ /path/to/sno/sno-docker.sh sno --version
Sno v0.2.1.dev0
GDAL v3.0.3
PyGit2 v0.28.2; Libgit2 v0.28.2
```


## Code formatting

We use [Black](https://github.com/psf/black) to ensure consistent code formatting. We recommend integrating black with your editor:

* Sublime Text: install [sublack](https://packagecontrol.io/packages/sublack) via Package Control
* VSCode [instructions](https://code.visualstudio.com/docs/python/editing#_formatting)

We use the default settings, and target python 3.7+.