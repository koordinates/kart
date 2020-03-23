# Contributing

We welcome all contributions, bug reports, and suggestions!

## Installing the development version

By default, vendored dependencies are downloaded from recent CI artifacts to save you a lot of time and effort building them. If you're keen, explore the make targets in `vendor/` to build/assemble them manually.

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
$ venv/bin/sno --version
Sno v0.2.1.dev0
GDAL v3.0.3
PyGit2 v0.28.2; Libgit2 v0.28.2
```

### Linux

Requirements:
* Python 3.7
* wget
* cmake

```console
$ git clone git@github.com:koordinates/sno.git
$ cd sno
$ make

# check it's working
$ venv/bin/sno --version
Sno v0.2.1.dev0
GDAL v3.0.3
PyGit2 v0.28.2; Libgit2 v0.28.2
```

### Windows

Requirements:
* Windows 10 64-bit
* MS Visual Studio 2017
* Python 3.7
* cmake
* 7-Zip

```console
$ git clone git@github.com:koordinates/sno.git
$ cd sno
$ nmake /F makefile.vc

# check it's working
$ venv\Scripts\sno --version
Sno v0.2.1.dev0
GDAL v3.0.3
PyGit2 v0.28.2; Libgit2 v0.28.2
```
