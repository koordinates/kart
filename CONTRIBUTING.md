# Contributing

We welcome all contributions, bug reports, and suggestions!

## Installing the development version

By default, vendored dependencies are downloaded from recent CI artifacts to save you a lot of time and effort building them. If you're keen, explore the make targets in `vendor/` to build/assemble them manually.

### macOS

Requirements (install via Homebrew/somehow):
* Python 3.7
* wget
* jq

```console
$ git clone git@github.com:koordinates/sno.git
$ cd sno
$ make

# check it's working
$ venv/bin/sno --version
Sno v0.3.1.dev0, Copyright (c) Sno Contributors
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
$ git clone git@github.com:koordinates/sno.git
$ cd sno
$ make

# check it's working
$ venv/bin/sno --version
Sno v0.3.1.dev0, Copyright (c) Sno Contributors
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
> git clone git@github.com:koordinates/sno.git
> cd sno
> nmake /F makefile.vc

# check it's working
> venv\Scripts\sno --version
Sno v0.3.1.dev0, Copyright (c) Sno Contributors
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
