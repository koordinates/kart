Sno: Distributed version-control for datasets
---------------------------------------------

[![Homebrew/macOS](https://github.com/koordinates/sno/workflows/Homebrew%20Dev/badge.svg)](https://github.com/koordinates/sno/actions)
[![Docker/Linux](https://badge.buildkite.com/621292fbfad27fe132e84c142ad0618d2a50375c29266d83a1.svg)](https://buildkite.com/koordinates/sno)


## Installing

> ##### ℹ️ If you're new to git
> You need to have [an SSH key added in Github and available on your computer](https://help.github.com/en/github/authenticating-to-github/connecting-to-github-with-ssh) to be able to install Sno during the preview period.

### macOS

You need [Homebrew](https://brew.sh/) installed.

#### For general running/updating from the latest release (recommended)
```console
$ brew tap koordinates/sno git@github.com:koordinates/sno.git
$ brew install sno

# check it's working
$ sno --version
Project Sno v0.2.0
GDAL v2.4.2
PyGit2 v0.28.2; Libgit2 v0.28.2
```

To upgrade:
```console
$ brew upgrade sno
```

#### For the latest development work
```console
$ brew tap koordinates/sno git@github.com:koordinates/sno.git
$ brew install --HEAD sno

# check it's working
$ sno --version
Project Sno v0.3.0.dev0
GDAL v2.4.2
PyGit2 v0.28.2; Libgit2 v0.28.2
```

To upgrade:
```console
$ brew reinstall sno
```

To build a Docker container, install from source, for Sno development see the [Contributing Notes](CONTRIBUTING.md).

## Usage

See the [documentation](https://github.com/koordinates/sno/wiki) for tutorials and reference.

> ##### ℹ️ If you're new to git
> Configure the identity you will use for Sno commits with:
> ```console
> $ git config --global user.email "you@example.com"
> $ git config --global user.name "Your Name"
> ```

#### Quick Start

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
