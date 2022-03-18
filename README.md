<img title="Kart logo" width="50%" src="https://kartproject.org/images/kart-github.png">

## Kart: Distributed version-control for datasets

![Build](https://github.com/koordinates/kart/workflows/Build/badge.svg?event=push)
![Docs](https://readthedocs.org/projects/kart-project/badge/?version=latest&style=flat)

## Installing

### Upgrading to v0.11.1

See the [v0.11.1 release notes](https://github.com/koordinates/kart/releases/tag/v0.11.1) for changes, upgrading, and compatibility notes.

### Windows

Download the .msi installer from the [release page](https://github.com/koordinates/kart/releases/tag/v0.11.1).

> 💡 If Windows Defender SmartScreen says "it prevented an unrecognized app from starting" after downloading, you'll need to click "Run anyway".

### macOS

Download the .pkg installer from the [release page](https://github.com/koordinates/kart/releases/tag/v0.11.1);

Or use [Homebrew](https://brew.sh) to install: `brew install koordinates/kart/kart`

### Linux

For Debian/Ubuntu-based distributions, download the .deb package from the [release page](https://github.com/koordinates/kart/releases/tag/v0.11.1) and install via `dpkg -i kart_*.deb`.

For RPM-based distributions, download the .rpm package from the [release page](https://github.com/koordinates/kart/releases/tag/v0.11.1) and install via `rpm -i kart-*.rpm`.

### Source

For Kart development see the [Contributing Notes](CONTRIBUTING.md).

## Usage

See the [documentation](https://github.com/koordinates/kart/wiki) for tutorials and reference.

> ##### 💡 If you're new to git
>
> Configure the identity you will use for Kart commits with:
>
> ```console
> $ kart config --global user.email "you@example.com"
> $ kart config --global user.name "Your Name"
> ```

#### Quick Start

1. Export a GeoPackage from [Koordinates](https://koordinates.com/) with any combination of vector layers and tables.
2. Create a new Kart repository and import the GeoPackage (eg. `kx-foo-layer.gpkg`).
   ```console
   $ kart init myproject --import GPKG:kx-foo-layer.gpkg
   $ cd myproject
   ```
   Use this repository as the directory to run all the other commands in.
   This will also create a working copy as `myproject/myproject.gpkg` to edit.
3. Editing the working copy in QGIS/etc:
   - will track changes in the internal `kart` tables
   - additions/edits/deletes of features are supported
   - changing feature PKs is supported
   - schema changes should be detected, but aren't supported yet (will error).
   - Use F5 to refresh your QGIS map after changing the underlying working-copy data using `kart`.
4. With your working copy, `kart` commands should work if run from the `myproject/` folder. Check `--help` for options, the most important ones are supported. In some cases options are passed straight through to an underlying git command:
   - `kart diff` diff the working copy against the repository (no index!)
   - `kart commit -m {message}` commit outstanding changes from the working copy
   - `kart log` review commit history
   - `kart branch` & `kart checkout -b` branch management
   - `kart fetch` fetch upstream changes.
   - `kart status` show working copy state.
   - `kart merge` merge. Supports `--ff`/`--no-ff`/`--ff-only` from one merge source.
   - `kart switch` switch to existing or new branches.
   - `kart reset` & `kart restore` discard changes in the working copy.
   - `kart tag ...`
   - `kart remote ...`. Remember simple remotes can just be another local directory.
   - `kart push` / `kart pull`
   - `kart clone` initialise a new repository from a remote URL,

## License

#### GPLv2 with linking exception

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License,
version 2, as published by the Free Software Foundation.

In addition to the permissions in the GNU General Public License,
the authors give you unlimited permission to link the compiled
version of this file into combinations with other programs,
and to distribute those combinations without any restriction
coming from the use of this file. (The General Public License
restrictions do apply in other respects; for example, they cover
modification of the file, and distribution when not linked into
a combined executable.)

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; see the file [`COPYING`](COPYING). If not, write to
the Free Software Foundation, 51 Franklin Street, Fifth Floor,
Boston, MA 02110-1301, USA.
