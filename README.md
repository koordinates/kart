Snowdrop
========

Proof of concept of a desktop sync client for Koordinates data.

[!Quick Demo Screencast](https://slack-files.com/T051EMLQG-FBTHZGJPN-4b8b33cdcd)

How it works
------------

You'll need a GeoPackage exported from a Koordinate site with one or more full-layer vector/table layers, an API key for that site (WXS + Layers r/o scopes).

And the `kx-sync` binary from [Slack](https://koordinates.slack.com/messages/CBQ9M5CEA) or elsewhere.

#### Initialisation

```sh
./kx-sync init kx-my-data.gpkg
```

Starting with a GeoPackage exported from a Koordinate site, it finds the Layer ID and Site from the GeoPackage description data. Using the Koordinates Layers API, it finds the associated version for each GeoPackage layer, and saves the layer and version details to a new `.kx_sync_layers` table. The site, API key, and other misc info is saved to a new `.kx_sync` table.

#### Syncing

```sh
./kx-sync init kx-my-data.gpkg
```

Syncing checks whether any newer versions are available for each layer in the GeoPackage using the Layers API. If there are, it downloads the changes via WFS Changesets, applying them to the existing table.

#### Options

* `-v 2` will turn verbosity up a lot, `-v 0` will make it very quiet.

Limitations
-----------

1. Only support for full layers — no cropping in the initial export.
2. Doesn't support schema changes
3. Vector/table datasets only
4. Not sure how to refresh open datasets in QGIS, so close/reopen your project after a sync currently.
5. No detection of edits to synced layers, and updates may fail when applied to edited/removed features.

Developing
----------

Developed with Python 3.6/3.7.

```sh
brew install libspatialite
git clone git@github.com/koordinates/snowdrop.git
cd snowdrop
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
kx-sync --help
```

#### Standalone Binaries

Using [PyInstaller](https://www.pyinstaller.org/) we can create fully-bundled binaries for multiple OS'. In theory.

##### Buil an OSX binary

* Use Sierra, since building on newer OS versions seems to restrict it to that OS version.
* [OSX on vagrant](https://github.com/AndrewDryga/vagrant-box-osx)

```sh
vagrant up
vagrant ssh

brew update
# PyInstaller has issues with Python3.7; this is 3.6.5
brew install https://raw.githubusercontent.com/Homebrew/homebrew-core/f2a764ef944b1080be64bd88dca9a1d80130c558/Formula/python.rb
brew install libspatialite

cd /vagrant
pip3 install virtualenv
virtualenv ./.venv-osxbuild
source .venv-osxbuild/bin/activate
pip install -r requirements.txt

# build
rm -rf build/ dist/
python setup.py pyinstaller -o '--clean'
# issues? Try `-o '--clean --log-level=DEBUG --debug=all'`
# and switching to the Debug path in snowdrop.spec

# libspatialite + dependencies
brew unlink libspatialite proj python@2 geos libxml2 freexl
# Python3 + dependencies
brew unlink python@3 gdbm openssl readline sqlite xz

# Check
./dist/kx-sync -v 2 -V
```