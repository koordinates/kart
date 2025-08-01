# Contributing

We welcome all contributions, bug reports, and suggestions!

* Ask support and usage questions in [Discussions](https://github.com/koordinates/kart/discussions)
* Read and submit bug reports or feature requests at [Issues](https://github.com/koordinates/kart/issues)

Kart now uses CMake as a better system for building Kart and its dependencies (compared to Makefiles which were used previously).
This was was a major overhaul and there may still be documentation or unused snippets of code that need to be brought up to date - if you find them, let us know.

[Building on macOS and Linux](#building-the-development-version-with-cmake-macos-and-linux)\
[Building on Windows](#building-the-development-version-with-cmake-windows)

## Building the development version with CMake (macOS and Linux)

Requirements:
* CMake >= v3.25
* Git
* Python >= 3.10
* Go >= 1.17
* Ninja
* Rustc

On Ubuntu:
```console
$ apt-get install autoconf build-essential curl flex git golang libtool patchelf python3-pip python3-venv tar unzip zip
```
On macOS (with [Homebrew](https://brew.sh)):
```console
brew install automake autoconf cmake git python go ninja rust pandoc pkg-config libtool
```
Clone Kart from Github:
```console
$ git clone https://github.com/koordinates/kart.git
$ cd kart
$ git submodule update --init --recursive
```

### Setting Python3

Python 3.11 is now used (as of Kart v0.15) to build official Kart releases.
CMake will try to automatically find a Python3 install on your system with which to build Kart.
However, to avoid any issues caused by different Python implementations, it is recommended to force CMake
to use Python 3.11 , by supplying the flag `-DPython3_EXECUTABLE` - for example:
`-DPython3_EXECUTABLE=$(command -v python3.11)`

When using CI artifacts to build this is not just recommended, but required.

### Building

Then configure Kart:
```console
$ cmake -B build -S . -DPython3_EXECUTABLE=/path/to/python3.11 -DUSE_VCPKG=ON
```

Configuration builds all the dependencies using [VCPKG](https://github.com/microsoft/vcpkg)
and can take quite a while. VCPKG caches by default, so future builds will be
much quicker.

Then build the development/editable version:

```console
$ cmake --build build
$ build/kart --version
```

To build & install a bundled binary app. This will also create a
`/usr/local/bin/kart` symlink to the installed application.

```console
$ cmake --build build --target bundle
$ cmake --install build
```

Kart includes a background helper for improved command-line performance, but this
feature is disabled by default in development builds. To enable it, configure
Kart with `-DCLI_HELPER=ON`.

### Downloading vendor dependencies from CI

If you're having issues with VCPKG in the above, you can download a recent master-branch vendor CI artifact for your platform (eg: `vendor-macos-X64-py3.11.zip`).
To do this, take the following steps:
1. Start at the list of recent [successful builds on master](https://github.com/koordinates/kart/actions/workflows/build.yml?query=branch%3Amaster+is%3Asuccess).
1. Select a commit - ideally the commit that you have checked out locally, but if you don't see it, just choosing the top one will generally work.
1. Click through the commit and scroll down to the bottom to see the artifacts.\
(Scrolling down is best achieved with your mouse curson on the left side of the page - see this [GitHub ticket](https://github.com/community/community/discussions/18035))
1. Download the vendor-archive artifact for your platform and CPU architecture.

Then:

```console
$ cmake -B build -S . -DPython3_EXECUTABLE=/path/to/python3.11 -DVENDOR_ARCHIVE=/path/to/downloaded/vendor-{os}-{arch}-py3.11.zip -DUSE_VCPKG=OFF
$ cmake --build build
$ build/kart --version
```

Note you'll need to have the same version of Python that Kart CI currently uses (Python 3.11).

### Running the tests

```console
$ ./build/venv/bin/pytest -v
```

## Building the development version with CMake (Windows)

Requirements:
* Windows 64-bit 8.1 / Windows Server 64-bit 2016; or newer
* MS Visual Studio 2017 or newer, with C++ tools installed
* Python >= 3.10
* CMake >= v3.25
* Git for Windows

Clone Kart from Github:

```console
> git clone https://github.com/koordinates/kart.git
> cd kart
```

### Setting Python3

Python 3.11 is now used (as of Kart v0.15) to build official Kart releases.
CMake will try to automatically find a Python3 install on your system with which to build Kart.
However, to avoid any issues caused by different Python implementations, it is recommended to force CMake
to use Python 3.11, by supplying the flag `-DPython3_EXECUTABLE` - for example:
`-DPython3_EXECUTABLE="C:\Program Files\Python311\python.exe"`

When using CI artifacts to build this is not just recommended, but required.

### Building

Configure and build Kart:

```console
> cmake -B build -S . -DPython3_EXECUTABLE=C:\path\to\python310.exe -DUSE_VCPKG=ON
> cmake --build build
> .\build\venv\Scripts\kart.exe --version
```

The commands above should be run from within a Visual Studio 64-bit command
prompt; an SDK prompt will not work. CMake is generating a Visual Studio
solution file and several project files, and by default it chooses the latest
version of Visual Studio that’s installed on your machine. Use CMake-GUI or the
`-G` flag to CMake to select an alternative compiler.

To build a bundled binary app.

```console
$ cmake --build build --target bundle
$ build\pyinstaller\dist\kart\kart.exe --version
```

### Downloading vendor dependencies from CI

If you're having issues with VCPKG in the above, you can download a recent master-branch vendor CI artifact for your platform (eg: `vendor-windows-X64-py3.11.zip`).
To do this, take the following steps:
1. Start at the list of recent [successful builds on master](https://github.com/koordinates/kart/actions/workflows/build.yml?query=branch%3Amaster+is%3Asuccess).
1. Select a commit - ideally the commit that you have checked out locally, but if you don't see it, just choosing the top one will generally work.
1. Click through the commit and scroll down to the bottom to see the artifacts.\
(Scrolling down is best achieved with your mouse curson on the left side of the page - see this [GitHub discussion](https://github.com/community/community/discussions/18035))
1. Download the vendor-archive artifact for your platform.

Then:

```console
> cmake -B build -S . -DPython3_EXECUTABLE=C:\path\to\python310.exe -DVENDOR_ARCHIVE=D:\path\to\downloaded\vendor-windows-X64-py3.11.zip -DUSE_VCPKG=OFF
> cmake --build build
> .\build\venv\Scripts\kart.exe --version
```

Note you'll need to have the same version of Python that Kart CI currently uses (Python 3.11).

### Running the tests

```console
$ .\build\venv\Scripts\pytest.exe -v
```

## CI

Continuous integration builds apps, tests, and installers for every commit on supported platforms. Artifacts are published to Github Actions, including vendor library bundles, test results, and unsigned installers.

## Code formatting

We use [Ruff](https://docs.astral.sh/ruff/) to ensure consistent code formatting. We recommend integrating Ruff with your editor - [see instructions here](https://docs.astral.sh/ruff/editors/setup/)

We use the default settings, and target python 3.10+.

One easy solution is to install [pre-commit](https://pre-commit.com), run `pre-commit install --install-hooks` and it'll automatically validate your changes as a git pre-commit hook.
