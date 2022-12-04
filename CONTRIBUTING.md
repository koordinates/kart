# Contributing

We welcome all contributions, bug reports, and suggestions!

* Ask support and usage questions in [Discussions](https://github.com/koordinates/kart/discussions)
* Read and submit bug reports or feature requests at [Issues](https://github.com/koordinates/kart/issues)

We're moving to CMake as a better system for building Kart and its
dependencies. Currently CMake is still a work in progress and doesn't yet build
all the vendor dependencies or create packages suitable for distribution.

## Building the development version with CMake

Requirements:
* CMake >= v3.25
* Git
* Python >= 3.10
* Go >= 1.17
* Ninja
* Rustc

On Ubuntu:
```console
$ apt-get install autoconf build-essential curl git golang libtool patchelf python3-pip python3-venv tar unzip zip
```

Clone Kart from Github:

```console
$ git clone https://github.com/koordinates/kart.git
$ cd kart
```
### Building

Then configure Kart:
```console
$ cmake -B build -S . -DWITH_VCPKG=ON
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

### Downloading vendor dependencies from CI

If you're having issues with VCPKG in the above, you can download a [recent
master-branch vendor CI artifact](https://github.com/koordinates/kart/actions/workflows/build.yml?query=branch%3Amaster+is%3Asuccess) for your platform (eg: `vendor-macos-X64-3.10`). Then:

```console
$ cmake -B build -S . -DVENDOR_ARCHIVE=/path/to/downloaded/vendor-Darwin.zip -DUSE_VCPKG=OFF
$ cmake --build build
$ build/kart --version
```

Note you'll need to have the same version of Python that Kart CI currently uses
(Python 3.10). Get CMake to pick up the right Python using `-DPython3_ROOT=...` -
for example, `-DPython3_ROOT=$(which python3.10 | sed 's@/bin/.*@/@')`

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

Configure and build Kart:

```console
> cmake -B build -S . -DPython3_ROOT=C:\Program Files\Python310 -DUSE_VCPKG=ON
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

If you're having issues with VCPKG in the above, you can download a [recent
master-branch vendor CI artifact](https://github.com/koordinates/kart/actions/workflows/build.yml?query=branch%3Amaster+is%3Asuccess) for your platform (eg: `vendor-windows-X64-3.10`). Then:

```console
> cmake -B build -S . -DPython3_ROOT=C:\Program Files\Python310 -DVENDOR_ARCHIVE=D:\path\to\downloaded\vendor-windows-X64-3.10.zip -DUSE_VCPKG=OFF
> cmake --build build
> .\build\venv\Scripts\kart.exe --version
```

Note you'll need to have the same version of Python that Kart CI currently uses
(Python 3.10).

### Running the tests

```console
$ .\build\venv\Scripts\pytest.exe -v
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
