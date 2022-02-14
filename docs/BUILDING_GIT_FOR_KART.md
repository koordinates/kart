# Building Git for Kart

## Background

Kart comes bundled with a custom build of Git, which has a few changes, depending on the platform:

- On macOS and Linux, the custom build of Git supports list-object-filter extensions, and comes bundled with one such extension - a spatial filter for Kart. See [Spatial Filtering](SPATIAL_FILTERING.md) for more information. The file `vendor/git/Makefile` builds this custom build of Git from source on those platforms.

- On Windows, the custom build of Git supports list-object-filter extensions, but does not come bundled with any such extensions. This means that spatially filtered partial clones are partly supported on Windows - Kart on Windows can be the client during a spatial filtered partial clone, but not the server.

### Building Git for Kart on macOS or Linux

From the root of the Kart repository, run the following command:

`make -C vendor lib-git`

This builds the Kart fork of Git - which supports list-object-filter extensions - and links in the spatial filter extension, which is found at `vendor/spatial-filter`. The executables will be output to the `vendor/env/bin/` directory.
There is some documentation on how to write and build filter extensions in the [koordinates/git fork](https://github.com/koordinates/git/blob/list-objects-filter-extensions/contrib/filter-extensions/README.txt). It is hoped (but is not certain) that the concept of filter extensions will one day be merged into Git (unlike the spatial filter extension itself, which is too specific to be generally useful in Git).

Simply building Kart by running `make` will include this step by either building Git for Kart locally, or downloading a prebuilt version from Github.

### Building Git for Kart on Windows

This process is not yet included in any makefiles or similar and must be performed manually.

The makefile that builds vendor dependencies for Kart on Windows is `vendor/makefile.vc`, and all that it does is download a prebuilt MinGit binary hosted on GitHub and extract it to the appropriate location. In order to build a new version of Git for Kart on Windows, the following steps must be taken on a 64-bit Windows platform:

(These instructions are adapted from the [Git for Windows documentation](https://github.com/git-for-windows/git/wiki/Building-Git#installing-a-build-environment))

#### Installing the build environment
1. Download and run the [Git for Windows SDK installer](https://gitforwindows.org/#download-sdk)

#### Building Git for Kart
1. An initial git clone and make should have already occurred when running the SDK installer.
2. Open the *Git for Windows SDK* *MinGW* shell by double clicking either the Shortcut on the desktop `Git SDK 64-bit.lnk` or by double clicking `mingw64_shell.bat` in the install folder.
3. Change directory to the Git repository: `cd /usr/src/git`
4. Add `koordinates/git` as a remote: `git remote add kx https://github.com/koordinates/git`
5. Fetch the branch with the source for the custom build of Git for Kart: `git fetch kx windows-kx-latest`
6. Check out the branch that was just fetched: `git checkout windows-kx-latest`
7. Run `make install` to build and install the latest version of git.
   > The first line of output will contain a version-number eg `2.34.0.windows.1.13.g93318cbc8d`. Using some or all of this string as the version number generally makes sense (see step 10 below) but you can customise the version number if desired.
8. Check that the installed git (at `/mingw64/bin/git`) is the right version by running `git --version` and make sure it has the functionality you expect.
9. Change directory to the build-extra repository: `cd /usr/src/build-extra`
10. Check out the main branch: `git fetch && git checkout main`
11. Set a version name or number for the release: `VERSION=v2.34.0.windows.1.13`
12. Build a MinGit release of Git: `./mingit/release.sh $VERSION`. The script will create a new zip file in your home directory.

#### Uploading to GitHub and using in a Kart release
1. Create a new release in the `koordinates/git` repository and attach the new zip as an artifact.
2. Modify `vendor/makefile.vc` to download the new zip from the new release.
