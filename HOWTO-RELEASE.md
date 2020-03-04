Notes on Preparing a Sno Release
================================

This process only supports a single release branch (master). It'll need to be expanded over time.

### Prerequisites

1. Make sure you're on `master`.

2. Bump Python dependencies (`make py-requirements-upgrade`); commit; push; check CI passes.

3. Rebuild the vendor dependencies from scratch:
   ```console
   $ make cleanest
   $ make -C vendor build-Darwin
   $ make -C vendor build-Linux
   ```

4. Check tests all pass:
   ```console
   $ make
   $ make test
   ```

5. Package for Linux and run E2E tests:
   ```console
   $ make -C platforms deb rpm
   $ make -C platforms test-deb-all test-rpm-all
   ```

6. Package for macOS and run E2E tests:
   ```console
   $ make -C platforms pkg
   $ sudo installer -pkg platforms/macos/dist/Sno-*.pkg -target /
   # make sure your virtualenv is deactivated
   $ realpath $(which sno)
   /Applications/Sno.app/Contents/MacOS/sno_cli
   $ tests/scripts/e2e-1.sh
   ```

7. Upload vendor dependencies (requires AWS credentials)
   ```console
   $ make -C vendor upload
   ```

8. Decide on your version number:
   * The [SemVer format](https://semver.org/) is `1.2.3` or `1.2.3-beta.3` or `1.2.3-rc.1`
   * The [Python format](https://www.python.org/dev/peps/pep-0440/) equivalent is `1.2.3` or `1.2.3b3` or `1.2.3rc1`
   * The Git tag format is `v{SemVer}`, eg: `v1.2.3` or `v1.2.3-beta.3` or `v1.2.3-rc.1`

9. Create a [new draft release in github](https://github.com/koordinates/sno/releases/new) and write the release notes. Topic/section suggestions:
   * Overview
   * Compatibility / Upgrading
   * New features
   * Bugs fixed
   * External Contributors

### Release

1. Update `sno/VERSION` to the new Python-style version number.

2. Update any version numbers in `README.md` to point to the new version.

3. Commit with a message like "Release v1.2.3"

4. Tag the release with the Github-style version number:
   ```console
   $ git tag v1.2.3
   ```

5. Package for Linux and run E2E tests:
   ```console
   $ make -C platforms clean
   $ make -C platforms deb rpm
   $ make -C platforms test-deb-all test-rpm-all
   ```

6. Package for macOS with signing and notarization and run E2E tests:
   ```console
   $ export CODESIGN="Developer ID Application: ..."
   $ export PKGSIGN="Developer ID Installer: ..."
   $ export NOTARIZE_USER="apple.developer.id@example.com"
   $ export NOTARIZE_PASSWORD="@keychain:AppleDeveloperIdCredentialItem"
   $ make -C platforms pkg-notarize
   ```
   Wait for the notarization to complete, it can take a few minutes. Check the status via `make -C platforms pkg-notarize-check`. Once it's succeeded:
   ```console
   $ make -C platforms pkg-notarize-staple
   $ sudo installer -pkg platforms/macos/dist/Sno-*.pkg -target /
   # make sure your virtualenv is deactivated
   $ realpath $(which sno)
   /Applications/Sno.app/Contents/MacOS/sno_cli
   $ tests/scripts/e2e-1.sh
   ```

7. Attach the binaries to the Github release:
   * `platforms/linux/dist/sno_{version}-1_amd64.deb`
   * `platforms/linux/dist/sno-{version}-1.x86_64.rpm`
   * `platforms/macos/dist/Sno-{version}.pkg`

8. Push the tag:
   ```console
   $ git push origin v1.2.3
   ```

9. Update `sno/VERSION` to the next [development Python version](https://www.python.org/dev/peps/pep-0440/#developmental-releases) eg: `1.2.4.dev0`. If it's an alpha/beta/rc release, then it should be set to the next _release version_ with `.dev0` appended.

10. Commit with a message like "Set development version as v1.2.4.dev0" and push
