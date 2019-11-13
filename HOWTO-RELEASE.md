Notes on Preparing a Sno Release
================================

This process only supports a single release branch (master), and works with the private repo & inline Homebrew tap. It'll need to be expanded over time.

### Prerequisites

1. Bump Python dependencies (`make requirements-upgrade`); commit; push; check CI passes.

2. Decide on your version number:
   * The [SemVer format](https://semver.org/) is `1.2.3` or `1.2.3-beta.3` or `1.2.3-rc.1`
   * The [Python format](https://www.python.org/dev/peps/pep-0440/) equivalent is `1.2.3` or `1.2.3b3` or `1.2.3rc1`
   * The Git tag format is `v{SemVer}`, eg: `v1.2.3` or `v1.2.3-beta.3` or `v1.2.3-rc.1`

3. Create a [new draft release in github](https://github.com/koordinates/sno/releases/new) and write the release notes. Topic/section suggestions:
   * Overview
   * Compatibility
   * Upgrading
   * New features
   * Bugs fixed
   * Contributors

### Release

1. Update `setup.py`:
   * set `version=` to the new Python version

2. Update any version numbers in `README.md` to point to the new Python version.

3. Commit with a message like "Release v1.2.3"

4. Tag the release
   ```console
   $ git tag v1.2.3
   $ git push origin v1.2.3
   ```

5. Update `HomebrewFormula/sno.rb` to publish the new release. Note that only master is relevant here, and we can only point to one stable release.
   * set `.stable.url.tag` with the new Git Tag version
   * set `.stable.url.revision` with the sha1 hash from the Git tag (`git rev-parse v1.2.3`)
   * set `.stable.version` to the new SemVer version

6. Update `README.md` to link to the latest release notes & stable version.

6. Commit with a message like "Update Homebrew stable release to v1.2.3"

7. Update `setup.py` to set `version=` to the next [development Python version](https://www.python.org/dev/peps/pep-0440/#developmental-releases) eg: `1.2.4.dev0`. It should always hang off the next patch version, not the next rc/beta.

8. Commit with a message like "Set development version as v1.2.4.dev0"

9. Push those commits.
