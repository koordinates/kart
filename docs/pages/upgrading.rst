Upgrading
=========

.. warning:: A reminder that compatibility for Kart 0.x releases (software or repositories) isn't guaranteed, Kart is evolving quickly and things will change. However, we aim to provide the means to upgrade existing repositories between 0.x versions and to 1.0.*

v0.9 (first "Kart" release)
---------------------------

As of release 0.9, Sno is renamed to Kart.

Newly created repositories will have local config and structure named
after ``kart`` instead of ``sno`` - for instance, a Kart repo's objects
are now hidden inside a ``.kart`` folder. Existing Sno repos using the
older names will continue to be supported going forward. To modify a
repo in place to use the ``kart`` based names instead of the ``sno``
ones, use ``kart upgrade-to-kart PATH``.

Your data, changes, and branch/tag/commit histories will be preserved.
Even commit hashes remain unchanged - only the local configuration is
changed.

v0.8 (last "Sno" release)
-------------------------

As of Sno 0.8, :ref:`Table Datasets V1`
is no longer supported. Any repositories you have that are still using
this format can still be upgraded as described in the v0.5 release,
using ``sno upgrade /path/to/old-repo /path/to/new-repo``.

v0.5
----

Repositories created with Sno 0.2 or higher can be used with 0.5,
however *some* new functionality requires that you upgrade your
repositories to the new format (:ref:`Table Datasets V2`).
A future release of Sno will drop support for v1 repositories.

To upgrade your repositories to the new format, run
``sno upgrade /path/to/old-repo /path/to/new-repo``.

Your data, changes, and branch/tag/commit histories will be preserved,
along with commit dates/times/authors. **Commit hashes will change.**

v0.4
----

Repositories created with Sno 0.2 or higher are compatible with v0.4.
Some command argument syntax has changed (especially ``init`` and
``import`` commands.) See the `Changelog <changelog_>`_
for more information.

v0.3
----

Repositories created with v0.2 Sno are compatible with v0.3. Some
command argument syntax has changed. See the documentation for more
information.

macOS
^^^^^

1. Uninstall the old version:
   ``brew uninstall sno; brew untap koordinates/sno``
2. Install Sno v0.3 — see the `Readme <readme_>`_.

v0.2
----

Repositories created with older Snowdrop preview versions are not
compatible with v0.2. Some command names and argument syntax have
changed. See the documentation for more information.

1. Commit any outstanding changes to the old repositories
2. Uninstall the old Snowdrop software:
   ``brew uninstall --force snowdrop; brew untap koordinates/snowdrop``
3. Install Sno v0.2 — see the `Readme <readme_>`_.
4. Make an empty directory for each upgraded repository
   (``mkdir /path/to/new``)
5. Run ``sno upgrade 00-02 /path/to/old-repo.sno /path/to/new {layer}``
   where ``{layer}`` is the name of the layer in your old repository.
6. Your data, changes, and branch/tag/commit histories will be
   preserved, along with commit dates/times/authors. **Commit hashes
   will change.**
