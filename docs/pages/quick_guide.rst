Quick Guide
===========

Installing
----------

**Windows**

Download the .msi installer from the `release page <releases_>`_.

**macOS**

Download the .pkg installer from the `release page <releases_>`_.

Or use Homebrew to install:
``brew install koordinates/kart/kart --cask``

.. note::

   Kart is no longer built for Intel-based Macs. The last version supporting Intel Macs is `v0.16.1 <https://github.com/koordinates/kart/releases/v0.16.1>`_.

**Linux**

For Debian/Ubuntu-based distributions, download the .deb package from
the `release page <releases_>`_ and
install via ``dpkg -i kart_*.deb``

For RPM-based distributions, download the .rpm package from the `release page <releases_>`_ and install via
``rpm -i kart-*.rpm``

Quick Start
-----------

See the :doc:`documentation </index>` for
tutorials and reference.

   .. rubric:: 💡 If you're new to git
      :name: if-youre-new-to-git

   Configure the identity you will use for Kart commits with:

   .. code:: console

      $ kart config --global user.email "you@example.com"
      $ kart config --global user.name "Your Name"


1. Export a GeoPackage from `Koordinates <koordinates_website_>`_
   with any combination of vector layers and tables.

2. Create a new Kart repository and import the GeoPackage (eg.
   ``kx-foo-layer.gpkg``).

   .. code:: console

      $ kart init myproject --import GPKG:kx-foo-layer.gpkg
      $ cd myproject

   Use this repository as the directory to run all the other commands
   in. This will also create a working copy as
   ``myproject/myproject.gpkg`` to edit.

3. Editing the working copy in QGIS/etc:

   -  will track changes in the internal ``kart`` tables
   -  additions/edits/deletes of features are supported
   -  changing feature PKs is supported
   -  schema changes should be detected, but aren't supported yet (will
      error).
   -  Use F5 to refresh your QGIS map after changing the underlying
      working-copy data using ``kart``.

4. With your working copy, ``kart`` commands should work if run from the
   ``myproject/`` folder. Check ``--help`` for options, the most
   important ones are supported. In some cases options are passed
   straight through to an underlying git command:

   -  ``kart diff`` diff the working copy against the repository (no
      index!)
   -  ``kart commit -m {message}`` commit outstanding changes from the
      working copy
   -  ``kart log`` review commit history
   -  ``kart branch`` & ``kart checkout -b`` branch management
   -  ``kart fetch`` fetch upstream changes.
   -  ``kart status`` show working copy state.
   -  ``kart merge`` merge. Supports ``--ff``/``--no-ff``/``--ff-only``
      from one merge source.
   -  ``kart switch`` switch to existing or new branches.
   -  ``kart reset`` & ``kart restore`` discard changes in the working
      copy.
   -  ``kart tag ...``
   -  ``kart remote ...``. Remember simple remotes can just be another
      local directory.
   -  ``kart push`` / ``kart pull``
   -  ``kart clone`` initialise a new repository from a remote URL,

Create a new repository & import dataset
----------------------------------------

Start with a GeoPackage dataset (``my.gpkg``), and create an empty
folder for your new kart project:

.. code:: console

   $ mkdir myproject
   $ cd myproject
   $ kart init --import /path/to/my.gpkg

Use this repository as the directory to run all the other commands in.
This will also create a working copy as ``myproject/myproject.gpkg`` to
use for editing.

Workflow
--------

Your repository consists of two "trees" maintained by kart. The first
one is your *working copy* which is a GeoPackage holding the features
you access & edit with your data tools. The second one is the
*HEAD* which points to the last commit you've made.

Using the Working Copy
----------------------

Editing with QGIS/ArcGIS/GDAL/etc:

-  Will track all changes made to layers and tables in the repository.
-  Additions/edits/deletes of features are supported
-  Editing of a feature's primary key value is supported
-  Creating new fields or deleting existing fields is supported
-  In QGIS, use F5 to refresh your map after changing the underlying
   working-copy data using ``kart``.

Commit
------

To commit changes you've made to your working copy, use:

``kart commit -m "Commit message"``

Now the file is committed to the *HEAD*.

Before committing changes, you can also preview them by using
``kart diff``

Branching
---------

Branches are used to make changes isolated from each other.
The master branch is the "default" branch when you create a repository.
Use other branches for editing and merge them back to the master branch
upon completion.

Create a new branch named "edit_x" and switch to it using:
``kart checkout -b edit_x``

Switch back to master: ``kart checkout master``

And delete the branch again: ``kart branch -d edit_x``

Merge
-----

To merge another branch into your active branch (e.g. master), use
``kart merge <branch>``

Kart tries to auto-merge changes.

Tagging
-------

it's recommended to create tags for data releases. You can create a new
tag named ``2019.11`` by executing

``kart tag 2019.11 1b2e1d63``

the ``1b2e1d63ff`` stands for the first few characters of the commit id
you want to reference with your tag. You can get the commit id by
looking at the…

Log
---

in its simplest form, you can study repository history using
``kart log``

``kart show`` will show the latest HEAD commit

``kart status`` shows a summary of your branch state and working copy
changes.

Switch back to an old revision
------------------------------

Find the commit id (or tag or branch) you want to reference from
``kart log`` , then switch your working copy to it using
``kart checkout 1b2e1d63``

Replace working copy changes
----------------------------

You can replace edits to your working copy using the command
``kart reset HEAD``. This replaces the changes in your working copy with
the last content in *HEAD*.
