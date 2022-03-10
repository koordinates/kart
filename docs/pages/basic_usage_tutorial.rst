Basic Usage Tutorial
====================

Tutorial Requirements
---------------------

-  QGIS - A Free and Open Source Geographic Information System - `Download <qgis_download_>`_
-  Wellington Building Outlines Example Data - :download:`Download
   ZIP </_static/wellington-building-outlines.gpkg.zip>`
-  Kart - :ref:`Quick Guide`

In this tutorial we are working with a data set of Wellington City
building outlines, published by Land Information New Zealand. You should
be familiar with basic terminal commands and QGIS editing functions.

Create or choose an empty folder for this tutorial and copy the example
GeoPackage into your tutorial folder. For example:

.. code:: console

   $ ls
   wellington-building-outlines.gpkg

Creating a new Repository & importing data
------------------------------------------

Kart makes it possible to store spatial and tabular data in a version
controlled data repository.

Decide on a path to your new repository. To create the repository and
import the table, use the ``init`` command. Since there is only one
table in the GeoPackage, ``wellington_building_outlines``, you don't
need to specify which table to import.

.. code:: console

   $ mkdir buildings-project
   $ cd buildings-project
   $ kart init --import=GPKG:../wellington-building-outlines.gpkg
   Starting git-fast-import...
   Importing 75,408 features from GPKG:../wellington-building-outlines.gpkg:wellington_building_outlines to wellington_building_outlines/ ...
   Added 75,408 Features to index in 2.6s
   Overall rate: 29198 features/s)
   /Users/me/code/kart/venv/libexec/git-core/git-fast-import statistics:
   ---------------------------------------------------------------------
   Alloc'd objects:     125000
   Total objects:       120522 (         0 duplicates                  )
         blobs  :        75431 (         0 duplicates      73930 deltas of      73936 attempts)
         trees  :        45090 (         0 duplicates          0 deltas of          0 attempts)
         commits:            1 (         0 duplicates          0 deltas of          0 attempts)
         tags   :            0 (         0 duplicates          0 deltas of          0 attempts)
   Total branches:           1 (         1 loads     )
         marks:           1024 (         0 unique    )
         atoms:          75691
   Memory total:         26402 KiB
          pools:         18590 KiB
        objects:          7812 KiB
   ---------------------------------------------------------------------
   pack_report: getpagesize()            =       4096
   pack_report: core.packedGitWindowSize = 1073741824
   pack_report: core.packedGitLimit      = 35184372088832
   pack_report: pack_used_ctr            =          2
   pack_report: pack_mmap_calls          =          1
   pack_report: pack_open_windows        =          1 /          1
   pack_report: pack_mapped              =   16667668 /   16667668
   ---------------------------------------------------------------------

   Closed in 1s
   Checkout to /Users/me/kart-tutorial/buildings-project/buildings-project.gpkg as GPKG ...
   Commit: dd4d5159a020d1c7a661d6fe7a8e099a92cba7e1

Once the import is complete, you will have an initialised repository
with your data imported. Check its current status:

.. code:: console

   $ kart status
   On branch master

   Nothing to commit, working copy clean

Working Copy
------------

A working copy is a version of your data repository in a form you view
and edit. Your working copy is a new GeoPackage with additional tracking
information.

Normally your working copy is named the same as your project folder and
is located inside your project folder.

In our example, in the ``buildings-project/`` folder the working copy is
named ``building-projects.gpkg``.

Remember that the *repository* is the folder path
``buildings-project/``. This is the location where you should run your
``kart`` commands to manage your repository, checkouts, branches, etc.
The working copy is your tracked data - i.e. the data you can view &
modify.

Open QGIS and find the tutorial folder in the file browser. Open
``buildings-project.gpkg`` and add the ``wellington-building-outlines``
layer to your map. QGIS will display the layer content.

.. image:: /_static/basic-tutorial-1.png

Making and Committing Changes
-----------------------------

A "commit" is another word for saving a change in your data to the
repository. One commit can contain any number of changes -
modifications, additions and deletions.

In QGIS, select the ``wellington_building_outlines`` layer and toggle
editing on. Select a feature and open the attributes table and change a
value. In this example, we select a stadium and modify the ``use`` from
``Unknown`` to ``Stadium``.

.. image:: /_static/basic-tutorial-2.png

Once you have edited the feature, save the layer edits and toggle layer
editing off.

From your terminal, use ``kart status`` to see the effect of the edit.

.. code:: console

   $ kart status
   On branch master

   Changes in working copy:
     (use "kart commit" to commit)
     (use "kart reset" to discard changes)

     wellington_building_outlines/
       modified:  1 feature

kart reports a single modified feature. We can see the detail of the
change with ``kart diff``.

.. code:: console

   $ kart diff
   --- wellington_building_outlines:fid=4381
   +++ wellington_building_outlines:fid=4381
   -                                      use = Unknown
   +                                      use = Stadium

Once you've checked that the change looks correct, you can commit the
change to the repository with ``commit``. You need to provide a commit
message - a human readable description of the change.

.. code:: console

   $ kart commit -m "Updated stadium usage attribute for the Kart usage tutorial."
   [master 094b328] Updated stadium usage attribute for the Kart usage tutorial.
     wellington_building_outlines/
       modified:  1 feature
     Date: Fri Jun 19 12:11:40 2020 +1200

Your change has now been saved in the history of your repository.
Running ``status`` will show the new 'clean' state of your working copy:

.. code:: console

   $ kart status
   On branch master

   Nothing to commit, working copy clean

Resetting Changes
-----------------

Kart provides a simple method to undo the changes you've made since your
last commit, called ``reset``.

Switch back to your QGIS window. Toggle editing back on, select a large
number of features and delete them. Toggle editing off in QGIS, saving
the QGIS layer edits when prompted.

.. image:: /_static/basic-tutorial-3.png

Now run ``kart status`` to see the effect of your edit on the working
copy data:

.. code:: console

   $ kart status
   On branch master

   Changes in working copy:
     (use "kart commit" to commit)
     (use "kart reset" to discard changes)

     wellington_building_outlines/
       deleted:   199 features

Rather than save this edit, roll the data back to the previous commit
with ``kart reset``:

.. code:: console

   $ kart reset
   Updating buildings-project.gpkg ...

In QGIS, press ``f5`` or click the 'refresh' button. The layer will be
updated to it's previous state before the features were deleted.

.. image:: /_static/basic-tutorial-4.png
