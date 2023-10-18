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
   Initialized empty Git repository in buildings-project/.kart/
   Starting git-fast-import...
   Importing 75,409 features from wellington-building-outlines.gpkg:wellington_building_outlines to wellington_building_outlines/ ...
   Added 75,409 Features to index in 9.4s
   Overall rate: 8028 features/s)
   Closed in 0s
   Creating GPKG working copy at buildings-project.gpkg ...
   Writing features for dataset 1 of 1: wellington_building_outlines
   wellington_building_outlines: 100%|████████████████████████████████████████████████| 75409/75409 [00:09<00:00, 7783.25F/s]

Once the import is complete, you will have an initialised repository
with your data imported. Check its current status:

.. code:: console

   $ kart status
   On branch main

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
   On branch main

   Changes in working copy:
     (use "kart commit" to commit)
     (use "kart restore" to discard changes)

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
   [main 094b328] Updated stadium usage attribute for the Kart usage tutorial.
     wellington_building_outlines/
       modified:  1 feature
     Date: Fri Jun 19 12:11:40 2020 +1200

Your change has now been saved in the history of your repository.
Running ``status`` will show the new 'clean' state of your working copy:

.. code:: console

   $ kart status
   On branch main

   Nothing to commit, working copy clean

Resetting Changes
-----------------

Kart provides a simple method to undo the changes you've made since your
last commit, called ``restore``.

Switch back to your QGIS window. Toggle editing back on, select a large
number of features and delete them. Toggle editing off in QGIS, saving
the QGIS layer edits when prompted.

.. image:: /_static/basic-tutorial-3.png

Now run ``kart status`` to see the effect of your edit on the working
copy data:

.. code:: console

   $ kart status
   On branch main

   Changes in working copy:
     (use "kart commit" to commit)
     (use "kart restore" to discard changes)

     wellington_building_outlines/
       deleted:   199 features

Rather than save this edit, roll the data back to the previous commit
with ``kart restore``:

.. code:: console

   $ kart restore
   Updating buildings-project.gpkg ...

In QGIS, press ``f5`` or click the 'refresh' button. The layer will be
updated to it's previous state before the features were deleted.

.. image:: /_static/basic-tutorial-4.png

Hosting Repositories Remotely
-----------------------------

Because Kart is a direct extension of Git, hosting a remote repository
to enable synchronization between multiple local repository copies is as
simple as just pushing to any host compatible with the git CLI. This 
includes github.
