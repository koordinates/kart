Working with Point Cloud Datasets
====================================

.. Note:: Before using this quick guide, it will help your understanding to do the :doc:`Basic Usage Tutorial </pages/basic_usage_tutorial>`.

Dataset type
~~~~~~~~~~~~
Point Cloud datasets are currently implemented as :doc:`Point Cloud Datasets V1 </pages/development/pointcloud_v1>`. Kart version controls point cloud tiles themselves as LAZ files, which means the tiles are checked out into the working copy without any conversion step - the contents of the working copy is an accurate reflection of the contents of the dataset itself at a particular point in time.

Importing point cloud datasets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- ``kart import <tile> [<tile>] [<tile>]``
- ``kart import --dataset=<dataset_name> <tile> [<tile>] [<tile>]``

This command imports one or more point-cloud tiles into the kart repository in the
working directory. All tiles are imported into the same dataset.

The tiles to import must be LAS files of some variant - compressed tiles (LAZ) and cloud-optized tiles (COPC) are allowed.
By default, all tiles are converted to COPC (`Cloud Optimized Point Cloud <copc_>`_) as they are imported.
You can specify ``--keep-existing-format`` to keep them as they are.

For more information, see :ref:`Import vectors / tables into an existing repository`.

Working copy
~~~~~~~~~~~~

Point cloud datasets are stored in a file-system based working copy associated with the Kart repository. This working copy is found in folders inside the Kart repository, one folder for each point cloud dataset, with the same name as the dataset. Since the file system working copy is simply in the Kart repository, it doesn't need to be configured, and you don't need any special software to connect to it.

Making edits
~~~~~~~~~~~~

You can use any software you have that can edit a local LAS file to make edits to the working copy, such as `PDAL <pdal_>`_. Overwrite the files in their original location, or add more files to the existing directory, and Kart will pick up the changes - you can see a summary of the newly-added or modified tiles by running ``kart status`` or ``kart diff``. To commit these changes, run ``kart commit``, just as when editing vector or table data.

Git LFS
~~~~~~~
For technical reasons, point cloud tiles are stored within the repository using `Git LFS <git_lfs_>`_, instead of the more fundamental backend store which is the "Git Object Database". For more details, see the section on :doc:`Git LFS </pages/git_lfs>`.
