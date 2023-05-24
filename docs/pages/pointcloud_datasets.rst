Working with Point Cloud Datasets
====================================

.. Note:: Before using this quick guide, it will help your understanding to do the :doc:`Basic Usage Tutorial </pages/basic_usage_tutorial>`.

Dataset type
~~~~~~~~~~~~
Point Cloud datasets are currently implemented as :doc:`Point Cloud Datasets V1 </pages/development/pointcloud_v1>`. Kart version controls point cloud tiles themselves as LAZ files, which means the tiles are checked out into the working copy without any conversion step - the contents of the working copy is an accurate reflection of the contents of the dataset itself at a particular point in time.

Importing point cloud datasets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- ``kart import <tile> [<tile>] [<tile>] [--convert-to-copc/--preserve-format]``
- ``kart import --dataset=<dataset_name> <tile> [<tile>] [<tile>] [--convert-to-copc/--preserve-format]``

This command imports one or more point-cloud tiles into the Kart repository in the
working directory. All tiles are imported into the same dataset.

The tiles to import must be LAS files of some variant - compressed tiles (LAZ) and cloud-optized tiles (COPC) are allowed.

If ``--convert-to-copc`` is specified, all tiles that are not cloud-optimized will be converted to cloud-optimized as they are imported, and the dataset will
also have a constraint attached which means that only cloud-optimized tiles can be added. This constraint can be dropped later if needed.

If ``--preserve-format`` is specified, all tiles will be imported as-is, without any conversion step. This is allowed for any kind of compressed tile (LAZ) but not for uncompressed tiles (LAS), which would make the repository unnecessarily large. A further requirement is that the entire dataset must have the same LAZ version (for example, every tile in the dataset uses LAZ 1.4).

If neither option is specified, Kart will prompt to see which you prefer.

Importing tiles as cloud-optimized is recommended, since web-viewers of point-clouds only work on tiles that have been cloud-optimized. If you ever decide to publish your repository or some part of it on the internet, it will be easier at that time if the entire revision history of that repository is all in a single, web-viewable format.

On the other hand, converting non-COPC tiles to the COPC format will make the import take longer and use more disk space.

For more information, see :ref:`Import vectors / tables into an existing repository`.

Working copy
~~~~~~~~~~~~

Point cloud datasets are stored in a file-system based working copy associated with the Kart repository. This working copy is found in folders inside the Kart repository, one folder for each point cloud dataset, with the same name as the dataset. Since the file system working copy is simply in the Kart repository, it doesn't need to be configured, and you don't need any special software to connect to it.

Making edits
~~~~~~~~~~~~

You can use any software you have that can edit a local LAS file to make edits to the working copy, such as `PDAL <pdal_>`_. Overwrite the files in their original location, or add more files to the existing directory, and Kart will pick up the changes - you can see a summary of the newly-added or modified tiles by running ``kart status`` or ``kart diff``. To commit these changes, run ``kart commit``, just as when editing vector or table data.

If you have added more tiles to the directory that are not compatible with the existing Kart dataset, you will be asked to supply one of the following options.

If ``--convert-to-dataset-format`` is specified, the newly added tiles will be converted so that they conform to the dataset's format - for instance, by converting them to the COPC format, or by converting them to a different LAZ version.

If ``--no-convert-to-dataset-format`` is specified, the dataset's format will be changed or relaxed so that the new tiles can be added alongside any remaining old tiles, without changing either the new tiles or the old tiles. The commit might still be disallowed if the resulting set of tiles would not be allowed as a dataset (e.g. if multiple LAZ versions are present.)

Note that these options only apply to the format of the tiles (how they are stored, as opposed to their content.) There are no options that automatically change the content of tiles while they are being committed, so keeping the contents coherent is the responsibility of the user.

For instance, every tile in a dataset must have the same CRS. If newly added tiles have a different CRS to the rest of the dataset, they cannot be committed until the user has fixed the CRS issue themselves.

Git LFS
~~~~~~~
For technical reasons, point cloud tiles are stored within the repository using `Git LFS <git_lfs_>`_, instead of the more fundamental backend store which is the "Git Object Database". For more details, see the section on :doc:`Git LFS </pages/git_lfs>`.
