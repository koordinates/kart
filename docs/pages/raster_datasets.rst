Working with Raster Datasets
====================================

.. Note:: Before using this quick guide, it will help your understanding to do the :doc:`Basic Usage Tutorial </pages/basic_usage_tutorial>`.

Dataset type
~~~~~~~~~~~~
Kart internally stores raster tiles as GeoTIFF files, which means the tiles are checked out into the working copy without any conversion step - the contents of the working copy is an accurate reflection of the contents of the dataset itself at a particular point in time.

To find out more about the internal storage of raster datasets, see :doc:`Raster Datasets V1 </pages/development/raster_v1>`.

Importing raster datasets
~~~~~~~~~~~~~~~~~~~~~~~~~

- ``kart import <tile> [<tile>] [<tile>] [--convert-to-cog/--preserve-format]``
- ``kart import --dataset=<dataset_name> <tile> [<tile>] [<tile>] [--convert-to-cog/--preserve-format]``

This command imports one or more raster tiles into the Kart repository. All tiles are imported into the same dataset.

The tiles to import must be GeoTIFF files of some variant - and cloud-optized GeoTIFFs (COGs) are allowed.

If ``--convert-to-cog`` is specified, all tiles that are not cloud-optimized will be converted to cloud-optimized as they are imported, and the dataset will
also have a constraint attached which means that only cloud-optimized tiles can be added. This constraint can be dropped later if needed.

If ``--preserve-format`` is specified, all tiles will be imported as-is, without any conversion step.

If neither option is specified, Kart will prompt to see which you prefer.

Importing tiles as cloud-optimized is recommended, since web-viewers of GeoTIFFs only work on tiles that have been cloud-optimized. If you ever decide to publish your repository or some part of it on the internet, it will be easier at that time if the entire revision history of that repository is all in a single, web-viewable format.

On the other hand, converting non-COG tiles to the COG format will make the import take longer and use more disk space.

For more information, see :ref:`Import vectors / tables into an existing repository`.

Working copy
~~~~~~~~~~~~

Raster datasets are stored in a filesystem-based working copy associated with the Kart repository. This working copy is found in folders inside the repository, one folder for each raster dataset, with the same name as the dataset.

Making edits
~~~~~~~~~~~~

You can use any software you have that can edit a local GeoTIFF file to make edits to the working copy, such as `GDAL <gdal_>`_. Overwrite the files in their original location, or add more files to the existing directory, and Kart will pick up the changes - you can see a summary of the newly-added or modified tiles by running ``kart status`` or ``kart diff``. To commit these changes, run ``kart commit``, just as when editing vector or table data.

If you have added more tiles to the directory that are not compatible with the existing Kart dataset, you will be asked to supply one of the following options.

If ``--convert-to-dataset-format`` is specified, the newly added tiles will be converted so that they conform to the dataset's format. (In practice this currently only means converting the newly added tiles to the COG format, no other conversions will be performed.)

If ``--no-convert-to-dataset-format`` is specified, the dataset's format will be changed or relaxed so that the new tiles can be added alongside any remaining old tiles, without changing either the new tiles or the old tiles. (In practice this currently only means removing the consraint that a dataset contain only COG tiles, there are no other constraints that can be changed or relaxed.)

Note that these options only apply to the format of the tiles (how they are stored, as opposed to their content.) There are no options that automatically change the content of tiles while they are being committed, so keeping the contents coherent is the responsibility of the user.

For instance, every tile in a dataset must have the same CRS. If newly added tiles have a different CRS to the rest of the dataset, they cannot be committed until the user has fixed the CRS issue themselves.

Git LFS
~~~~~~~
For technical reasons, raster tiles are stored within the repository using `Git LFS <git_lfs_>`_, instead of the more fundamental backend store which is the "Git Object Database". For more details, see the section on :doc:`Git LFS </pages/git_lfs>`.

VRT files
~~~~~~~~~
Setting an environment variable ``KART_RASTER_VRTS=1`` when creating the Kart working copy or checking out a commit will cause Kart to create a `VRT <vrt_>`_ (Virtual Raster) file for each raster dataset. This single file comprises a mosaic of all the tiles in the working copy that belong to that dataset, so that if you load this file in your tile-editing software, you have effectively loaded the entire dataset. The individual tiles are referenced by the VRT, rather than the data from each tile being duplicated in the VRT. Creation of VRTs is still experimental but should become the default in a future version of Kart.
