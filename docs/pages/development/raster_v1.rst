Raster Datasets V1
-----------------------

Overall structure
~~~~~~~~~~~~~~~~~

A V1 raster dataset is a folder named ``.raster-dataset.v1`` that contains two folders. These are ``meta`` which contains information about the entire dataset, irrespective of a particular tile - and ``tile``, which contains raster tiles in the ``GeoTIFF`` format. The schema file contains information about each band in the raster. The "name" of the dataset is the path to the ``.raster-dataset.v1`` folder.

For example, here is the basic folder structure of a dataset named
``aerials/north-head``:

::

   aerials/
   aerials/north-head/
   aerials/north-head/.raster-dataset.v1/
   aerials/north-head/.raster-dataset.v1/meta/
   aerials/north-head/.raster-dataset.v1/meta/title              # Title of the dataset
   aerials/north-head/.raster-dataset.v1/meta/description        # Description of the dataset
   aerials/north-head/.raster-dataset.v1/meta/format.json        # Format of the dataset
   aerials/north-head/.raster-dataset.v1/meta/schema.json        # Schema of the dataset
   aerials/north-head/.raster-dataset.v1/meta/crs.wkt            # CRS of the dataset
   aerials/north-head/.raster-dataset.v1/meta/band-1/...         # Extra metadata specific to the first band...
   aerials/north-head/.raster-dataset.v1/meta/band-2/...         # Extra metadata specific to the second band...
   aerials/north-head/.raster-dataset.v1/meta/...                # Other dataset metadata

   aerials/north-head/.raster-dataset.v1/tile/...                # All of the tiles in the dataset - all GeoTIFFs

Meta items
~~~~~~~~~~

The following items are stored in the meta part of the dataset, and have the following structure.

``meta/title``
^^^^^^^^^^^^^^

Contains the title of the dataset, encoded using UTF-8. The title is freeform text.

``meta/description``
^^^^^^^^^^^^^^^^^^^^

A long-form description of the dataset, encoded using UTF-8. The description is freeform text.

``meta/format.json``
^^^^^^^^^^^^^^^^^^^^

Contains information about constraints related to file format that the GeoTIFF tiles must follow. A single dataset cannot contain a mixture of all kinds of GeoTIFF files - there must be some commonality between them. For example, every tile in a dataset must have the same CRS.

In terms of format, currently there are only one or two constraints stored in the format.json that every tile in the dataset must follow:
1. All tiles must be GeoTIFF (this constraint is always present for Raster Datasets V1 - no other file types are supported).
2. All tiles must be `Cloud-Optimized GeoTIFFs <cog_>`_. This constraint is only set for certain datasets where the user has requested it.

Constraining the tiles to be Cloud Optimized GeoTIFFs (COGs) means they can be viewed by webviewers - the COG format is a subtype of GeoTIFF file which allows for fasting seeking and rendering of whichever part of the raster is currently within the user's viewport.

For example, here is the format of a dataset that has both constraints:

.. code:: json

    {
      "fileType": "geotiff",
      "profile": "cloud-optimized"
    }

If the cloud-optimized constraint is relaxed, such that both COGs and non-COGs are allowed, then the ``profile`` field is omitted.

.. _raster-meta-schema-json:

``meta/schema.json``
^^^^^^^^^^^^^^^^^^^^

Contains information about the "bands" of information, that is, each unit of data that is stored per-pixel. This item has a similar format as in other types of datasets, but one important difference is that the bands in a raster dataset have no identifying ID or name. The properties that identify a raster band are the following:

1. Its position in the list.
2. Its "interpretation" (if it has one), which describes how to interpret a band in terms of letting it contribute to the color of the pixel / grid cell.

Two example schemas follow - a more in-depth explanation of the possible fields is found below under the heading “Syntax”.

Example schema for a dataset where each tile has 4 bands - the channels red, green, blue and alpha.

.. code:: json

    [
      {
        "dataType": "integer",
        "size": 8,
        "interpretation": "red",
        "unsigned": true
      },
      {
        "dataType": "integer",
        "size": 8,
        "interpretation": "green",
        "unsigned": true
      },
      {
        "dataType": "integer",
        "size": 8,
        "interpretation": "blue",
        "unsigned": true
      },
      {
        "dataType": "integer",
        "size": 8,
        "interpretation": "alpha",
        "unsigned": true
      }
    ]

Example schema for a dataset where each tile has a single band, and the value of that band at each pixel is used to decide the color of that pixel by consulting a "palette" or lookup-table.

.. code:: json

    [
      {
        "dataType": "integer",
        "size": 8,
        "description": "Land use type",
        "interpretation": "palette",
        "unsigned": true
      }
    ]

Syntax
''''''

Every JSON object in the array represents a band in each tile. Note that all tiles in a dataset are required to have the same schema. These objects are listed in the same order as the bands are ordered in each tile. Each of these object has at least the two required attributes - ``dataType`` and ``size`` - and some have many
more optional attributes.


Required attributes
'''''''''''''''''''

``dataType``

There are only two possible values for ``dataType``, which are as follows:

-  ``integer``

   -  stores an integer value, using a fixed number of bits.

-  ``float``

   -  stores a floating point number using a fixed number of bits.
      Floating point values have reasonable but imperfect precision over
      a huge range.


``size``

The size property refers to the number of bits that a band uses, per-pixel, when uncompressed. For example, a raster that stores pixels with 24-bit "full color"
with one byte for red, one byte for green, one byte for blue, would have three integer bands, each of size 8.

For complex types (see below), the band contains two identical data types: the size is specified for one of them, so the total size is twice what is specified. Eg a “complex float size=32” would actually take 64 bits to store both floats.

Optional attributes
'''''''''''''''''''

``description``

Optional freeform text describing the meaning of this band

``interpretation``

If present, this must be one of the following strings which describe how to interpret this band, in terms of letting it contribute to the color of the pixel / grid cell.

- ``palette``
- ``red``
- ``green``
- ``blue``
- ``alpha``
- ``hue``
- ``saturation``
- ``lightness``
- ``cyan``
- ``magenta``
- ``yellow``
- ``black``

Commonly, a few bands will work together to provide the final color of the pixel - for instance, a schema with a "red", "green" and "blue" band can store any RGB color by varying the brightness of these three channels. Note that it is not necessary that every band have an interpretation - datasets where no bands have an interpretation are also allowed.

If the interpretation is "palette", then this means a lookup-table is consulted to find the eventual color of the pixel / grid cell. The lookup-table is not considered to be part of the schema, it remains in the tile but is not imported into the "meta" part of the dataset. As such, these lookup-tables can differ slightly between one tile and its neighbour without violating the constraint that single dataset should have only one active schema at a particular point in time.

``complex``

This field must be true if present, and when not present it is implicitly false. If set, this band has two identical data types - a “real” one and an “imaginary” one, and each of these data types is of the size specified - such that the entire band takes up twice the specified size.

``noData``

Specifies which value should not be read literally, but instead should be interpreted as meaning “no data” eg ``0``, ``-1``, ``0xffffffff``, ``+Inf``. If not present, there is no single value that means “no data”.

``hasMask``

This field must be true if present, and when not present it is implicitly false. If true, this band is accompanied by a 1-bit-integer band which controls whether this band is visible/invisible (or data/no-data) at a particular pixel. This mask band is not otherwise shown in the schema.


``unsigned`` This field must be true if present, when not present it is implicitly false. When present, it specifies that an integer band should be treated as unsigned, and when not present, the integer band is interpreted as signed. This property only applies to band with a data type of "integer".


``meta/crs.wkt``
^^^^^^^^^^^^^^^^

This is the Coordinate Reference System that is common to all the tiles, stored in the `Well Known Text format <well_known_text_format_>`_

``meta/band/1/rat.xml``
^^^^^^^^^^^^^^^^^^^^^^^

This is indexed starting at one - ``band/1/rat.xml`` is for the first band, ``band/2/rat.xml`` is for the second band, and so on. Only certain bands will have this metadata attached.

This XML contains the column headings of the `Raster Attribute Table <rat_>`_ associated with this band, if there is one. This information is extracted from the `Persistent Auxiliary Metadata <pam_>`_ (PAM) files (the ``.aux.xml`` files) associated with the tiles, if there are any.

If a particular band has a raster-attribute-table, it must have the same raster attribute table for every tile: that is, every instance of that raster attribute table attached to each tile should have the same number of fields with the same field definitions. However, the rows of the raster attribute table may vary from tile to tile. This is a continuation of the principle that every tile should have the same schema, but the actual data of each tile may vary to any extent.

``meta/band/1/categories.json``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This is indexed starting at one - ``band/1/categories.json`` is for the first band, ``band/2/categories.json`` is for the second band, and so on. Only certain bands will have this metadata attached.

This JSON object describes a mapping from each possible value that the band can take, to a freeform text description of what that value means. As a simple example,
it could be ``{"1": "Land", "2": "Water"}``. This information is extracted from the `Persistent Auxiliary Metadata <pam_>`_ (PAM) files (the ``.aux.xml`` files) associated with the tiles, if there are any.

If a particular band has categories, it must be the same categories for every tile: that is, every instance of that raster attribute table attached to each tile should have same category labels. However, this rule is relaxed slightly in that not every category need be defined for every tile - if one tile has defined categories for the values "1" and "2", but its neighbor only does so for the value "1", that is allowed, so long as they agree on what "1" means.


Tiles
~~~~~

The tiles folder contains one or more tiles in the GeoTIFF format. The name of each tile, and the tiling system used, is chosen by the user - these are not specified by Kart. Raster tiles are often large files, and Kart uses Git object storage which is poorly suited for dealing with large files. So, raster tiles are stored using `Git Large File Storage <git_lfs_>`_. For more information, see the section on :doc:`Git LFS </pages/git_lfs>`.

Git LFS details
^^^^^^^^^^^^^^^

Git LFS splits a single Git object into two pieces of information. The first is small - it is the pointer file - this is held in Git's object storage with a particular name, at a particular path, at one or more particular revisions. The contents of the pointer file is not much more than a hash of the original large file
contents, which is all that is needed to find the original large file in either the local LFS cache, or failing that, at a remote LFS server.

The other part is the contents of the original large file, now stored in another content addressed system, similar to but separate from the Git Object Database. This file is now stored without a name or path or revision information, since the pointer file is responsible for storing that information.

Kart follows these same principles when storing tiles as LFS files, but makes the following changes:

* The path of the tile is still stored as the path to the pointer file (since the LFS file doesn't have a real path) - but for Kart Raster datasets, this path is not wholly chosen by the user. The user chooses the name, and this is used to generate a path that includes that name, but also has a subdirectory for technical reasons. (See :ref:`Path to the pointer file`)

* Extra information is stored in the pointer file - notably the extent of the tile (both in its native coordinate reference system, and with the 2D component of its extent projected to ``EPSG:4326``). This allows for quicker spatial filtering without having to download the entire tile to see if it matches a filter.

Path to the pointer file
^^^^^^^^^^^^^^^^^^^^^^^^

Strictly speaking, this is the path to the pointer file of the tile - see :ref:`Git LFS details`.

For technical reasons, it is best if only a relatively small number of pointer files are stored together in a single directory. This is why, rather than all being stored in a single flat directory, these pointer files are sharded into several directories, with the directory chosen based on the hash of the filename.

The exact path of a tile with a user-chosen name such as ``my-example-tile.tif`` is generated as follows:

1. Any GeoTIFF file extensions such as ``.tif``, or ``.tiff`` are stripped from the name, leaving ``my-example-tile``.
2. A directory named for the first two hexadecimal characters of the SHA256 of the hash is prepended to the path, giving ``f5/my-example-tile``.
