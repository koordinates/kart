Point Cloud Datasets V1
-----------------------

Overall structure
~~~~~~~~~~~~~~~~~

A V1 point cloud dataset is a folder named ``.point-cloud-dataset.v1`` that contains two
folders. These are ``meta`` which contains information about the entire dataset,
irrespective of a particular point or tile - and ``tile``, which contains point cloud
tiles in the ``LAZ`` format (that is, compressed ``LAS`` files). The schema file contains
the structure of the each point, ie the name and type of each field that is stored per point.
The "name" of the dataset is the path to the ``.point-cloud-dataset.v1`` folder.

For example, here is the basic folder structure of a dataset named
``lidar/alpine-fault``:

::

   lidar/
   lidar/alpine-fault/
   lidar/alpine-fault/.point-cloud-dataset.v1/
   lidar/alpine-fault/.point-cloud-dataset.v1/meta/
   lidar/alpine-fault/.point-cloud-dataset.v1/meta/title              # Title of the dataset
   lidar/alpine-fault/.point-cloud-dataset.v1/meta/description        # Description of the dataset
   lidar/alpine-fault/.point-cloud-dataset.v1/meta/format.json        # Format of the dataset
   lidar/alpine-fault/.point-cloud-dataset.v1/meta/schema.json        # Schema of the dataset
   lidar/alpine-fault/.point-cloud-dataset.v1/meta/crs.wkt            # CRS of the dataset
   lidar/alpine-fault/.point-cloud-dataset.v1/meta/...                # Other dataset metadata

   lidar/alpine-fault/.point-cloud-dataset.v1/tile/...                # All points in the point-cloud
                                                                      # (spread across one or more tiles, all LAZ files)

Meta items
~~~~~~~~~~

The following items are stored in the meta part of the dataset, and have
the following structure.

``meta/title``
^^^^^^^^^^^^^^

Contains the title of the dataset, encoded using UTF-8. The title is freeform text.

``meta/description``
^^^^^^^^^^^^^^^^^^^^

A long-form description of the dataset, encoded using UTF-8. The description is freeform text.

``meta/format.json``
^^^^^^^^^^^^^^^^^^^^

Contains information about constraints that the LAS tiles must follow.
All the tiles must be LAS files, but there are various versions and flavours of LAS and one dataset cannot contain a mixture of all of them.
At a minimum, the dataset's tiles must be constrained in such a way that every tile has the same schema (see :ref:`meta/schema.json <pointcloud-meta-schema-json>` below).
Datasets may optionally have further constraints - for instance, they may be constrained to conform to the `COPC specification <copc_>`_,
which is a subtype of LAS file which allows for fasting seeking and rendering of whichever part of the point cloud is currently within the user's viewport.

For example, here is the format of a dataset that is constrained to use ``LAZ`` compression (rather than uncompressed ``LAS``), must conform to Point Data Record Format 7 of LAS version 1.4, and the points must be chunked according to the ``COPC 1.0`` specification.

.. code:: json

    {
      "compression": "laz",
      "lasVersion": "1.4",
      "optimization": "copc",
      "optimizationVersion": "1.0",
      "pointDataRecordFormat": 7,
      "pointDataRecordLength": 36
    }

If certain constraints are relaxed - for example, if any LAS version is allowed - then the relevant fields are ommitted.

.. _pointcloud-meta-schema-json:

``meta/schema.json``
^^^^^^^^^^^^^^^^^^^^

Contains the current schema of the table, as a JSON array. Each item in
the array represents a field that is stored for each point. Most
of these fields are determined by the LAS specification, according to
the "Point Data Record Format" specified in the LAS file.

For example, this is the schema of a dataset using "PDRF 7":

.. code:: json

    [
      {
        "name": "X",
        "dataType": "integer",
        "size": 32
      },
      {
        "name": "Y",
        "dataType": "integer",
        "size": 32
      },
      {
        "name": "Z",
        "dataType": "integer",
        "size": 32
      },
      {
        "name": "Intensity",
        "dataType": "integer",
        "size": 16,
        "unsigned": true
      },
      {
        "name": "Return Number",
        "dataType": "integer",
        "size": 4,
        "unsigned": true
      },
      {
        "name": "Number of Returns",
        "dataType": "integer",
        "size": 4,
        "unsigned": true
      },
      {
        "name": "Synthetic",
        "dataType": "integer",
        "size": 1
      },
      {
        "name": "Key-Point",
        "dataType": "integer",
        "size": 1
      },
      {
        "name": "Withheld",
        "dataType": "integer",
        "size": 1
      },
      {
        "name": "Overlap",
        "dataType": "integer",
        "size": 1
      },
      {
        "name": "Scanner Channel",
        "dataType": "integer",
        "size": 2,
        "unsigned": true
      },
      {
        "name": "Scan Direction Flag",
        "dataType": "integer",
        "size": 1
      },
      {
        "name": "Edge of Flight Line",
        "dataType": "integer",
        "size": 1
      },
      {
        "name": "Classification",
        "dataType": "integer",
        "size": 8,
        "unsigned": true
      },
      {
        "name": "User Data",
        "dataType": "integer",
        "size": 8,
        "unsigned": true
      },
      {
        "name": "Scan Angle",
        "dataType": "integer",
        "size": 16
      },
      {
        "name": "Point Source ID",
        "dataType": "integer",
        "size": 16,
        "unsigned": true
      },
      {
        "name": "GPS Time",
        "dataType": "float",
        "size": 64
      },
      {
        "name": "Red",
        "dataType": "integer",
        "size": 16,
        "unsigned": true
      },
      {
        "name": "Green",
        "dataType": "integer",
        "size": 16,
        "unsigned": true
      },
      {
        "name": "Blue",
        "dataType": "integer",
        "size": 16,
        "unsigned": true
      }
    ]


Note: Kart vs PDAL schema extraction
####################################

Kart uses `PDAL <pdal_>`_ internally to read and write LAS files. PDAL is an abstraction layer that can read data from a variety of different
types of point cloud files, and as such, it interprets the schema in its own way to make it more interoperable with the rest of PDAL.
The schema that Kart conveys is schema of the LAS file as it is stored or specified, not as PDAL reads it, although these two concepts are very similar. Here are some differences between stored / specified schema and PDAL's interpretation:

* Where the specification gives a dimension's name as multiple words, ie "Number of Returns", PDAL reports it in CamelCase, ie "NumberOfReturns".
* PDAL converts some dimensions which are technically stored as integers to floating point values as it applies scaling factors to them - for example, X, Y, and Z.
* Sometimes PDAL loads newer and older versions of a particular dimension in a version-independent way - ie the older 8-bit field "Scan Angle Rank" and the newer 16-bit field "Scan Angle" are both loaded as "ScanAngleRank", and both converted to floating point.

If you need to see PDAL's interpretation of a schema instead of Kart's, you can run ``pdal info --schema <FILENAME>``.
A PDAL command-line executable can be found in the directory where Kart is installed.

``meta/crs.wkt``
^^^^^^^^^^^^^^^^

This is the Coordinate Reference System used to interpret each point, stored in the `Well Known Text format <well_known_text_format_>`_

Tiles
~~~~~

The tiles folder contains one or more tiles in the LAS format. The name of each tile, and the tiling system used, is chosen by the user - these are not specified by Kart. Point cloud tiles are often large files, and Kart uses Git object storage which is poorly suited for dealing with large files. So, point cloud tiles are stored using `Git Large File Storage <git_lfs_>`_. For more information, see the section on :doc:`Git LFS </pages/git_lfs>`.

Git LFS details
^^^^^^^^^^^^^^^

Git LFS splits a single Git object into two pieces of information. The first is small - it is the pointer file - this is held in Git's object storage with a particular name, at a particular path, at one or more particular revisions. The contents of the pointer file is not much more than a hash of the original large file
contents, which is all that is needed to find the original large file in either the local LFS cache, or failing that, at a remote LFS server.

The other part is the contents of the original large file, now stored in another content addressed system, similar to but separate from the Git Object Database. This file is now stored without a name or path or revision information, since the pointer file is responsible for storing that information.

Kart follows these same principles when storing tiles as LFS files, but makes the following changes:

* The path of the tile is still stored as the path to the pointer file (since the LFS file doesn't have a real path) - but for Kart Point Cloud datasets, this path is not wholly chosen by the user. The user chooses the name, and this is used to generate a path that includes that name, but also has a subdirectory for technical reasons. (See :ref:`Path to the pointer file`)

* Extra information is stored in the pointer file - notably the extent of the tile (both in its native coordinate reference system, and with the 2D component of its extent projected to ``EPSG:4326``). This allows for quicker spatial filtering without having to download the entire tile to see if it matches a filter.

Path to the pointer file
^^^^^^^^^^^^^^^^^^^^^^^^

Strictly speaking, this is the path to the pointer file of the tile - see :ref:`Git LFS details`.

For technical reasons, it is best if only a relatively small number of pointer files are stored together in a single directory. This is why, rather than all being stored in a single flat directory, these pointer files are sharded into several directories, with the directory chosen based on the hash of the filename.

The exact path of a tile with a user-chosen name such as ``my-example-tile.copc.laz`` is generated as follows:

1. Any LAS file extensions such as ``.las``, ``.laz`` and ``.copc.laz`` are stripped from the name, leaving ``my-example-tile``.
2. A directory named for the first two hexadecimal characters of the SHA256 of the hash is prepended to the path, giving ``f5/my-example-tile``.
