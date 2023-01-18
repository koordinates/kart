Datasets
--------

A Kart repository may contain one or more version-controlled datasets.
Each dataset is homogenous, that is to say, the same type of information is stored for every piece of data in the dataset.
There are a few different types of datasets that Kart supports.

Types of Datasets
~~~~~~~~~~~~~~~~~

Kart supports the following types of datasets:

+--------------------+--------------------------------------------------------------------+-------------------+
| Dataset type       | Storage format specification                                       | Supported since   |
+====================+====================================================================+===================+
| Table (or vector)  | :doc:`Table Datasets V3 </pages/development/table_v3>`             | Kart 0.10         |
+--------------------+--------------------------------------------------------------------+-------------------+
| Point Cloud        | :doc:`Point Cloud Datasets V1 </pages/development/pointcloud_v1>`  | Kart 0.12         |
+--------------------+--------------------------------------------------------------------+-------------------+

There are some older variants of the table storage format (hence the V3), but most users will not encounter these any more.


Dataset Names
~~~~~~~~~~~~~

A dataset is stored in a hidden folder with a name starting with a dot, eg ``.table-dataset``.
The name of this hidden folder depends on the type of the dataset.
The path to this hidden folder is the name of the dataset.
For example, a table dataset called ``hydro/soundings`` would have its data stored in a hidden folder named ``hydro/soundings/.table-dataset``.


Dataset Name Limitations
^^^^^^^^^^^^^^^^^^^^^^^^

Kart enforces the following rules about these paths:

-  Paths may contain most unicode characters
-  Paths must not contain any ASCII control characters (codepoints 00 to
   1F), or any of the characters ``:``, ``<``, ``>``, ``"``, ``|``,
   ``?``, or ``*``
-  Paths must not start or end with a ``/``
-  No path component (``/``-separated) may:

   -  be empty
   -  start or end with a ``.``
   -  end with a ` ` (space)
   -  be any of these `reserved Windows
      filenames <reserved_windows_filenames_>`_:
      ``CON``, ``PRN``, ``AUX``, ``NUL``, ``COM1``, ``COM2``, ``COM3``,
      ``COM4``, ``COM5``, ``COM6``, ``COM7``, ``COM8``, ``COM9``,
      ``LPT1``, ``LPT2``, ``LPT3``, ``LPT4``, ``LPT5``, ``LPT6``,
      ``LPT7``, ``LPT8``, ``LPT9``.

-  Repositories may not contain more than one dataset with names that
   differ only by case.

Additionally, backslashes (``\``) in dataset paths are converted to
forward slashes (``/``) when imported.

These rules exist to help ensure that Kart repositories can be checked
out on a range of operating systems and filesystems.

Dataset Contents
~~~~~~~~~~~~~~~~

Datasets contain at one folder at the top level called ``meta``. This contains the dataset's metadata, which is generally needed to properly interpret the bulk of - the dataset's data. That metadata could be its schema, its Coordinate Reference System, etc. This metadata is limited in size (compared to the bulk of the data) and somewhat human readable - it could be any or all of of plain-text, JSON, Well-Known-Text, and XML.

The bulk of the data will be stored in a different folder or folders at the top level. The name of this folder and the storage format of this data depends entirely on the type of dataset.

For more details on the dataset storage formats, see :ref:`Types of Datasets`.
