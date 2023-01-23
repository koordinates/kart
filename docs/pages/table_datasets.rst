Working with Vector / Table Datasets
====================================

.. Note:: Before using this quick guide, it will help your understanding to do the :doc:`Basic Usage Tutorial </pages/basic_usage_tutorial>`.

Dataset type
~~~~~~~~~~~~
Both spatial and non-spatial tabular datasets are stored as "Table Datasets", currently implemented as :doc:`Table Datasets V3 </pages/development/table_v3>`. Version controlled table rows are stored in an abstract way, independent of any of the supported working copy types, and so are converted to fit into a specific type of database table during checkout.

Importing vector / table datasets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``kart import <source> [<table>] [<table>]``

This command imports one or more tables into the Kart repository in the
working directory. Tables can be imported with a different name to the name
they have in the source by providing a table specification like so:
``<table_to_import>:<new_name_for_table>``

Data can be imported from any of the following types of databases:

- `GeoPackage <gpkg_>`_
- `PostGIS <postgis_>`_
- `Microsoft SQL Server <sql_server_>`_
- `MySQL <mysql_>`_
- `Shapefiles <shapefiles_>`_

For more information, see :ref:`Import vectors / tables into an existing repository`.

Working copy
~~~~~~~~~~~~

Tabular datasets are stored in a tabular working copy associated with the Kart repository. Every Kart repository has at most one tabular working copy associated with it, and its exact location can be configured by running ``kart create-workingcopy``. This working copy could be a file within the Kart repository, or it could be part of a database on a server somewhere. For more information on configuring the working copy, see :ref:`Managing Working Copies`.

The working copy associated with a Kart repository can be any of the following types:

- `GeoPackage <gpkg_>`_
- `PostGIS <postgis_>`_
- `Microsoft SQL Server <sql_server_>`_
- `MySQL <mysql_>`_

Making edits
~~~~~~~~~~~~

Probably the most readily available way to edit the data in your working copy is to configure a GeoPackage working copy, and open it using `QGIS <qgis_download_>`_.
QGIS can also connect to PostGIS or Microsoft SQL Server databases, depending on the version you have installed, but not MySQL. Depending on the type of edits you need to do, it may be sufficient to connect to your database using a SQL command-line client, such as `sqlite3 <sqlite3_tool_>`_, `psql <psql_tool_>`_, `sqlcmd <sqlcmd_tool_>`_, or `mysql <mysql_tool_>`_, depending on your working copy type.
