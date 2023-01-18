Legacy Datasets
---------------

Legacy Table Datasets
~~~~~~~~~~~~~~~~~~~~~

+--------------+-------------------------+--------------------------------------------+
| Kart Version | Kart's name at the time | Repository structure version               |
+==============+=========================+============================================+
| 0.0 to 0.1   | Snowdrop                | Table Datasets V0                          |
+--------------+-------------------------+--------------------------------------------+
| 0.2 to 0.4   | Sno                     | Table Datasets V1                          |
+--------------+-------------------------+--------------------------------------------+
| 0.5 to 0.8   | Sno                     | Table Datasets V2                          |
+--------------+-------------------------+--------------------------------------------+
| 0.9          | Kart                    | Table Datasets V2                          |
+--------------+-------------------------+--------------------------------------------+
| 0.10         | Kart                    | Table Datasets V3 (but v2 still supported) |
+--------------+-------------------------+--------------------------------------------+

The current version of the Table Datasets storage format is Table Datasets V3. In this
storage format, each row is stored in a separate file. This means table rows can be stored
using git-style version control, resulting in a database-table which has version history.

The main improvement of Table Datasets V3 is how the rows are divided into
different folders (or "trees" in git terminology) for more efficient
storage when there are a large number of revisions and features. See
:doc:`Table Datasets V2 </pages/development/table_v2>` for more information on the preivous
system.

The main improvement of Table Datasets V2 is that the schema of a table can be
changed in isolation without having to rewrite every row in the table.
Rows that were written with a previous schema are adapted to fit the
current schema when read. See :doc:`Table Datasets V1 </pages/development/table_v1>` for more
information on the previous system.

Legacy Repositories
~~~~~~~~~~~~~~~~~~~
Earlier in Kart's history, the most fundamental feature of a Kart repository was which
of these table-dataset versions it was using. This information was global to the entire repo
and so was stored in a blob at the root-tree called ``.kart.repostructure.version``.

This blob is still present in newer Kart repos, but now in practice always contains nothing but the number '3',
and provides no further information about what other types of datasets the repository may contain.
For newer dataset types (such as Point Clouds) the version information is stored alongside within
the dataset itself, so global version markers like this are no longer necessary for other types of dataset.

If a Kart repo does not contain a ``.kart.repostructure.version`` blob with the number '3' in it, then
it is a legacy repository and can be upgraded. Legacy table datasets are only found in legacy repositories.

Upgrading
~~~~~~~~~

To upgrade a Kart repository to the latest supported repository
structure, run ``kart upgrade SOURCE DEST`` where ``SOURCE`` is the path
to the existing repo, and ``DEST`` is the path to where the upgraded
repo will be created. This will rewrite your repository history — all
commit information is preserved but the commit identifiers will all
change. Merging changes across upgrades will not work out.
