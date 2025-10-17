Patch Format
============

Kart patches are JSON files that contain both the changes (diff) and metadata (author, timestamp, message) needed to apply those changes to a repository. Patches are applied using ``kart apply``. Patches can be created from an existing commit using ``kart create-patch``.

Kart's patch format is currently subject to change. If you're building something significant relying on patches, best to get in touch to discuss your use case.


Basic Structure
---------------

A Kart patch file has two main sections:

.. code-block:: json

    {
      "kart.patch/v1": {
        "authorName": "Jane Developer",
        "authorEmail": "jane@example.com",
        "authorTime": "2025-10-21T12:34:56Z",
        "authorTimeOffset": "+12:00",
        "message": "Update feature attributes",
        "base": "abc123..."
      },
      "kart.diff/v1+hexwkb": {
        "dataset-name": {
          "meta": {},
          "feature": []
        }
      }
    }

Patch Metadata (kart.patch/v1)
------------------------------

The ``kart.patch/v1`` section contains commit metadata:

- **authorName** (required): Full name of the patch author
- **authorEmail** (required): Email address of the patch author
- **authorTime** (required): ISO 8601 UTC timestamp when the patch was created
- **authorTimeOffset** (required): Timezone offset in ISO 8601 format (e.g., "+12:00")
- **message** (required): Commit message describing the changes
- **base** (optional): Git commit hash that this patch is based on. When present, enables updates of existing features without a `-` key. Also allows patches to omit unchanged fields from feature diffs.

Diff Data (kart.diff/v1+hexwkb)
-------------------------------

The ``kart.diff/v1+hexwkb`` section contains the actual changes:

.. code-block:: json

    {
      "kart.diff/v1+hexwkb": {
        "my-dataset": {
          "meta": {
            "title": {
              "-": "Old Title",
              "+": "New Title"
            }
          },
          "feature": [
            {
              "+": {
                "fid": 1,
                "name": "New Feature",
                "geom": "0101000000..."
              }
            },
            {
              "-": {
                "fid": 2
              }
            },
            {
              "-": {
                "fid": 3,
                "name": "Old Name",
                "geom": "0101000000..."
              },
              "+": {
                "fid": 3,
                "name": "Updated Name",
                "geom": "0101000000..."
              }
            }
          ]
        }
      }
    }

Feature Changes
~~~~~~~~~~~~~~~

Features are specified in the ``feature`` array. Each element represents a change:

Insert a new feature
^^^^^^^^^^^^^^^^^^^^

To insert a new feature, use the ``+`` key with all required fields:

.. code-block:: json

    {
      "+": {
        "fid": 123,
        "name": "New Feature",
        "geom": "0101000000...",
        "category": "A"
      }
    }

Delete a feature
^^^^^^^^^^^^^^^^

To delete a feature, use the ``-`` key with at minimum the primary key field:

.. code-block:: json

    {
      "-": {
        "fid": 123
      }
    }

Update a feature
^^^^^^^^^^^^^^^^

To update a feature, include both ``-`` (old values) and ``+`` (new values):

.. code-block:: json

    {
      "-": {
        "fid": 123,
        "name": "Old Name",
        "category": "A"
      },
      "+": {
        "fid": 123,
        "name": "New Name",
        "category": "B"
      }
    }

Partial Feature Updates
------------------------

When a patch includes a ``base`` commit hash, feature updates can be **partial** - they don't need to include all fields. Missing fields are automatically resolved from the base commit.

This is useful for:

- Updating only specific attributes without needing to include geometry
- Creating smaller, more focused patches
- Reducing patch file size

Example of a partial update:

.. code-block:: json

    {
      "kart.patch/v1": {
        "base": "abc123...",
        "message": "Update name only",
        ...
      },
      "kart.diff/v1+hexwkb": {
        "my-dataset": {
          "feature": [
            {
              "+": {
                "fid": 123,
                "name": "Updated Name"
              }
            }
          ]
        }
      }
    }

In this example, only the ``name`` field is specified. The ``geom`` and other fields will be preserved from the feature with ``fid=123`` in the base commit.

**Important limitations:**

- Partial updates only work for **existing features** (updates, not inserts)
- The primary key field is always required
- For new feature inserts, all fields must be provided
- For deletes, only the primary key is needed

Metadata Changes
~~~~~~~~~~~~~~~~

Dataset metadata can be changed using the ``meta`` object:

.. code-block:: json

    {
      "meta": {
        "title": {
          "-": "Old Title",
          "+": "New Title"
        },
        "description": {
          "+": "New description"
        },
        "schema.json": {
          "-": {...},
          "+": {...}
        }
      }
    }

Geometry Encoding
-----------------

Geometry fields are encoded as hexadecimal WKB (Well-Known Binary) in the GeoPackage format. The ``v1+hexwkb`` in the key name indicates this encoding.

Binary Fields
-------------

Binary/blob fields are encoded as hexadecimal strings. A ``null`` value for a binary field is represented as JSON ``null``.

Reprojected Patches
-------------------

Patches can include geometries in a different coordinate reference system (CRS) than the target dataset. This allows you to create patches in a convenient CRS (like EPSG:4326/WGS84) and apply them to datasets in any CRS.

Creating Reprojected Patches
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the ``--crs`` option when creating a patch:

.. code-block:: bash

    kart create-patch HEAD --crs=EPSG:4326 > my-changes.kartpatch

The patch will include a ``crs`` field in the metadata:

.. code-block:: json

    {
      "kart.patch/v1": {
        "base": "abc123...",
        "crs": "EPSG:4326",
        "message": "Update features in WGS84",
        ...
      },
      "kart.diff/v1+hexwkb": {
        ...
      }
    }

Applying Reprojected Patches
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When applying a patch with a ``crs`` field, Kart automatically transforms geometries from the patch CRS to the dataset's CRS:

.. code-block:: bash

    kart apply my-changes.kartpatch

**Important limitations for reprojected patches:**

- A ``base`` commit is **required** (for conflict resolution)
- Feature **updates cannot include both** ``-`` **and** ``+`` **keys**

  - Updates with both old and new values would require comparing the ``-`` geometry with the dataset geometry for conflict detection, but CRS transformations are not reliably reversible
  - Instead, use only the ``+`` key to replace/update an existing feature's geometry

- **Inserts and deletes work normally** using ``+`` or ``-`` keys
- **Partial updates work** - specify only ``+`` with the fields you want to change

Example of a valid reprojected update:

.. code-block:: json

    {
      "kart.patch/v1": {
        "base": "abc123...",
        "crs": "EPSG:4326"
      },
      "kart.diff/v1+hexwkb": {
        "my-dataset": {
          "feature": [
            {
              "+": {
                "fid": 123,
                "geom": "0101000000..."
              }
            }
          ]
        }
      }
    }

This updates the geometry (and any other specified fields) of the feature with ``fid=123``, transforming the geometry from EPSG:4326 to the dataset's CRS.

Creating Patches
----------------

To create a patch file from a commit:

.. code-block:: bash

    kart create-patch HEAD > my-changes.kartpatch
    kart create-patch abc123 > my-changes.kartpatch
    kart create-patch HEAD~3..HEAD > my-changes.kartpatch

Applying Patches
----------------

To apply a patch file:

.. code-block:: bash

    kart apply my-changes.kartpatch
    cat my-changes.kartpatch | kart apply -

Options:

- ``--no-commit``: Apply changes to working copy without creating a commit
- ``--ref=<branch>``: Apply patch to a different branch
