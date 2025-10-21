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
- **base** (optional): Git commit hash that this patch is based on. When present, enables partial feature updates (see below)

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
