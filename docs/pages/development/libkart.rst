libkart C API
-------------

``libkart`` is a native C-ABI shared library (written in Rust) that lets an
external program read Kart repositories *in-process*, instead of shelling out to
the ``kart`` CLI. It is loaded directly into the host process (for example via a
`cffi <https://cffi.readthedocs.io/>`_ wrapper in Python) and exposes a small,
stable C ABI for opening a repo, listing and opening
:doc:`datasets </pages/development/datasets>`, reading feature/tile blobs, and
decoding GeoPackage geometries.

The library ships inside the Kart bundle, alongside the ``kart`` executable:

- shared library: ``_internal/libkart.so`` (Linux), ``_internal/libkart.dylib``
  (macOS), or ``_internal/libkart.dll`` (Windows)
- C header: ``_internal/share/kart/libkart.h``

The ``_internal/`` directory lives inside the installed Kart application
directory (the same place the ``kart`` executable is shipped from); if you do not
know that path at build time, locate the bundle from the installed ``kart``
binary at runtime, or set your own environment variable pointing at the library.
The header is C and C++ compatible (wrapped in ``extern "C"``) and includes
``<stddef.h>`` and ``<stdint.h>``; the ``uint64_t`` / ``uint8_t`` / ``size_t``
types used below are the standard fixed-width/size types from those headers.

.. c:type:: uint64_t

.. c:type:: uint8_t

.. c:type:: size_t

What libkart does and does not do
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

libkart decodes Kart's storage formats, but it is **not** a git library: it does
not open arbitrary git objects, walk trees, or enumerate the features/tiles in a
dataset for you. The two functions that decode an individual feature or tile
(:c:func:`kart_feature_geometry`, :c:func:`kart_tile_summary_json`) take *raw
git blob bytes that you read yourself* using a separate git library (e.g.
`pygit2 <https://www.pygit2.org/>`_ / libgit2). See
`Reading feature and tile blobs`_ for how to obtain those bytes.

Terminology
~~~~~~~~~~~

A few Kart-internal terms appear below. You normally do not need to understand
the storage format in detail — libkart decodes it for you — but the glosses help
make sense of the error messages and the feature/tile inputs:

- **refish** — any git ref or commit-ish: a branch or tag name, ``HEAD``, or a
  commit hash. (The special values ``""`` and ``"[EMPTY]"`` resolve to the empty
  tree, and ``"HEAD"`` on an unborn HEAD also resolves to the empty tree; see
  :c:func:`kart_repo_list_datasets`.)
- **dataset's internal tree** — each dataset is stored under a hidden child
  directory (``.table-dataset`` for tables, ``.point-cloud-dataset.v1`` for point
  clouds). It contains a ``meta/`` subtree (schema, CRS, etc.) and a ``feature/``
  (table) or ``tile/`` (point-cloud) subtree of blobs. See
  :doc:`/pages/development/table_v3` and :doc:`/pages/development/pointcloud_v1`.
- **legend** — a small per-revision blob in ``meta/legend/`` that maps a stored
  feature's value slots (which omit the primary-key columns) back to schema
  column ids. libkart loads it for you to find which slot holds the geometry; you
  never construct one.
- **WKB** — the OGC Well-Known Binary geometry encoding. **GPKG /
  StandardGeoPackageBinary** — Kart's name for the standard
  `GeoPackage binary geometry encoding <gpkg_gpb_data_blob_format_>`_ (a small
  header followed by WKB); see `GPKG geometry byte format`_.

Typical call sequence
~~~~~~~~~~~~~~~~~~~~~~

#. :c:func:`kart_repo_open` — open the repo, get a repo handle.
#. :c:func:`kart_repo_list_datasets` — list dataset paths at a refish.
#. :c:func:`kart_dataset_open` — open one dataset, get a dataset handle.
#. :c:func:`kart_dataset_type` / :c:func:`kart_dataset_schema_json` /
   :c:func:`kart_dataset_crs_wkt` — inspect the dataset. The type tells you which
   decoder applies: ``table`` => :c:func:`kart_feature_geometry`;
   ``point-cloud`` => :c:func:`kart_tile_summary_json`; ``raster`` /
   ``unsupported`` => no per-blob reader is provided. (Raster is not yet
   implemented rather than fundamentally different: raster tiles are LFS
   pointers like point-cloud tiles, but their summaries also involve the PAM
   sidecar files, and no consumer needs them yet.)
#. *Read blobs yourself* (see `Reading feature and tile blobs`_): use your own
   git library to walk the dataset's ``feature/`` or ``tile/`` subtree and read
   each blob's raw bytes.
#. :c:func:`kart_feature_geometry` / :c:func:`kart_tile_summary_json` — decode a
   blob; then optionally :c:func:`kart_gpkg_to_wkb` etc. on the geometry.
#. Free every out-buffer with :c:func:`kart_free`, and every handle with
   :c:func:`kart_repo_free` / :c:func:`kart_dataset_free`.

A complete worked example is in `Usage example (Python / cffi)`_.

ABI conventions
~~~~~~~~~~~~~~~

Return codes
^^^^^^^^^^^^

Every fallible function returns an ``int`` return code: ``0`` means success and
``-1`` means error. The two free functions (:c:func:`kart_repo_free`,
:c:func:`kart_dataset_free`) and :c:func:`kart_free` return ``void``;
:c:func:`kart_last_error` returns ``const char *``.

On error (rc ``-1``) the failing function sets a thread-local message
retrievable via :c:func:`kart_last_error`. Messages are prefixed by category,
e.g. ``not implemented:``, ``not found:``, ``format error:``, ``git error:``,
``msgpack error:``, ``json error:``, ``utf-8 error:``.

Handles
^^^^^^^

Repos and datasets are referred to by opaque ``uint64_t`` handles; ``0`` is
never a valid handle.

- Free a repo handle with :c:func:`kart_repo_free` and a dataset handle with
  :c:func:`kart_dataset_free`.
- A dataset is an independent in-memory snapshot: at open time it eagerly copies
  the dataset's ``meta/`` subtree, and it does **not** keep the repo alive. You
  may free the repo and keep using datasets opened from it.
- Passing an unknown/already-freed handle to a fallible function returns rc
  ``-1`` with ``not found: repo handle`` or ``not found: dataset handle``.
  Passing an unknown handle to a free function is a silent no-op.

Returned buffers and memory ownership
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Functions that return data through ``char **`` / ``uint8_t **`` (plus a
``size_t *`` length) out-params allocate the buffer with ``malloc``. The caller
**must** release it with :c:func:`kart_free` (never with their own allocator's
``free``). ``kart_free(NULL)`` is a safe no-op.

Returned text/JSON buffers are **not** NUL-terminated. Always use the
accompanying ``*out_len``; never call ``strlen`` on them. JSON buffers are
UTF-8-encoded JSON bytes.

Absent vs. error
^^^^^^^^^^^^^^^^

A buffer-returning function whose result is logically absent or empty returns rc
``0`` with ``*out == NULL`` and ``*out_len == 0`` (no buffer to free). Callers
must treat a ``NULL`` out-pointer as "absent", which is distinct from an error
(rc ``-1``).

Input strings
^^^^^^^^^^^^^

C string in-arguments (``path``, ``refish``, ``name``) must be valid
NUL-terminated UTF-8. A ``NULL`` pointer yields rc ``-1``
``format error: unexpected NULL string argument``; invalid UTF-8 yields a utf-8
error. Byte ``(ptr, len)`` in-arguments are treated as an empty slice if the
pointer is ``NULL`` or ``len`` is ``0``.

Error message lifetime
^^^^^^^^^^^^^^^^^^^^^^^

:c:func:`kart_last_error` returns a pointer owned by libkart that is valid only
until the *next* libkart call on the same thread. Copy the string if you need to
keep it. Never free it.

Thread-safety
^^^^^^^^^^^^^

The handle registries are mutex-protected, so calls are thread-safe.
``last_error`` is thread-local, so each thread reads its own most-recent error.
Calls that touch the same registry serialize: each operation holds the lock for
its duration, so do not call another libkart function that touches the same
registry from a thread already blocked inside one (e.g. via a host-language
signal handler or reentrant wrapper). No explicit initialization is required —
functions are usable as soon as the library is loaded. Handles live in
process-global registries; freeing them is optional at process exit but required
to avoid leaks in a long-running host.

Function reference
~~~~~~~~~~~~~~~~~~

Repo functions
^^^^^^^^^^^^^^

.. c:function:: int kart_repo_open(const char *path, uint64_t *out_repo)

   Open the Kart repository rooted at ``path``, returning an opaque repo handle.
   Pass the path to the Kart repository directory (the folder created by ``kart
   init`` / ``kart clone``).

   .. note::

      Implementation detail: it looks for the git dir under ``.kart/``
      (preferred) or legacy ``.sno/`` beneath ``path``, falling back to treating
      ``path`` itself as a bare git dir if neither exists.

   :param path: NUL-terminated UTF-8 filesystem path to the repo root.
   :param out_repo: out-param receiving the new (non-zero) repo handle on success.
   :returns:
      - ``*out_repo`` — the new, non-zero repo handle. Free it with
        :c:func:`kart_repo_free`.
   :errors:
      - ``path`` is not a Kart repository (``git error:``).

.. c:function:: void kart_repo_free(uint64_t repo)

   Release a repo handle previously returned by :c:func:`kart_repo_open`. No-op
   if the handle is unknown/already freed. Datasets opened from this repo remain
   valid after freeing it (they hold their own snapshot).

   :param repo: the handle to free.

.. c:function:: int kart_repo_table_dataset_version(uint64_t repo, int *out_version)

   Return the Kart table-dataset repository-structure format version (e.g. ``3``
   for current ``.kart`` repos, ``2`` for legacy ``.sno``).

   .. note::

      Implementation detail — resolution order: (1) read the version blob in the
      ``HEAD`` root tree (``.kart.repostructure.version`` => 3-style, or legacy
      ``.sno.repository.version`` => 2-style; the blob's own integer contents are
      parsed); (2) fall back to git config keys ``kart.repostructure.version``
      then ``sno.repository.version``; (3) default to ``3`` if none found.

   :param repo: repo handle.
   :param out_version: out-param receiving the integer version.
   :returns:
      - ``*out_version`` — the integer format version (e.g. ``3``).
   :errors:
      - unknown repo handle (``not found: repo handle``).
      - the version blob is not UTF-8 (``utf-8 error:``) or not an integer
        (``format error: invalid version blob contents: ...``).
      - a git error.

.. c:function:: int kart_repo_list_datasets(uint64_t repo, const char *refish, uint8_t **out_json, size_t *out_len)

   List the paths of all datasets present at a given refish, as a JSON array of
   strings (sorted ascending). A tree is recognised as a dataset if it has a
   direct child tree whose name matches the dataset-dir pattern
   (``\.[^/]*-dataset[^/]*``); dot-prefixed (hidden) trees are skipped during the
   walk.

   :param repo: repo handle.
   :param refish: NUL-terminated UTF-8 git refish (see refish under
      `Terminology`_). An empty string or ``"[EMPTY]"`` resolves to the git empty
      tree; ``"HEAD"`` on an unborn/missing HEAD also resolves to the empty tree
      (=> empty list).
   :param out_json: out-param receiving a malloc'd UTF-8 JSON buffer.
   :param out_len: out-param receiving the buffer length.
   :returns:
      - ``*out_json`` — a UTF-8 JSON array of dataset path strings, e.g.
        ``["census2016_sdhca_ot_ra_short","nz_pa_points_topo_150k"]``. An empty
        repo yields ``"[]"`` (still a buffer, not ``NULL``). Free with
        :c:func:`kart_free`.
   :errors:
      - unknown repo handle (``not found: repo handle``).
      - a git error.

Dataset functions
^^^^^^^^^^^^^^^^^

.. c:function:: int kart_dataset_open(uint64_t repo, const char *refish, const char *path, uint64_t *out_ds)

   Open the dataset at ``path`` as it exists at ``refish``, returning an opaque
   dataset handle. Eagerly loads the dataset's entire ``meta/`` subtree
   (``schema.json``, legends, CRS WKT, format, etc.) into memory so subsequent
   calls need no further git access and the repo need not stay alive.

   Opening succeeds for any recognised dataset directory, including ones whose
   type libkart does not specifically support (their :c:func:`kart_dataset_type`
   is ``unsupported``).

   .. note::

      Implementation detail — inner-dir-to-type mapping: ``.table-dataset`` /
      ``.sno-dataset`` => ``table``, ``.point-cloud-dataset.v1`` =>
      ``point-cloud``, ``.raster-dataset.v1`` => ``raster``, any other
      ``.*-dataset*`` => ``unsupported``.

   :param repo: repo handle.
   :param refish: NUL-terminated UTF-8 refish (see refish under `Terminology`_).
   :param path: NUL-terminated UTF-8 dataset path within the repo (e.g.
      ``"nz_pa_points_topo_150k"``); must be non-empty.
   :param out_ds: out-param receiving the new (non-zero) dataset handle.
   :returns:
      - ``*out_ds`` — the new, non-zero dataset handle. Free with
        :c:func:`kart_dataset_free`.
   :errors:
      - unknown repo handle (``not found: repo handle``).
      - empty ``path`` (``not found: empty dataset path``).
      - ``path`` not found, or not a tree (``not found: dataset path not found:
        ...`` / ``... is not a tree: ...``).
      - no dataset directory under ``path`` (``not found: no dataset dir under
        path: ...``).
      - a git or parse error.

.. c:function:: void kart_dataset_free(uint64_t ds)

   Release a dataset handle previously returned by :c:func:`kart_dataset_open`.
   No-op if the handle is unknown/already freed.

   :param ds: the handle to free.

.. c:function:: int kart_dataset_type(uint64_t ds, uint8_t **out, size_t *out_len)

   Return the dataset's Kart type string.

   :param ds: dataset handle.
   :param out: out-param for a malloc'd UTF-8 text buffer.
   :param out_len: out-param receiving the buffer length.
   :returns:
      - ``*out`` — one of ``table``, ``point-cloud``, ``raster`` or
        ``unsupported``. Always present for an open dataset (never ``NULL``); not
        NUL-terminated, so use ``out_len``. Free with :c:func:`kart_free`.
   :errors:
      - unknown dataset handle (``not found: dataset handle``).

.. c:function:: int kart_dataset_schema_json(uint64_t ds, uint8_t **out, size_t *out_len)

   Return a JSON object summarising the dataset: its path, type, whether it has a
   geometry column, primary key, geometry column name, and full column list. See
   `kart_dataset_schema_json output`_ for the object shape.

   :param ds: dataset handle.
   :param out: out-param for a malloc'd UTF-8 JSON buffer.
   :param out_len: out-param receiving the buffer length.
   :returns:
      - ``*out`` — a UTF-8 JSON object (never ``NULL``); see
        `kart_dataset_schema_json output`_. With no ``schema.json``, ``columns``
        is ``[]`` and the geometry/primary-key fields are ``null``. Free with
        :c:func:`kart_free`.
   :errors:
      - unknown dataset handle (``not found: dataset handle``).
      - a JSON serialization error.

.. c:function:: int kart_dataset_crs_wkt(uint64_t ds, uint8_t **out, size_t *out_len)

   Return the WKT of the dataset's geometry CRS.

   :param ds: dataset handle.
   :param out: out-param for a malloc'd UTF-8 text buffer.
   :param out_len: out-param receiving the buffer length.
   :returns:
      - ``*out`` — the CRS WKT string, when the dataset has a geometry column
        whose ``geometryCRS`` resolves to a stored ``crs/<crsName>.wkt`` meta
        item. Free with :c:func:`kart_free`.
      - ``*out == NULL`` (absent) when there is no such CRS: no ``schema.json``,
        no geometry column, no ``geometryCRS``, or the ``crs/*.wkt`` is missing.
   :errors:
      - unknown dataset handle (``not found: dataset handle``).
      - bad schema JSON, or non-UTF-8 WKT (``utf-8 error:``).

.. c:function:: int kart_dataset_meta_item(uint64_t ds, const char *name, uint8_t **out, size_t *out_len)

   Return the raw bytes of a named meta item from the dataset's ``meta/`` subtree
   (passthrough, no parsing). Common keys are ``schema.json``, ``format.json``,
   ``title``, ``description`` and ``crs/<crsName>.wkt``; content-addressed
   ``legend/<hash>`` blobs also live here. Most callers should prefer the
   higher-level :c:func:`kart_dataset_schema_json` / :c:func:`kart_dataset_crs_wkt`
   rather than fetching meta items by name. There is no API to enumerate the
   available meta keys; see :doc:`/pages/development/table_v3` for the layout.

   :param ds: dataset handle.
   :param name: NUL-terminated UTF-8 key relative to ``meta/`` (e.g.
      ``"schema.json"``, ``"format.json"``, ``"title"``, ``"crs/EPSG:4326.wkt"``,
      ``"legend/<hash>"``).
   :param out: out-param for a malloc'd raw byte buffer.
   :param out_len: out-param receiving the buffer length.
   :returns:
      - ``*out`` — the raw meta-item bytes when ``name`` exists, returned
        verbatim (may be JSON, plain text, msgpack, ...) and **not**
        NUL-terminated. Free with :c:func:`kart_free`.
      - ``*out == NULL`` (absent) when ``name`` is not present.
   :errors:
      - ``NULL`` or invalid ``name``.
      - unknown dataset handle (``not found: dataset handle``).

Feature and tile functions
^^^^^^^^^^^^^^^^^^^^^^^^^^

Reading feature and tile blobs
""""""""""""""""""""""""""""""

The two functions below decode an *individual* feature or tile from its raw git
blob bytes. **libkart does not read those blobs for you** — it provides no
tree-walking or blob-reading API. You must read them yourself with a git library
(e.g. `pygit2 <https://www.pygit2.org/>`_ / libgit2):

#. Resolve the refish to a commit/tree with your git library.
#. Descend into the dataset's path, then into its hidden internal tree
   (``.table-dataset`` for tables, ``.point-cloud-dataset.v1`` for point clouds).
#. Walk the ``feature/`` (table) or ``tile/`` (point-cloud) subtree and read each
   leaf blob's raw bytes. The blob *paths* are an encoded layout; you do not need
   to decode them, only to read the blob contents.
#. Pass each blob's bytes to :c:func:`kart_feature_geometry` (table) or
   :c:func:`kart_tile_summary_json` (point-cloud).

For the on-disk subtree layout and feature/tile encoding, see
:doc:`/pages/development/table_v3` and :doc:`/pages/development/pointcloud_v1`.
Raster and ``unsupported`` datasets have no per-blob reader here; use
:c:func:`kart_dataset_meta_item` and external tooling instead.

.. c:function:: int kart_feature_geometry(uint64_t ds, const uint8_t *blob, size_t blob_len, uint8_t **out, size_t *out_len)

   Decode a single table/vector feature's git blob and extract its
   `GeoPackage geometry bytes <GPKG geometry byte format_>`_ for the dataset's
   geometry column. Given the raw feature blob, returns the GPKG geometry (or
   absent if the feature has no geometry); the dataset must be a table dataset
   with a geometry column.

   .. note::

      Internal encoding (not needed to call this function): the blob
      msgpack-decodes to ``[legend, non-pk-values]``; libkart loads the named
      legend from the dataset's ``meta/legend/`` (see legend under
      `Terminology`_) to find the geometry value's slot, which must be a msgpack
      ext code ``'G'`` (``0x47``) carrying GPKG bytes.

   :param ds: dataset handle (must be a table dataset with a geometry column to
      yield anything).
   :param blob: the raw feature blob bytes you read from git; ``NULL``/``0`` is
      treated as an empty slice.
   :param blob_len: length of ``blob``.
   :param out: out-param for a malloc'd GPKG geometry byte buffer.
   :param out_len: out-param receiving the buffer length.
   :returns:
      - ``*out`` — GPKG geometry bytes (starting with magic ``GP``) when the
        feature has a non-null geometry. Free with :c:func:`kart_free`.
      - ``*out == NULL`` (absent) when there is no geometry: the dataset or the
        feature's legend has no geometry column, or the value is null.
   :errors:
      - unknown dataset handle (``not found: dataset handle``).
      - a malformed blob or legend (msgpack/format error), a missing legend
        (``not found: legend not found in meta: ...``), or an unexpected
        non-geometry value.

.. c:function:: int kart_tile_summary_json(uint64_t ds, const uint8_t *blob, size_t blob_len, uint8_t **out, size_t *out_len)

   Decode a point-cloud tile's Git-LFS pointer blob into a JSON summary object.
   See `kart_tile_summary_json output`_ for the object shape.

   A point-cloud tile's git blob is a Git-LFS *pointer*: a small text file that
   references the real ``.laz`` / ``.copc`` tile data (stored separately) by a
   content hash (``oid``). This function summarises that pointer; it does not
   read the tile data itself.

   :param ds: dataset handle (point-cloud).
   :param blob: the raw LFS pointer file text bytes you read from git;
      ``NULL``/``0`` => empty slice.
   :param blob_len: length of ``blob``.
   :param out: out-param for a malloc'd UTF-8 JSON buffer.
   :param out_len: out-param receiving the buffer length.
   :returns:
      - ``*out`` — a UTF-8 JSON object summarising the tile (never ``NULL``); see
        `kart_tile_summary_json output`_. Free with :c:func:`kart_free`.
   :errors:
      - unknown dataset handle (``not found: dataset handle``).
      - a non-UTF-8 pointer blob, a bad size field, or bad base64/msgpack.

   .. note::

      The C ABI provides no way to pass the tile's git blob name, so the
      ``name`` key (the tile filename) is **absent** from the returned JSON.
      Derive the filename yourself from the git blob's name (using the returned
      ``format`` for the extension).

GPKG geometry functions
^^^^^^^^^^^^^^^^^^^^^^^

These operate on raw `StandardGeoPackageBinary <gpkg_gpb_data_blob_format_>`_
geometry bytes (Kart's name for the standard GeoPackage binary geometry
encoding; see `GPKG geometry byte format`_) and require no dataset handle.

.. c:function:: int kart_gpkg_is_empty(const uint8_t *g, size_t n, int *out)

   Test whether a StandardGeoPackageBinary geometry is flagged empty.

   :param g: GPKG geometry bytes (``NULL``/``0`` => empty slice, which fails the
      header parse).
   :param n: length of ``g``.
   :param out: out-param receiving ``0``/``1`` (``1`` = empty).
   :returns:
      - ``*out`` — ``1`` if the geometry's empty flag is set, else ``0``.
   :errors:
      - the GPKG header cannot be parsed: too short, missing ``GP`` magic
        (``format error: Expected GeoPackage Binary Geometry``), unsupported
        version, an extended header, or an invalid envelope indicator.

.. c:function:: int kart_gpkg_geometry_type(const uint8_t *g, size_t n, int *out)

   Return the OGR/ISO WKB geometry type code of a GPKG geometry, read using the
   WKB's own endianness byte. See `WKB type codes`_.

   :param g: GPKG geometry bytes.
   :param n: length of ``g``.
   :param out: out-param receiving the WKB type code.
   :returns:
      - ``*out`` — the ISO/OGR WKB type integer (e.g. ``1`` Point, ``2``
        LineString, ``3`` Polygon, ``1003`` Polygon Z); see `WKB type codes`_.
   :errors:
      - the GPKG header cannot be parsed, or the WKB is truncated
        (``format error: GPKG geometry truncated WKB``).

.. c:function:: int kart_gpkg_envelope(const uint8_t *g, size_t n, int only_2d, int calc, double *out6, int *out_count)

   Read the GPKG geometry's stored bounding-box envelope into a caller-supplied
   array of doubles. See `Envelope tuple order`_.

   :param g: GPKG geometry bytes.
   :param n: length of ``g``.
   :param only_2d: if non-zero, truncate the result to the first 4 doubles (drop Z/M).
   :param calc: ``calculate_if_missing`` flag.
   :param out6: caller-provided array of at least 6 doubles to fill.
   :param out_count: out-param set to the number of valid doubles written
      (``0``, ``4``, or ``6``).
   :returns:
      - ``*out_count`` ``4`` (XY) or ``6`` (XYZ), with ``out6[0..*out_count]``
        filled. An 8-double XYM/ZM envelope is capped to the first 6.
      - ``*out_count == 0`` and ``out6`` untouched when there is no usable
        envelope: the geometry is flagged empty, none is stored (and
        ``calc == 0``), or a stored value is ``NaN``.
   :errors:
      - the GPKG header cannot be parsed, or the envelope is truncated.
      - ``calc != 0`` but no envelope is stored (see the warning below).

   .. warning::

      ``calculate_if_missing`` is **not yet implemented**, and only fires when
      there is literally no stored envelope (envelope indicator ``0``) *and*
      ``calc != 0``: libkart cannot compute envelopes from WKB (it would need
      GDAL) and returns rc ``-1`` with
      ``not implemented: gpkg.envelope calculate_if_missing (needs GDAL)``. A
      *stored* envelope containing ``NaN`` is treated as missing and always
      yields ``*out_count == 0`` (rc ``0``) regardless of ``calc``.

.. c:function:: int kart_gpkg_to_wkb(const uint8_t *g, size_t n, uint8_t **out, size_t *out_len)

   Strip the GPKG header and return the plain WKB geometry, normalised to
   little-endian byte order.

   :param g: GPKG geometry bytes.
   :param n: length of ``g``.
   :param out: out-param for a malloc'd WKB byte buffer.
   :param out_len: out-param receiving the buffer length.
   :returns:
      - ``*out`` — the plain WKB geometry, normalised to little-endian (first
        byte ``1``; big-endian input is byte-swapped). Free with
        :c:func:`kart_free`.
   :errors:
      - the GPKG header cannot be parsed, the WKB is truncated, its byte-order
        marker is invalid (``format error: Invalid WKB byte-order marker: ...``),
        or its geometry type is unsupported while byte-swapping big-endian WKB.

Misc functions
^^^^^^^^^^^^^^

.. c:function:: const char *kart_last_error(void)

   Return the current thread's most recent libkart error message.

   :returns: a pointer to a NUL-terminated UTF-8 C string owned by libkart
      (thread-local). Never ``NULL`` (empty string ``""`` if no error yet). Valid
      only until the next libkart call on the same thread; copy it if you need to
      keep it. Do **not** free it. Messages are category-prefixed (e.g.
      ``not found: ...``, ``format error: ...``, ``git error: ...``,
      ``not implemented: ...``).

.. c:function:: void kart_free(void *ptr)

   Free a buffer previously returned by libkart through a ``uint8_t **`` /
   ``char **`` out-param.

   :param ptr: the buffer pointer (the value written to ``*out``). ``NULL`` is
      allowed.
   :returns: nothing. Calls libc ``free``; ``kart_free(NULL)`` is a safe no-op.
      Use this (not your own allocator's ``free``, and never on the pointer
      returned by :c:func:`kart_last_error`).

Returned data shapes
~~~~~~~~~~~~~~~~~~~~

kart_dataset_schema_json output
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A UTF-8 JSON object with keys:

- ``path`` — string, the dataset path.
- ``type`` — string dataset type (e.g. ``"table"``).
- ``has_geometry`` — bool, ``true`` iff a geometry column id was found.
- ``primary_key`` — string column name, or ``null`` (only set when there is
  exactly one primary-key column).
- ``geom_column_name`` — string name of the geometry column, or ``null``.
- ``columns`` — array: the verbatim contents of the dataset's ``meta/schema.json``
  (the Kart column descriptors); an empty array if there is no ``schema.json``.

Each column descriptor in ``columns`` comes straight from ``schema.json`` and
includes at least ``id`` (uuid string), ``name`` (string) and ``dataType`` (e.g.
``"integer"``, ``"text"``, ``"geometry"``). The geometry column (``dataType ==
"geometry"``) additionally carries ``geometryType`` and ``geometryCRS`` (e.g.
``"EPSG:4326"``); primary-key columns carry ``primaryKeyIndex`` (integer). For
the authoritative schema and CRS definitions, see
:doc:`/pages/development/table_v3`.

kart_tile_summary_json output
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A UTF-8 JSON object built from the LFS pointer's ``key value`` text lines plus
the decoded ``ext-0-kart-encoded.*`` map. Each line is parsed: ``size`` becomes
an integer; ``ext-0-kart-encoded.<b64>`` lines are base64-decoded (altchars
``.``/``-``, padding-tolerant) then msgpack-map-decoded and merged in; the
``version`` line is dropped. Typical keys: ``oid`` (string, e.g.
``"sha256:..."``), ``size`` (integer), ``format`` (string, e.g.
``"laz-1.4/copc-1.0"``), ``pointCount`` (integer), ``nativeExtent`` (string,
comma-separated ``minx,maxx,miny,maxy,minz,maxz``), ``crs84Extent`` (WKT
``POLYGON`` string), ``sourceOid`` (string).

As noted above, ``name`` is absent from C-ABI output; supply the tile filename
yourself (the ``oid`` is the LFS content hash of the real tile data).

GPKG geometry byte format
^^^^^^^^^^^^^^^^^^^^^^^^^

The geometry bytes accepted and produced by the ``kart_gpkg_*`` functions and
:c:func:`kart_feature_geometry` use the standard
`GeoPackage binary geometry encoding <gpkg_gpb_data_blob_format_>`_ (which Kart
calls *StandardGeoPackageBinary*):

- magic ``GP`` (``0x47 0x50``)
- version byte (must be ``0``)
- flags byte
- optional ``srs_id`` + envelope
- then the WKB geometry

You do not normally parse these bytes yourself — the ``kart_gpkg_*`` functions
do. For reference, the flags byte encodes: ``0x01`` = header fields
little-endian; ``0x0E`` (``>> 1``) = envelope-contents indicator (``0`` => 0
doubles, ``1`` => 4 XY, ``2`` => 6 XYZ, ``3`` => 6 XYM, ``4`` => 8 XYZM);
``0x10`` = empty; ``0x20`` = ExtendedGeoPackageBinary. The WKB starts at offset
``8 + envelope_doubles * 8``. ExtendedGeoPackageBinary and a version ``!= 0`` are
rejected.

Envelope tuple order
^^^^^^^^^^^^^^^^^^^^

The doubles written by :c:func:`kart_gpkg_envelope` are ordered
``(minx, maxx, miny, maxy[, minz, maxz][, minm, maxm])`` — note the per-axis
``(min, max)`` pairing, **not** ``minx, miny, maxx, maxy``. ``out_count`` is
``0`` (none/empty/NaN), ``4`` (XY, or when ``only_2d`` truncates), or ``6`` (XYZ;
an 8-double XYZM stored envelope is capped to the first 6 doubles). Any ``NaN`` in
the stored envelope is treated as missing (count ``0``).

WKB type codes
^^^^^^^^^^^^^^

:c:func:`kart_gpkg_geometry_type` returns the OGR/ISO WKB integer type: base
``1`` = Point, ``2`` = LineString, ``3`` = Polygon, ``4`` = MultiPoint, ``5`` =
MultiLineString, ``6`` = MultiPolygon, ``7`` = GeometryCollection. ISO
dimensionality is encoded as ``base + 1000*Z + 2000*M`` (e.g. ``1003`` = Polygon
Z, ``2002`` = LineString M, ``3001`` = Point ZM). :c:func:`kart_gpkg_to_wkb`
always emits little-endian WKB (first byte ``1``).

Memory ownership summary
~~~~~~~~~~~~~~~~~~~~~~~~

- **Handles** (``uint64_t``): owned by the caller; free repos with
  :c:func:`kart_repo_free` and datasets with :c:func:`kart_dataset_free`.
  Datasets outlive the repo they were opened from.
- **Out-buffers** (``uint8_t **`` / ``char **``): owned by the caller; free with
  :c:func:`kart_free`. A ``NULL`` out-pointer means "absent" — nothing to free.
- **Caller-provided arrays** (``out6`` in :c:func:`kart_gpkg_envelope`): owned by
  the caller; libkart only writes into them.
- **Error strings** (:c:func:`kart_last_error`): owned by libkart, thread-local,
  valid only until the next call on that thread. Never free; copy if needed.

Usage example (Python / cffi)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following uses `cffi <https://cffi.readthedocs.io/>`_ to load the library
(from the ``LIBKART_PATH`` environment variable; see
`What libkart does and does not do`_ for where the library lives) and open a
repo, list its datasets, read a dataset's schema, then read one feature and
extract its geometry as WKB. The feature blob is read with
`pygit2 <https://www.pygit2.org/>`_, since libkart does not walk git trees (see
`Reading feature and tile blobs`_). Note how each fallible call is checked, every
out-buffer is freed, and a ``NULL`` out-pointer is treated as absent.

.. code-block:: python

   import json
   import os
   import pygit2
   from cffi import FFI

   ffi = FFI()
   ffi.cdef("""
       int kart_repo_open(const char* path, uint64_t* out_repo);
       void kart_repo_free(uint64_t repo);
       int kart_repo_list_datasets(uint64_t repo, const char* refish,
                                   uint8_t** out_json, size_t* out_len);
       int kart_dataset_open(uint64_t repo, const char* refish,
                             const char* path, uint64_t* out_ds);
       void kart_dataset_free(uint64_t ds);
       int kart_dataset_schema_json(uint64_t ds, uint8_t** out, size_t* out_len);
       int kart_feature_geometry(uint64_t ds, const uint8_t* blob, size_t blob_len,
                                 uint8_t** out, size_t* out_len);
       int kart_gpkg_to_wkb(const uint8_t* g, size_t n,
                            uint8_t** out, size_t* out_len);
       const char* kart_last_error(void);
       void kart_free(void* ptr);
   """)
   lib = ffi.dlopen(os.environ["LIBKART_PATH"])  # e.g. .../_internal/libkart.so


   def check(rc):
       # rc 0 == ok, -1 == error; kart_last_error() holds the message.
       if rc != 0:
           err = lib.kart_last_error()
           msg = ffi.string(err).decode("utf-8", "replace") if err != ffi.NULL else ""
           raise RuntimeError(msg or f"libkart call failed (rc={rc})")


   def take_buffer(out_ptr, out_len):
       # Read a libkart-malloc'd out-buffer into bytes, then free it. NULL -> None.
       ptr = out_ptr[0]
       if ptr == ffi.NULL:
           return None
       try:
           return bytes(ffi.buffer(ptr, out_len[0]))
       finally:
           lib.kart_free(ptr)


   def gpkg_to_wkb(gpkg):
       # Strip the GPKG header, returning plain little-endian WKB.
       g = ffi.from_buffer(gpkg)
       out, out_len = ffi.new("uint8_t**"), ffi.new("size_t*")
       check(lib.kart_gpkg_to_wkb(g, len(gpkg), out, out_len))
       return take_buffer(out, out_len)


   def walk_blobs(git, tree):
       # Recursively yield every leaf blob under a pygit2 tree.
       for entry in tree:
           obj = git[entry.id]
           if isinstance(obj, pygit2.Tree):
               yield from walk_blobs(git, obj)
           elif isinstance(obj, pygit2.Blob):
               yield obj


   repo_path = "/path/to/repo"

   # Open a repo (handle is a uint64_t; 0 is never valid).
   repo_out = ffi.new("uint64_t*")
   check(lib.kart_repo_open(repo_path.encode(), repo_out))
   repo = repo_out[0]
   try:
       # List datasets at a refish; returned buffer is JSON, NOT NUL-terminated.
       out, out_len = ffi.new("uint8_t**"), ffi.new("size_t*")
       check(lib.kart_repo_list_datasets(repo, b"HEAD", out, out_len))
       datasets = json.loads(take_buffer(out, out_len) or b"[]")
       ds_path = datasets[0]

       # Open one dataset and read its schema.
       ds_out = ffi.new("uint64_t*")
       check(lib.kart_dataset_open(repo, b"HEAD", ds_path.encode(), ds_out))
       ds = ds_out[0]
       try:
           out, out_len = ffi.new("uint8_t**"), ffi.new("size_t*")
           check(lib.kart_dataset_schema_json(ds, out, out_len))
           schema = json.loads(take_buffer(out, out_len) or b"{}")

           # Read one feature blob from git ourselves (libkart does not do this).
           git = pygit2.Repository(repo_path)
           tree = git.revparse_single("HEAD").peel(pygit2.Tree)
           feature_tree = tree[f"{ds_path}/.table-dataset/feature"]
           for blob in walk_blobs(git, feature_tree):
               # Decode this feature's GPKG geometry (NULL == no geometry).
               out, out_len = ffi.new("uint8_t**"), ffi.new("size_t*")
               check(lib.kart_feature_geometry(ds, blob.data, blob.size, out, out_len))
               gpkg = take_buffer(out, out_len)
               if gpkg is not None:
                   wkb = gpkg_to_wkb(gpkg)
               break  # just the first feature, for the example
       finally:
           lib.kart_dataset_free(ds)
   finally:
       lib.kart_repo_free(repo)
