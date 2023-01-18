Spatial Filtering
-----------------

Starting with Kart 0.11.0, Kart supports spatially-filtered
repositories. This is a Kart repository with an extra attached geometry
describing a region that the user is interested in - any features that
intersect that geometry are said to match the spatial filter, and they
are shown in the working copy. Conversely any features that do not
intersect with it do not match, and are hidden from the working copy.

When users are dealing with large (state/national/global) datasets with
a smaller area of interest, using spatial filtering provides two main
benefits:

-  Saves time, bandwidth, and disk space when cloning a repo and
   creating a working copy, by only downloading matching features.
-  Saves time loading the working copy into editors, minimising the
   amount of data applications need to deal with.

When working with small datasets the additional overhead of applying a
filter will likely outweigh any performance gains, particularly for
fetches over a network.

The spatial filter is applied in three different ways:

1. Only features that match the spatial filter are cloned during a
   ``kart clone`` and fetched via ``kart fetch``.
2. Kart commands that output features only output the features that
   match the spatial filter.
3. The working copy is only populated with features that match the
   spatial filter.

Setting the spatial filter
~~~~~~~~~~~~~~~~~~~~~~~~~~

Various commands support the option ``--spatial-filter`` followed by a
spatial filter specification (see the next section).

This can be supplied during a clone operation (which can save you from
downloading unneeded data). -
``kart clone URL --spatial-filter="EPSG:4326;POLYGON((...))"``

Or to reference a file containing the spatial filter specification: -
``kart clone URL --spatial-filter=@myspatialfilter.txt``

To change the spatial filter at any time after the clone: -
``kart checkout --spatial-filter="EPSG:4326;POLYGON((...))`` -
``kart checkout --spatial-filter=@myspatialfilter.txt``

To clear the spatial filter (such that all features match the spatial
filter, as happens by default): -
``kart checkout --spatial-filter=none``

The command ``kart status`` will tell you if a spatial filter is
currently active.

Specifying the spatial filter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A spatial filter specification has two parts - the CRS, and the
geometry. Specifying the CRS that the spatial filter is specified in is
necessary since the spatial filter applies to the entire repository,
which could contain a variety of datasets using a variety of CRSs. There
are two ways to specify a spatial filter, either inline or by creating a
file that contains the specification

Inline specification
^^^^^^^^^^^^^^^^^^^^

The inline specification for a spatial filter consists of the name of
the CRS, followed by a semicolon, followed by a valid Polygon or
Multipolygon encoded using WKT or hex-encoded WKB. It will look
something like one or other of the following:

-  ``EPSG:4326;POLYGON((...))`` (WKT)
-  ``EPSG:4269;01030000...`` (hex-encoded WKB)

File containing the specification
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The file should contain either the name of the CRS or the entire CRS
definition in WKT, followed by a blank line, followed by a valid Polygon
or Multipolygon encoded using WKT or hex-encoded WKB. For example:

::

   EPSG:4326

   POLYGON((...))

To reference a spatial filter file on your filesystem, use an @ symbol
followed by the path to the file. This syntax can be used instead of the
inline specification anywhere that a spatial filter specification is
expected.

Current limitations
~~~~~~~~~~~~~~~~~~~

-  Spatial filtering may not save much bandwidth or disk space in
   repositories where each individual feature takes very little room on
   disk. See
   `Appendix <#not-much-disk-space-is-saved-when-features-are-small-on-disk>`__
-  Repeatedly changing the spatial filter is not guaranteed to be more
   efficient than not using spatial filters at all. See
   `Appendix <#repeatedly-changing-the-spatial-filter-is-inefficient>`__
-  If the repository you are cloning from has not been spatially
   indexed, all features must be downloaded before the filter can be
   applied. See `Indexing <#indexing>`__
-  The spatial filter cannot be set to a geometry that can't be
   transformed into the CRS for every dataset in the repository.
-  Currently indexing isn't very clever with respect to certain CRS
   changes. If the CRS for a dataset has been drastically changed at
   some point, then the index generated for that dataset, although
   accurate, can be very inefficient, resulting in inefficient clones.
   (If the CRS is changed but the new CRS is similar to the old one in
   that the features are all in approximately the same place regardless
   of which CRS is used to interpret them, then an efficient index will
   still be generated).
   `#538 <kart_github_issue_538_>`_
-  Spatial filtered cloning is currently only supported on the
   server-side on the MacOS and Linux versions of Kart. It is currently
   not supported when the server is running Windows.
   `#539 <kart_github_issue_539_>`_

Effects of setting the spatial filter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

During clone / fetch
^^^^^^^^^^^^^^^^^^^^

Spatial filtering during a ``kart clone`` is only performed if the
spatial filter is supplied up front as part of the clone command. The
spatial filter can be changed any time after the clone operation is
complete, but at this point the entire repository will already have been
cloned and it will be too late to save on download time and bandwidth.
Note that none of the cloned data is deleted when changing the spatial
filter anytime after cloning. To put this another way:

1. During a clone, a spatial filter can be supplied. Only features that
   match this original spatial filter are fetched. The spatial filter
   applied during fetching remains the same from this point onwards.
2. Kart commands only output the features that match the *current*
   spatial filter, which can be changed at any time.
3. The working copy is only populated with features that match the
   *current* spatial filter, which can be changed at any time.

As a result, a current limitation is that the spatial filter can be
changed at any time, but only to a subset of the filter that was used
during the clone. If you need to grow the spatial filter beyond what was
originally cloned, the workaround currently is to start again with a
fresh clone.

There are two more qualifications regarding spatial filtering during
clone and fetch operations - - All features that are needed will be
downloaded, but some features may be downloaded that are not actually
needed. Since the spatial-filter is applied precisely in the other two
stages (Kart command output and working copy creation) any extra
features will be hidden from you. The cloned data on disk may contain
any number of features that are outside the spatial filter without any
adverse effects. - A spatial-filtered clone is not possible if no
spatial index has been generated at the remote you are cloning from (see
`Indexing <#indexing>`__ for more details). In this case your only
option is to clone the entire repository. Add the flag
``--spatial-filter-after-clone`` to the ``kart clone`` command to clone
the entire repository and then apply the specified spatial filter
immediately afterwards (before the working copy is created).

During command output
^^^^^^^^^^^^^^^^^^^^^

The following commands all show a diff with old and new versions of
features: ``kart diff``, ``kart show``, ``kart create-patch``. When a
spatial filter is active, these commands will not show those changes to
features that happen entirely outside the spatial filter. If the diff
happened entirely or partially inside the spatial filter, then it will
be shown. This includes all of the following:

-  Changes to features that remained inside the spatial filter
-  Creation or deletion of features inside the spatial filter
-  Movement of features from outside the spatial filter to inside the
   spatial filter
-  Movement of features from inside the spatial filter to outside the
   spatial filter

In all cases, both the new and old version of the feature will be shown,
even if only one of those versions match. It is only if neither version
matches that they are not shown.

During working copy creation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Kart will only populate the working copy with those features that match
the current spatial filter. If you change the spatial filter, Kart will
rewrite the working copy. Of course, you can change the working copy in
any way you like, and commit those changes. Take note however: other
primary key values other than the ones you can see in the working copy
may already be in use. Suppose a small dataset contains only four
features with primary key values of ``1``, ``2``, ``3``, ``4`` but only
the first two match the spatial filter. When editing the working copy,
nothing will prevent you from adding a third feature with primary key
``3``, but when you try to commit it, Kart will warn you that it
conflicts with an existing feature that is outside your working copy -
if you force Kart to commit it anyway, then that feature will be
overwritten.

Kart will warn about these conflicts when running ``kart status`` or
``kart diff``. They are called "spatial filter conflicts". Kart attempts to
help you avoid them by setting up the working copy so that the next
primary key in the sequence that is chosen by default will not conflict
with any existing features. If you do accidentally create spatial filter
conflicts, the appropriate fix is to reassign the conflicting features
new primary key values that are not used elsewhere.

Indexing
~~~~~~~~

Indexing is only useful if you are managing a repository will be cloned
by others who will do so using a spatial filter. In this case, you
should run the following command to generate a spatial index the
repository so that when they clone it, they only receive the features
that match the spatial filter. If there is no spatial index, they will
instead receive every single feature. As explained above, the resulting
repository will still behave as expected - Kart commands and working
copy will still be limited precisely according to the spatial filter
specified - but they will miss out on the saving of only downloading the
required data.

To index a repository, run the following command:

``kart spatial-filter index``

As more data is added to the repository, running the same command again
will index data that has not yet been indexed. Running this command on a
semi-regular basis as the repository has more data added will help
ensure users get the most efficient spatially filtered clones possible,
but forgetting to do so has no adverse effects apart from reduced
efficiency. This could be automated by using, for instance, the `git
post-receive hook <git_post_recieve_hook_>`_.

Indexing is performed on a best effort basis - certain features may fail
to index due to geometry or CRS issues and so these features will always
be cloned regardless of any spatial filter. This has no adverse effects
apart from reduced efficiency and so will not be noticeable as long as
these features aren't numerous.

Indexing a repository automatically enables the Git config setting
``uploadpack.allowAnySHA1InWant``. This is necessary to allow clients
who have made a spatial-filtered clone to separately fetch individual
features that they are missing, for the case that a particular operation
requires a particular feature that hasn't yet been fetched since it is
outside of the spatial filter.

Implementation
~~~~~~~~~~~~~~

.. _during-clone-fetch-1:

During clone / fetch
^^^^^^^^^^^^^^^^^^^^

Filters are sent to the server as envelopes in ``EPSG:4326``. If the
server is maintaining a spatial index - a list of envelopes, one per
feature, also in ``EPSG:4326`` - then the server will use this data to
skip the features where the envelopes don't overlap at all. This is
conservative - sometimes a feature will not intersect the spatial
filter, but it will be fetched anyway since their envelopes overlap. The
spatial index is stored SQLite database in a file in the Kart repository
internals named ``feature_envelopes.db``. It also stores information
about which commits have been indexed, which is what allows the index
command to be rerun at any time without it restarting from scratch.

Since a Kart repository is still basically a type of Git repository, the
standard Git mechanisms such as the ``git-upload-pack`` command are
still used for cloning and fetching. However, Kart maintains a custom
build of Git with some slight changes that allow for spatial filtered
clones. Firstly, the
`list-objects-filter <git_list_object_filter_>`_
specification is extended such that Git accepts "extension" filters -
extra filters that have names starting with "extension" and that may or
may not be compiled into a particular Git build. Secondly, a spatial
filter extension is introduced that skips Kart features that are outside
a specified envelope. The resulting git clone command is as follows:

``git clone URL --filter=extension:spatial=W,S,E,N``

where ``W``, ``S``, ``E`` and ``N`` are the extent of the envelope in
degrees longitude and latitude. The following constraints must hold
true: ``S <= N``\ and ``W <= E`` (unless the envelope crosses the
antimeridian, in which case ``E < W``). All longitudes must be in the
range ``-180 <= X <= 180`` and latitudes in the range
``-90 <= Y <= 90``.

The custom build of git which supports filter extensions is found on
GitHub at
`koordinates/git <kx_latest_>`_,
and the spatial filter extension is part of the `Kart
repository <spatial_filter_extenstions_>`_.

There is also a custom build of git for Windows
`here <custom_git_for_win_>`_
which supports filter extensions generally but doesn't include the
spatial filter extension specifically. This is sufficient so that
spatial filtered clones can be made with a Windows client, but they
cannot currently be made using a Windows server.

For more details, see :ref:`Building Git for Kart`

During command output / working copy creation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once the data is on the client, the index is no longer used. Instead,
Kart applies the spatial filter precisely to each dataset in turn by
transforming the spatial filter geometry to the dataset's CRS, and
outputting only those features that intersect with the resulting
geometry.

Kart also needs to skip over any features that have not fetched - since
they are not present locally, Kart doesn't know exactly what those
features are, but can infer that they must be features somewhere outside
the spatial filter, or they would have been fetched. Kart is only
willing to skip over missing features in this way if a spatial filter
was active during the clone operation, and the missing features are in
"promisor" packfiles, which are packfiles which can have missing objects
- partial clones result in these types of packfiles. Standard packfiles
(non-promisor) by contrast are guaranteed to not have any missing
objects, so Kart will abort immediately if it encounters a missing
object in such a packfile - since the guarantee has been unexpectedly
broken, the repository must be corrupt in some way.

The third party libraries that Kart uses for reading Git repositories -
pygit2 and libgit2 - currently don't have full support for partial
clones, so they don't have a way of separating objects that are
missing-but-promised (as in promisor packfiles) and objects that are
unexpectedly missing (that is, corrupt). Kart maintains a fork of each
project which has this functionality added, but which has not yet been
merged upstream. These are found here:

-  `Libgit2
   fork <libgit2_>`_
-  `Pygit2
   fork <pygit2_>`_

--------------

Appendix - More on Limitations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Not much disk space is saved when features are small on disk
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

(Previously tracked as
`#557 <kart_github_issue_557_>`_)

When spatial filtering is applied during a clone, some blobs are not
sent, but at this point, all tree objects are sent. (These "tree"
objects group the features into a hierarchy that is not visible to the
user, but which gives the repository a git-compatible structure).

When features are very small in terms of bytes on disk (ie, more
commonly for POINT geometries), then the feature blobs may be much
smaller than the tree structure, and a spatially filtered clone may not
provide much benefit at all in terms of bandwidth or disk space saved.

In this case, you might opt to clone without a spatial filter at all
since it is not benefitting you, or you might opt to clone with
``--spatial-filter-after-clone`` - this flag means that the spatial
filter is only applied locally, which means you can change it at any
time without having to refetch any missing features.

Repeatedly changing the spatial filter is inefficient
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

(Previously tracked as
`#558 <kart_github_issue_558_>`_)

Changing the spatial filter "locally" is relatively efficient - in this
scenario you have already fetched the necessary data, and Kart is just
changing your view of the data and repopulating the working copy with
only the data that matches your spatial filter.

These local-only changes happen if you cloned the entire repository
originally - in this case you can set any spatial filter you want from
that point on and it will always be a local-only change - or, if you
originally cloned with a large spatial filter, and you are now changing
the spatial filter to be a smaller spatial filter that is a subset of
it.

Changing your spatial filter to a new spatial filter that isn't a subset
of what was originally fetched, causes the fetching of data to begin
again from scratch - it is no more efficient in terms of bandwidth than
if you had cloned a brand new repository while specifying the new
spatial filter. Currently no attempt is made to skip the resending of
features that the client already has (this is possible, but technically
difficult and not yet implemented).

For this reason, if you intend to change the spatial filter often, it is
best to initially clone using a spatial filter that is a superset of the
various spatial filters you intend to use (for example, if you intend to
switch between using different cities in your state as spatial filters,
it is probably best to clone the entire state initially). Depending on
your use case, it may be simplest to clone the entire repository.
