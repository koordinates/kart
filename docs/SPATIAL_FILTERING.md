Spatial Filtering
-----------------

Starting with Kart 0.11.0, Kart supports spatially-filtered repositories. This is a Kart repository with an extra attached geometry describing a region that the user is interested in - any features that intersect that geometry are said to match the spatial filter, and they are shown. Conversely any features that do not intersect with it do not
match, and are hidden.

Spatial filtering has two main benefits:

- Saves time, bandwidth, and disk space when cloning a repo and creating a working copy, by only downloading matching features.
- Saves time loading the working copy into an editor and saves time for the user to focus in on their area of interest.

The spatial filter is applied in three different ways:

1. Only features that match the spatial filter are cloned during a `kart clone`.
2. Kart commands that output features only output the features that match the spatial filter.
3. The working copy is only populated with features that match the spatial filter.

All three of these ways have some additional complexity, which will be explained in the following three sections. If this seems like more detail than you will need, skip ahead to learn how to [set the spatial filter](#Setting-the-spatial-filter)

### During clone

Spatial filtering during a `kart clone` is only performed if the spatial filter is supplied up front as part of the clone command. The spatial filter can be changed any time after the clone operation is complete, but at this point the entire repository will already have been cloned and it will be too late to save on download time and bandwidth. Note that none of the cloned data is deleted when changing the spatial filter anytime after cloning. To put this another way:

1. During a clone, a spatial filter can be supplied. Only features that match this original spatial filter are cloned. The set of features cloned remains the same from this point onwards.
2. Kart commands only output the features that match the *current* spatial filter, which can be changed at any time.
3. The working copy is only populated with features that match the *current* spatial filter, which can be changed at any time.

As a result, a current limitation is that the spatial filter can be changed at any time, but only to a subset of the filter that was used during the clone. If you need to grow the spatial filter beyond what was originally cloned, the workaround currently is to start again with a fresh clone.

There are two more qualifications regarding spatial filtering during the clone operation -
- It is approximate or "best-effort" - some features may be downloaded that are not actually needed. However, since the spatial-filter is applied precisely in the other two stages (Kart command output and working copy creation) this will be hidden from you. The cloned data on disk may contain any number of features that are outside the spatial filter without any adverse effects apart from reduced efficiency.
- No filtering will occur at all if there is no spatial index on the repository which you are cloning from - instead you will the entire repository. See [indexing](#Indexing) for more details.

### During command output

The following commands all show a diff with old and new versions of features: `kart diff`, `kart show`, `kart create-patch`.
When a spatial filter is active, these commands will not show those changes to features that happen entirely outside the spatial filter. If the diff happened entirely or partially inside the spatial filter, then it will be shown. This includes all of the following:

- Changes to features that remained inside the spatial filter
- Creation or deletion of features inside the spatial filter
- Movement of features from outside the spatial filter to inside the spatial filter
- Movement of features from inside the spatial filter to outside the spatial filter

In all cases, both the new and old version of the feature will be shown, even if only one of those versions match. It is only if neither version matches that they are not shown.

### During working copy creation

Kart will only populate the working copy with those features that match the current spatial filter. If you change the spatial filter, Kart will rewrite the working copy. Of course, you can change the working copy in any way you like, and commit those changes. Take note however: other primary key values other than the ones you can see in the working copy may already be in use. Suppose a small dataset contains only four features with primary key values of `1`, `2`, `3`, `4` but only the first two match the spatial filter. When editing the working copy, nothing will prevent you from adding a third feature with primary key `3`, but when you try to commit it, Kart will warn you that it conflicts with an existing feature that is outside your working copy - if you force Kart to commit it anyway, then that feature will be overwritten.

### Specifying the spatial filter

A spatial filter specification has two parts - the CRS, and the geometry. Specifying the CRS that the spatial filter is specified in is necessary since the spatial filter applies to the entire repository, which could contain a variety of datasets using a variety of CRSs. There are two ways to specify a spatial filter, either inline or by creating a file that contains the specification

#### Inline specification

The inline specification for a spatial filter consists of the name of the CRS, followed by a semicolon, followed by a valid Polygon or Multipolygon encoded using WKT or hex-encoded WKB. It will look something like one or other of the following:

- `EPSG:4326;POLYGON((...))` (WKT)
- `EPSG:4269;01030000...` (hex-encoded WKB)

#### File containing the specification

The file should contain either the name of the CRS or the entire CRS definition in WKT, followed by a blank line, followed by a valid Polygon or Multipolygon encoded using WKT or hex-encoded WKB. For example:

```
EPSG:4326

POLYGON((...))
```

To reference a spatial filter file on your filesystem, use an @ symbol followed by the path to the file. This syntax can be used instead of the inline specification anywhere that a spatial filter specification is expected.

### Setting the spatial filter

This can be done during a clone operation (which can save you from downloading unneeded data, see # During clone for details).
- `kart clone URL --spatial-filter="EPSG:4326;POLYGON((...))"`

Or to reference a file containing the spatial filter specification:
- `kart clone URL --spatial-filter=@myspatialfilter.txt`


To change the spatial filter at any time after the clone:
- `kart checkout --spatial-filter="EPSG:4326;POLYGON((...))`
- `kart checkout --spatial-filter=@myspatialfilter.txt`

To clear the spatial filter (such that all features match the spatial filter, as happens by default):
- `kart checkout --spatial-filter=none`

The command `kart status` will tell you if a spatial filter is currently active.

### Indexing

Indexing is only useful if you are managing a repository will be cloned by others who will do so using a spatial filter. In this case, you should run the following command to generate a spatial index the repository so that when they clone it, they only receive the features that match the spatial filter. If there is no spatial index, they will instead receive every single feature. As explained above, the resulting repository will still behave as expected - Kart commands and working copy will still be limited precisely according to the spatial filter specified - but they will miss out on the saving of only downloading the required data.

To index a repository, run the following command:

`kart spatial-filter index`

As more data is added to the repository, running the same command again will index data that has not yet been indexed. Running this command on a semi-regular basis
as the repository has more data added will help ensure users get the most efficient spatially filtered clones possible, but forgetting to do so has no adverse effects apart from reduced efficiency. This could be automated by using, for instance, the [git post-receive hook](https://git-scm.com/docs/githooks#post-receive).

Indexing is performed on a best effort basis - certain features may fail to index and so these features will be cloned regardless of the spatial filter. This has no adverse effects apart from reduced efficiency and so will not be noticeable as long as these features are few.

### Current limitations

- The spatial filter can be changed at any time but it cannot be enlarged beyond the spatial filter that was originally used during `kart clone`.
- The spatial filter cannot be set to a geometry that can't be transformed into the CRS for every dataset in the repository.
- Currently indexing isn't very clever with respect to CRS changes. If the CRS for a dataset has been drastically changed at some point, then the index generated for that dataset, although accurate, can be very inefficient, resulting in inefficient clones. (If the CRS is changed but the new CRS is similar to the old one in that the features are all in approximately the same place regardless of which CRS is used to interpret them, then an efficient index will still be generated).
- 