Annotations
-----------

Kart can calculate certain metadata about commits (and actually about arbitrary diffs) and store it for later use.

This feature is probably mostly useful for apps needing to present commit metadata to users in a timely fashion.

At present only one type of annotation is available: feature counts.

##### Finding feature counts for a commit

`kart diff` supports the `--only-feature-count` option, which can produce feature count estimates for an arbitrary diff with a range of different accuracies.

```
$ kart diff HEAD^..HEAD --only-feature-count=good
landonline_setup:
    151552 features changed
```

To get an exact feature count, use `--only-feature-count=exact`, which looks at all of the features.

Other values are `veryfast`, `fast`, `medium`, and `good`, all of which sample a proportion of the layer to determine an approximate feature count.

##### Annotation storage

All annotations are stored in a SQLite database, `annotations.db`, in the repo directory (`.kart/annotations.db`)

The first time a given annotation is requested, it will be calculated and stored in the database. If the database doesn't exist, it will be created the first time it is used.

Subsequent requests for the same annotation will simply fetch it from the database, rather than re-calculate it.

If the database is unwritable for some reason, then annotations are not stored.

##### Bulk populating annotations

The `kart build-annotations` command populates annotations in bulk. It is useful as a post-commit, post-receive or post-merge hook for services handling Kart repos.

It takes a list of commit IDs from stdin, and builds `exact` feature counts for the diffs those commits represent.

Alternatively, provide `--all-reachable` to build annotations for all commits that are reachable from any local ref, rather than providing anything on stdin. Nothing is calculated for annotations that are already in the database.

If the database is unwritable for some reason, this command will error (exit code 20)
