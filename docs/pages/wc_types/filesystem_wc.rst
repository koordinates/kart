File-system Working Copy
------------------------

Inside the Kart repository folder, Kart maintains a file-system working copy where
you can see and edit all the files you have checked out. This is the simplest type
of working copy and will be familiar to many users from using Git or similar.
However, note the following two current limitations to the scope of this working copy type:

- Kart support for "attached files" is incomplete, so the only files which will
  be checked out into the working copy are the contents of datasets.
- The only dataset type which currently uses the file-system working copy is
  the :doc:`point cloud dataset </pages/development/pointcloud_v1>`

For these reasons, the file-system working copy is not populated in every Kart
repository, only those which contain point cloud datasets, and the only thing
in the working copy will be point cloud tiles.

Disk usage
~~~~~~~~~~

Files currently checked out in the file system working copy, or which have previously
been checked out in the file system working copy, may be present in the "LFS cache"
that is in the hidden ``.kart`` folder in the Kart repository. It is possible
to garbage collect the cached files which are currently not being used by running

``kart lfs+ gc``

For more information on LFS files, see the section on :doc:`Git LFS </pages/git_lfs>`.
