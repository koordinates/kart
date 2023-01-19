Git Large File Storage
----------------------

Git recommends against committing large files to its object database.
The solution, for users who want version controlled large files, is to
install an extension called `Git Large File Storage <git_lfs_>`_ (Git LFS).
This extension is tied into Git using Git's clean/smude filter mechanism,
and provides another storage location better suited to large files,
separate from the standard Object Database (ODB).

When Git checks out a certain commit by reading from the ODB, it may
encounter LFS "pointer files". These small files contain a hash of the
large LFS file, and signal to Git that it should not write the
pointer file to the working copy verbatim, but instead should substitute
in the full LFS file. The hash of the LFS file is sufficient information
to find it, either in the local LFS cache that Git maintains, or by
fetching it from a remote.

The main differences between the ODB and the LFS are as follows:

- The ODB remains responsible for names, paths, folder hierarchy, and
  versioning of all files. LFS only stores the contents of large files -
  not where and when they belong in the file hierarchy.

- Typically, Git stores all past revisions of every file, or at least
  all past revisions up to a certain point, and these are fetched
  during the initial clone. LFS files are fetched more "lazily", typically
  only versions of LFS files that is needed for the current check out
  are fetched. (However, these may remain in the cache for a time).

- The ODB is present in every Git repository, but for LFS to work, the user
  must install the Git LFS extension.

- The remote Git server from whence Git objects are fetched is separated
  somewhat from the remote LFS server. However, when properly configured,
  the necessary data will be seamlessly fetched from both without the user
  even being aware of it. For example, Git LFS servers are configured for
  GitHub, Bitbucket Cloud, and koordinates.com, so that pushing commits
  to these servers that reference LFS files should "just work".

Git LFS in Kart
~~~~~~~~~~~~~~~

Kart 0.12 adds support for :doc:`Point Cloud datasets </pages/development/pointcloud_v1>`.
Kart treats the tiles for Point Cloud datasets as LFS files. Therefore, Kart 0.12
onwards comes with Git LFS included. Kart's use of Git LFS differs from Git's in
the following manner:

- The necessary software and the hooking into the repository is handled by Kart -
  no need to run any extra commands to set up Kart repositories as LFS repositories.

- In a Git repository the user must configure which types of files to treat as LFS files.
  In a Kart repository, Kart decides which types of files to treat as LFS files.

- Kart stores some extra information in the pointer files that Git LFS normally does not -
  most importantly, the geographical extents of the tiles.

- The standard Git LFS commands do not necessarily work properly on Kart repositories.
  A Kart repository has a very different working copy to Git, so any commands which
  reference the working copy are not likely to work as expected. It is recommended instead
  to use the ``kart lfs+`` commands - see :ref:`Working with LFS files`.

Disk Usage
~~~~~~~~~~

Files currently checked out in the file system working copy, or which have previously
been checked out in the file system working copy, may be present in the LFS cache
that is in the hidden ``.kart`` folder in the Kart repository. It is possible
to garbage collect the cached files which are currently not being used by running

``kart lfs+ gc``

On macOS and Linux, Kart attempts to use `copy-on-write <copy_on_write_>`_ to minimise
the total disk-usage. This means that Kart will avoid copying a file from the LFS cache
to your working copy, and instead will create an identical file in the working copy that
references the same data in your file-system. Only if you then edit this file will your
operating system do a last-minute copy so that your edit only affects the copy of the
file, and the original file in the LFS cache remains unchanged.

Note that information provided by your operating system about how much free space remains
on your disk is generally accurate, but information provided by your operating system
about how much space a Kart repository takes up may be out by a factor of two - the OS
might double-count files that are referred to twice inside the Kart repository, even
though they both refer to the same physical location on your hard disk.

On Windows, copy-on-write is not supported. Currently, Kart simply does a regular copy,
which means all LFS files that are checked out in the working copy are also present
in the LFS cache, which is inefficient use of disk space. This is tracked as `#772 <kart_github_issue_772_>`_).
