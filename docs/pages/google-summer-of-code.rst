Google Summer of Code
=====================

Getting Started
---------------

Download Kart, follow the tutorial, and start exploring with your own
datasets. If you find any bugs, `report
them <https://github.com/koordinates/kart/issues>`__, and maybe try to
fix them? It's a good way to learn how the project works. We'd
definitely welcome suggestions to improve the help or documentation too.

Start communicating with the developers — start a
`discussion <https://github.com/koordinates/kart/discussions>`__ or
explore the `issue
tracker <https://github.com/koordinates/kart/issues>`__ to see the
current topics we've been focussing on.

Have a look through the `ideas list <#ideas-list>`__ and see whether any
of the proposed ideas interest you.

How to Apply
------------

1. Read the links and instructions given on this page. We've tried to
   give you all the information you need to be an awesome GSoC
   applicant.

2. Start a discussion and talk with your prospective mentor about what
   they expect of GSoC applicants. They can help you refine your project
   ideas. Listening to the mentors' recommendations is very important at
   this stage!

3. Usually we expect GSoC contributors to fix a bug or make an
   improvement, and have made a pull request to Kart. Your code doesn't
   have to be accepted and merged, but it does have to be visible to the
   public and it does have to be your own work.

4. Write your application (with help from your mentors!) The application
   template is available `here <#application-template>`__. All
   applications must go through `Google's application
   system <https://summerofcode.withgoogle.com/>`__; we can't accept any
   application unless it is submitted there. Make it easy for your
   mentors to give you feedback. If you're using Google Docs, enable
   comments and submit a "draft" (we can't see the "final" versions
   until applications close). If you're using a format that doesn't
   accept comments, make sure your email is on the document and don't
   forget to check for feedback!

5. Submit your application to Google before the deadline. Google does
   not extend this deadline, so it's best to be prepared early. You can
   keep editing your application up until the deadline.

      💡 Communication is probably the most important part of the
      application process. Talk to the mentors and other developers,
      listen when they give you advice, and demonstrate that you've
      understood by incorporating their feedback into what you're
      proposing. If your mentors tell you that a project idea won't work
      for them, you're probably not going to get accepted unless you
      change it.

Application Template
~~~~~~~~~~~~~~~~~~~~

An ideal application will contain 5 things:

1. A descriptive title
2. Information about you, including full contact information. Which time
   zone you're in. If you're studying, your institution, course, and
   year.
3. Link to a code contribution you have made/proposed to Kart. If you've
   made some contributions to other open source projects that you're
   proud of please link to them too.
4. Information about your proposed project. This should be fairly
   detailed and include a timeline.
5. Information about other commitments that might affect your ability to
   work during the GSoC period. (exams, classes, holidays, other jobs,
   weddings, etc.) We can work around a lot of things, but it helps to
   know in advance.

🤍 *Some of the content above was originally sourced from the fine folks
at* `Python Summer of Code <https://python-gsoc.org/>`__ *and their
documentation (CC-BY).*

--------------

Ideas List
----------

This is our current ideas list for 2022. Remember, you're welcome to
propose your own idea, but you need to `start a
discussion <https://github.com/koordinates/kart/discussions>`__ with the
mentors **before** submitting so we can all make sure it has the best
chance of being accepted. Keep checking back, this list will evolve as
we go along and we'll flesh out further details too.

Kart CLI Help Improvements
~~~~~~~~~~~~~~~~~~~~~~~~~~

-  Project Size: Medium (175h)
-  Mentors: @rcoup, @craigds
-  Skills needed: Python

Kart could do better to support CLI users as they interact with the
commands and the data in their repositories. Adding tab completion so it
works smoothly and consistently for Kart commands and their options
would make this better. Then expanding that further so datasets,
branches, tags, files, metadata, features, and other context-relevant
information is also presented where appropriate. Kart currently doesn't
have a means of building or exposing man-style documentation —
establishing such a framework the project can build upon would also fit
into this project (eg: via ``kart x --help``). This should be
cross-platform as much as possible, supporting bash/Zsh/fish/etc as well
as PowerShell on Windows.

Attachments Support
~~~~~~~~~~~~~~~~~~~

-  Project Size: Medium (175h)
-  Mentors: @olsen232
-  Skills needed: Python, Git

Kart enables version control of vector or tabular datasets, just as Git
enables version control of files and folders. If Kart also supported
version control of files and folders, then any files that are relevant
to these datasets - perhaps README files, images and thumbnails,
documentation PDFs, licenses, metadata XML - could be stored in the same
version controlled repository alongside the datasets they refer to. Kart
is already capable of storing version controlled files, using a Git
object database, but every Kart operation needs to be modified so that
it works simultaneously on tabular datasets and on files and folders
(ie, the command to commit changes should simultaneously commit tabular
changes from a database and file changes from the filesystem). And we
need a user experience that doesn't lead to datasets accidentally being
committed as attachments!

OGC Features API Support
~~~~~~~~~~~~~~~~~~~~~~~~

-  Project Size: Medium (175h) or Large (350h)
-  Mentors: @craigds, @rcoup
-  Skills needed: Python, HTTP APIs

`OGC API - Features <https://ogcapi.ogc.org/features/>`__ (OAPIF) is the
successor to `WFS <https://en.wikipedia.org/wiki/Web_Feature_Service>`__
as the key open standard for people to create, modify and query vector
spatial data on the web. We should be able to serve datasets via OAPIF
directly from Kart repositories as well as potentially integrate Kart
repositories into larger OAPIF server infrastructures like
`pygeoapi <https://pygeoapi.io/>`__ or
`GeoServer <https://geoserver.org>`__. There are several components for
OAPIF support as defined by the "Parts" of the standard:
`1:Core <https://docs.ogc.org/DRAFTS/17-069r4.html>`__; `2:Coordinate
Reference Systems <https://docs.ogc.org/DRAFTS/18-058r1.html>`__;
`3:Filtering <https://docs.ogc.org/DRAFTS/19-079r1.html>`__; and
`4:Create, Replace, Update, and
Delete <https://docs.ogc.org/DRAFTS/20-002.html>`__. So there are
opportunities for exploring this project from some different angles. The
project should aim to add robust support for serving read-only OAPIF
Core responses directly from Kart repositories, then deeper support for
one or more of:

1. developing plugins for pygeoapi (Python), GeoServer (Java), or `other
   OAPIF
   servers <https://github.com/opengeospatial/ogcapi-features/blob/master/implementations/servers/README.md>`__
2. additional Parts of the standard. eg: CRS; filtering, optionally with
   indexing support; writes
3. support for incorporating & exposing versioning history

Multi-version Spatial Indexing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-  Project Size: Medium (175h) or Large (350h)
-  Mentors: @craigds, @rcoup
-  Skills needed: Python, C/C++

Being able to serve and consume data directly from Kart repositories is
a key goal of the project. To that end, being able to very quickly query
data spatially is important. Typically an
`R-tree <https://en.wikipedia.org/wiki/R-tree>`__ or one of the variants
is used to do this, but this applies to a static dataset — in Kart's
case we have versions, and each commit should not require a new index to
be queried efficiently. One option for implementing this is to use a
multi-version R-tree index (eg: MVR-tree/PPR-tree or others) which can
reuse the same index for different commits. The project would aim to
select and integrate an appropriate spatial index into Kart (eg:
building on `libspatialindex <https://libspatialindex.org>`__ or other
implementations), including support for updating the index robustly,
deal with coordinate reference systems, and implement stable and high
performance querying. The spatial indexing will be the basis of other
future features of Kart.

Simple Repository Hosting
~~~~~~~~~~~~~~~~~~~~~~~~~

-  Project Size: Medium (175h)
-  Mentors: @rcoup
-  Skills needed: Python, Docker, Git, Linux administration, Writing

Make it easy for people to host Kart repositories on their own
infrastructure by establishing best-practises, tools, and guidelines.
Kart datasets can be large, so configuring Git well to host Kart
repositories efficiently is important. In addition, Kart supports
server-side spatial filtered clones, but this requires indexing when
pushes are received. And as repositories are updated, maintenance needs
to be run on them to keep them working efficiently. The project would be
to make all these pieces work well together, and designing and coding a
Docker setup for a
`Gitolite <https://gitolite.com/gitolite/index.html>`__ (or similar)
configuration with Kart which can be used to host Kart repositories via
SSH or for users to build further on for their own needs.
