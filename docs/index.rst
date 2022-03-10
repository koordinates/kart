.. image:: /_static/kart-github.png
    :alt: Kart logo
    :align: center
    :width: 400px

Kart
====

.. image:: https://github.com/koordinates/kart/workflows/Build/badge.svg?event=push
    :alt: build status
.. image:: https://readthedocs.org/projects/kart/badge/?version=latest&style=flat
    :alt: docs status

Welcome to the `Kart <kart_website_>`_ documentation. Kart
provides distributed version-control for geospatial and tabular data.

Why Kart
--------

-  **Built on Git, works like Git** - uses standard Git repositories and
   Git-like CLI commands. If you know Git, you'll feel right at home
   with Kart.
-  **Your choice of format** - supports Microsoft SQL Server,
   PostgreSQL/PostGIS, MySQL and GeoPackage, with more coming soon.
-  **Synchronize data** - accurately synchronize datasets between
   systems in seconds. Kart moves and applies a minimal compressed set
   of changes.
-  Interact directly from within `QGIS <qgis_>`_ with the `Kart Plugin <kart_plugin_>`_.
-  `And much more... <kart_website_>`_

Project Status
--------------

Kart is under rapid development and some APIs and data structures are
subject to change. While Kart has undergone considerable testing there
is potential for data corrupting bugs. Please use with caution.

Reporting Bugs & Feature Suggestions
------------------------------------

Please report bugs and feature suggestions to the Kart issue tracker
`here <kart_github_issues_>`_. Please include any relevant system information (e.g.operating system)
and Kart version (``kart --version``), and as much information about the
issue as possible. Screenshots, debugging outputs and detailed
explanations help us fix issues promptly.

Documentation
-------------

.. toctree::
   :maxdepth: 2

   pages/quick_guide
   pages/basic_usage_tutorial
   pages/command_reference
   pages/upgrading
   pages/wc_formats
   pages/development
   pages/meta_items
