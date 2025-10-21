# Why?

This is identical to upstream gdal port, except for:

1. we set `-DGDAL_AUTOLOAD_PLUGINS=OFF` to prevent unwanted plugins being loaded when run from within QGIS.
2. We don't remove `gdal-config` during builds. This helps with building Python bindings.
