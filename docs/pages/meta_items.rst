Meta Items
==========

Here are some example meta items.

::

   {
     "<layer>": {
       "title": "Example title",
       "description": "Source: Example source\nData last updated: 12 Jul 2012\nDownloaded from http://example.com/ 12 Jul 2012",
       "schema.json": [
         {
           "id": "b6828caa-56c4-a804-b8ae-90ffb11ca006",
           "name": "fid",
           "dataType": "integer",
           "primaryKeyIndex": 0,
           "size": 64
         },
         {
           "id": "0e81e0eb-9ad9-614c-5976-5c95d05eb1b6",
           "name": "geom",
           "dataType": "geometry",
           "primaryKeyIndex": null,
           "geometryType": "POINT",
           "geometryCRS": "EPSG:4326"
         },
         {
           "id": "3073e934-4245-73f6-9494-e2712becb644",
           "name": "crt_date",
           "dataType": "float",
           "primaryKeyIndex": null,
           "size": 64
         },
         {
           "id": "d432bc71-1112-4a9c-5e98-f11f40a0f98f",
           "name": "mod_date",
           "dataType": "float",
           "primaryKeyIndex": null,
           "size": 64
         }
       ],
       "crs/EPSG:4326.wkt": "GEOGCS[\"WGS 84\",DATUM[\"WGS_1984\",SPHEROID[\"WGS 84\",6378137,298.257223563,AUTHORITY[\"EPSG\",\"7030\"]],AUTHORITY[\"EPSG\",\"6326\"]],PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],UNIT[\"degree\",0.0174532925199433,AUTHORITY[\"EPSG\",\"9122\"]],AXIS[\"Latitude\",NORTH],AXIS[\"Longitude\",EAST],AUTHORITY[\"EPSG\",\"4326\"]]"
     }
   }

The possible meta items are named: - ``title`` - ``description`` -
``schema.json`` - ``crs/<some-identifier>.wkt`` - ``metadata.xml``

For a more complete specification, see Kart's
:ref:`Datasets V3`
documentation.
