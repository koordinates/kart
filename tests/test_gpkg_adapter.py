import html

from kart.sqlalchemy.adapter.gpkg import KartAdapter_GPKG
from kart.schema import Schema

V2_SCHEMA_DATA = [
    {
        "id": "f80fcc97-31b5-40c4-9cd9-b8c9773e7664",
        "name": "OBJECTID",
        "dataType": "integer",
        "size": 32,
        "primaryKeyIndex": 0,
    },
    {
        "id": "2df3a433-2e95-4ea9-b5c3-2b454f5212d3",
        "name": "GEOMETRY",
        "dataType": "geometry",
        "geometryType": "GEOMETRY",
        "geometryCRS": "EPSG:2193",
    },
    {
        "id": "aa33d0d8-589d-4389-b2d7-b117358b4320",
        "name": "Ward",
        "dataType": "text",
    },
    {
        "id": "9abe0a9f-6668-412a-83af-c3d6e44be647",
        "name": "Shape_Leng",
        "dataType": "float",
        "size": 64,
    },
    {
        "id": "07f1f8b0-42f5-4805-b884-cb25d015a06f",
        "name": "Shape_Area",
        "dataType": "float",
        "size": 64,
    },
]


class FakeDataset:
    pass


def test_adapt_schema():
    schema = Schema.from_column_dicts(V2_SCHEMA_DATA)
    dataset = FakeDataset()
    dataset.schema = schema
    dataset.has_geometry = schema.has_geometry
    dataset.tree = dataset
    dataset.name = "test_dataset"

    sqlite_table_info = KartAdapter_GPKG.generate_sqlite_table_info(dataset)
    assert sqlite_table_info == [
        {
            "cid": 0,
            "name": "OBJECTID",
            "pk": 1,
            "type": "INTEGER",
            "notnull": 1,
            "dflt_value": None,
        },
        {
            "cid": 1,
            "name": "GEOMETRY",
            "pk": 0,
            "type": "GEOMETRY",
            "notnull": 0,
            "dflt_value": None,
        },
        {
            "cid": 2,
            "name": "Ward",
            "pk": 0,
            "type": "TEXT",
            "notnull": 0,
            "dflt_value": None,
        },
        {
            "cid": 3,
            "name": "Shape_Leng",
            "pk": 0,
            "type": "REAL",
            "notnull": 0,
            "dflt_value": None,
        },
        {
            "cid": 4,
            "name": "Shape_Area",
            "pk": 0,
            "type": "REAL",
            "notnull": 0,
            "dflt_value": None,
        },
    ]


SAMPLE_XML = """<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis version=\"3.20.3-Odense\">
  <identifier></identifier>
  <parentidentifier></parentidentifier>
  <language></language>
  <type></type>
  <title></title>
  <abstract></abstract>
  <contact>
    <name></name>
    <organization></organization>
    <position></position>
    <voice></voice>
    <fax></fax>
    <email></email>
    <role></role>
  </contact>
  <links/>
  <fees></fees>
  <encoding></encoding>
  <crs>
    <spatialrefsys>
      <wkt></wkt>
      <proj4></proj4>
      <srsid>0</srsid>
      <srid>0</srid>
      <authid></authid>
      <description></description>
      <projectionacronym></projectionacronym>
      <ellipsoidacronym></ellipsoidacronym>
      <geographicflag>false</geographicflag>
    </spatialrefsys>
  </crs>
  <extent>
    <spatial minx=\"0\" miny=\"0\" dimensions=\"2\" maxz=\"0\" crs=\"\" maxy=\"0\" minz=\"0\" maxx=\"0\"/>
    <temporal>
      <period>
        <start></start>
        <end></end>
      </period>
    </temporal>
  </extent>
    </qgis>
"""

WRAPPED_SAMPLE_XML = f"""<GDALMultiDomainMetadata>
  <Metadata>
    <MDI key="GPKG_METADATA_ITEM_1">
{html.escape(SAMPLE_XML)}
    </MDI>
  </Metadata>
    </GDALMultiDomainMetadata>
"""

REPEATED_WRAPPED_SAMPLE_XML = f"""<GDALMultiDomainMetadata>
  <Metadata>
    <MDI key="GPKG_METADATA_ITEM_1">
{html.escape(SAMPLE_XML)}
    </MDI>
    <MDI key="GPKG_METADATA_ITEM_2">
{html.escape(SAMPLE_XML)}
    </MDI>
  </Metadata>
    </GDALMultiDomainMetadata>
"""


def test_find_sole_useful_xml():
    find_sole_useful_xml = KartAdapter_GPKG.find_sole_useful_xml
    # We can find the sole useful XML if the other XML is just the useful XML wrapped in a GDALMultiDomainMetadata.
    assert find_sole_useful_xml([SAMPLE_XML, WRAPPED_SAMPLE_XML]) == SAMPLE_XML
    assert find_sole_useful_xml([WRAPPED_SAMPLE_XML, SAMPLE_XML]) == SAMPLE_XML

    # We give up as soon as things get more complicated
    assert find_sole_useful_xml([SAMPLE_XML, REPEATED_WRAPPED_SAMPLE_XML]) is None
    assert (
        find_sole_useful_xml([SAMPLE_XML, WRAPPED_SAMPLE_XML, WRAPPED_SAMPLE_XML])
        is None
    )
