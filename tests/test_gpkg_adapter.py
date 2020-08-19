from sno.gpkg_adapter import generate_sqlite_table_info
from sno.schema import Schema

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
        "primaryKeyIndex": None,
    },
    {
        "id": "aa33d0d8-589d-4389-b2d7-b117358b4320",
        "name": "Ward",
        "dataType": "text",
        "primaryKeyIndex": None,
    },
    {
        "id": "9abe0a9f-6668-412a-83af-c3d6e44be647",
        "name": "Shape_Leng",
        "dataType": "float",
        "size": 64,
        "primaryKeyIndex": None,
    },
    {
        "id": "07f1f8b0-42f5-4805-b884-cb25d015a06f",
        "name": "Shape_Area",
        "dataType": "float",
        "size": 64,
        "primaryKeyIndex": None,
    },
]


class FakeDataset:
    pass


def test_adapt_schema():
    schema = Schema.from_column_dicts(V2_SCHEMA_DATA)
    dataset = FakeDataset()
    dataset.schema = schema
    dataset.tree = dataset
    dataset.name = "test_dataset"

    sqlite_table_info = generate_sqlite_table_info(dataset)
    assert sqlite_table_info == [
        {
            'cid': 0,
            'name': 'OBJECTID',
            'pk': 1,
            'type': 'INTEGER',
            'notnull': 0,
            'dflt_value': None,
        },
        {
            'cid': 1,
            'name': 'GEOMETRY',
            'pk': 0,
            'type': 'GEOMETRY',
            'notnull': 0,
            'dflt_value': None,
        },
        {
            'cid': 2,
            'name': 'Ward',
            'pk': 0,
            'type': 'TEXT',
            'notnull': 0,
            'dflt_value': None,
        },
        {
            'cid': 3,
            'name': 'Shape_Leng',
            'pk': 0,
            'type': 'REAL',
            'notnull': 0,
            'dflt_value': None,
        },
        {
            'cid': 4,
            'name': 'Shape_Area',
            'pk': 0,
            'type': 'REAL',
            'notnull': 0,
            'dflt_value': None,
        },
    ]
