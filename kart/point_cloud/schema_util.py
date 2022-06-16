# Utility functions for dealing with Point Cloud schemas.

PDRF8_SCHEMA = [
    {"name": "X", "dataType": "float", "size": 64},
    {"name": "Y", "dataType": "float", "size": 64},
    {"name": "Z", "dataType": "float", "size": 64},
    {"name": "Intensity", "dataType": "integer", "size": 16},
    {"name": "ReturnNumber", "dataType": "integer", "size": 8},
    {"name": "NumberOfReturns", "dataType": "integer", "size": 8},
    {"name": "ScanDirectionFlag", "dataType": "integer", "size": 8},
    {"name": "EdgeOfFlightLine", "dataType": "integer", "size": 8},
    {"name": "Classification", "dataType": "integer", "size": 8},
    {"name": "ScanAngleRank", "dataType": "float", "size": 32},
    {"name": "UserData", "dataType": "integer", "size": 8},
    {"name": "PointSourceId", "dataType": "integer", "size": 16},
    {"name": "GpsTime", "dataType": "float", "size": 64},
    {"name": "ScanChannel", "dataType": "integer", "size": 8},
    {"name": "ClassFlags", "dataType": "integer", "size": 8},
    {"name": "Red", "dataType": "integer", "size": 16},
    {"name": "Green", "dataType": "integer", "size": 16},
    {"name": "Blue", "dataType": "integer", "size": 16},
    {"name": "NIR", "dataType": "integer", "size": 16},
]

PDRF_TO_SCHEMA = {
    6: list(PDRF8_SCHEMA[0:15]),
    7: list(PDRF8_SCHEMA[0:18]),
    8: PDRF8_SCHEMA,
}


def get_schema_from_pdrf(pdrf):
    """
    Given a LAS PDRF (Point Data Record Format), get the file's schema.
    This schema is specified in Kart Dataset schema.json format, but the schema contents is as it
    would be loaded by PDAL, not necessarily what the file actually contains.
    Eg, scan angles are stored in LAS files as either integers or fixed-point numbers,
    but are always loaded by PDAL as floating point numbers, so that's what we put in the schema.
    """
    result = PDRF_TO_SCHEMA.get(pdrf)
    if not result:
        raise NotImplementedError
    return result


def equivalent_copc_pdrf(pdrf):
    """
    Given any LAS PDRF, returns the COPC compatible PDRF that should be used when converting
    data of this PDRF to COPC. COPC only allows three PDRFs: 6, 7 and 8.
    """
    if pdrf in (8, 10):
        # These PDRF's store R, G, B and NIR
        return 8
    elif pdrf in (2, 3, 5, 7):
        # These PDRF's store R, G, B but not NIR
        return 7
    else:
        return 6
