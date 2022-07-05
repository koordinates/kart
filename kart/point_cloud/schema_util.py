from kart.exceptions import NotYetImplemented

# Utility functions for dealing with Point Cloud schemas.

# Documentation on the actual schema as encoded in the LAS tile is available here:
# https://www.asprs.org/wp-content/uploads/2019/07/LAS_1_4_r15.pdf

# However, we are more concerned with the schema as it is loaded by PDAL, which can
# change names or types for consistency with other PDRFs, or for readability or convenience.

# The easiest way to get this data is just to use PDAL to extract it. For example, to see
# how PDAL loads a tile using PDRF 0, run the following:
#
# > pdal translate example.laz 0.laz --writers.las.format="0"
# > kart --post-mortem
# (kart) ipdb> from kart.point_cloud.metadata_util import extract_pc_tile_metadata
# (kart) ipdb> extract_pc_tile_metadata("0.laz")

PDRF0_SCHEMA = [
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
]

GPS_TIME = {"name": "GpsTime", "dataType": "float", "size": 64}
RED_GREEN_BLUE = [
    {"name": "Red", "dataType": "integer", "size": 16},
    {"name": "Green", "dataType": "integer", "size": 16},
    {"name": "Blue", "dataType": "integer", "size": 16},
]

PDRF6_SCHEMA = PDRF0_SCHEMA + [
    GPS_TIME,
    {"name": "ScanChannel", "dataType": "integer", "size": 8},
    {"name": "ClassFlags", "dataType": "integer", "size": 8},
]

INFRARED = {"name": "Infrared", "dataType": "integer", "size": 16}


PDRF_TO_SCHEMA = {
    0: PDRF0_SCHEMA + [],
    1: PDRF0_SCHEMA + [GPS_TIME],
    2: PDRF0_SCHEMA + RED_GREEN_BLUE,
    3: PDRF0_SCHEMA + [GPS_TIME] + RED_GREEN_BLUE,
    6: PDRF6_SCHEMA + [],
    7: PDRF6_SCHEMA + RED_GREEN_BLUE,
    8: PDRF6_SCHEMA + RED_GREEN_BLUE + [INFRARED],
}

PDRF_TO_RECORD_LENGTH = {
    0: 20,
    1: 28,
    2: 26,
    3: 34,
    6: 30,
    7: 36,
    8: 38,
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
        # PDAL doesn't support these either:
        raise NotYetImplemented(
            "Sorry, Kart does not support point formats with waveform data (4, 5, 9 and 10)"
        )
    return result


def get_record_length_from_pdrf(pdrf):
    result = PDRF_TO_RECORD_LENGTH.get(pdrf)
    if not result:
        raise NotYetImplemented(
            "Sorry, Kart does not support point formats with waveform data (4, 5, 9 and 10)"
        )
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


def pdal_schema_to_kart_schema(pdal_schema):
    """
    Given the JSON schema as PDAL loaded it, format it as a Kart compatiblie schema.json item.
    Eg "type" -> "dataType", size is measured in bits.
    """
    return [
        _pdal_col_schema_to_kart_col_schema(col) for col in pdal_schema["dimensions"]
    ]


def _pdal_col_schema_to_kart_col_schema(pdal_col_schema):
    return {
        "name": pdal_col_schema["name"],
        "dataType": _pdal_type_to_kart_type(pdal_col_schema["type"]),
        # Kart measures data-sizes in bits, PDAL in bytes.
        "size": pdal_col_schema["size"] * 8,
    }


# TODO - investigate what types PDAL can actually return - it's not the same as the LAZ spec.
# TODO - our dataset types don't have any notion of signed vs unsigned.
_PDAL_TYPE_TO_KART_TYPE = {
    "floating": "float",
    "unsigned": "integer",
    "string": "text",
}


def _pdal_type_to_kart_type(pdal_type):
    return _PDAL_TYPE_TO_KART_TYPE.get(pdal_type) or pdal_type
