import struct

from kart.exceptions import NotYetImplemented
from kart.schema import Schema

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
    {"name": "X", "dataType": "integer", "size": 32},
    {"name": "Y", "dataType": "integer", "size": 32},
    {"name": "Z", "dataType": "integer", "size": 32},
    {"name": "Intensity", "dataType": "integer", "size": 16, "unsigned": True},
    {"name": "Return Number", "dataType": "integer", "size": 3, "unsigned": True},
    {"name": "Number of Returns", "dataType": "integer", "size": 3, "unsigned": True},
    {"name": "Scan Direction Flag", "dataType": "integer", "size": 1},
    {"name": "Edge of Flight Line", "dataType": "integer", "size": 1},
    {"name": "Classification", "dataType": "integer", "size": 5, "unsigned": True},
    {"name": "Synthetic", "dataType": "integer", "size": 1},
    {"name": "Key-Point", "dataType": "integer", "size": 1},
    {"name": "Withheld", "dataType": "integer", "size": 1},
    {"name": "Scan Angle Rank", "dataType": "integer", "size": 8},
    {"name": "User Data", "dataType": "integer", "size": 8, "unsigned": True},
    {"name": "Point Source ID", "dataType": "integer", "size": 16, "unsigned": True},
]

GPS_TIME = {"name": "GPS Time", "dataType": "float", "size": 64}
RED_GREEN_BLUE = [
    {"name": "Red", "dataType": "integer", "size": 16, "unsigned": True},
    {"name": "Green", "dataType": "integer", "size": 16, "unsigned": True},
    {"name": "Blue", "dataType": "integer", "size": 16, "unsigned": True},
]

PDRF6_SCHEMA = [
    {"name": "X", "dataType": "integer", "size": 32},
    {"name": "Y", "dataType": "integer", "size": 32},
    {"name": "Z", "dataType": "integer", "size": 32},
    {"name": "Intensity", "dataType": "integer", "size": 16, "unsigned": True},
    {"name": "Return Number", "dataType": "integer", "size": 4, "unsigned": True},
    {"name": "Number of Returns", "dataType": "integer", "size": 4, "unsigned": True},
    {"name": "Synthetic", "dataType": "integer", "size": 1},
    {"name": "Key-Point", "dataType": "integer", "size": 1},
    {"name": "Withheld", "dataType": "integer", "size": 1},
    {"name": "Overlap", "dataType": "integer", "size": 1},
    {"name": "Scanner Channel", "dataType": "integer", "size": 2, "unsigned": True},
    {"name": "Scan Direction Flag", "dataType": "integer", "size": 1},
    {"name": "Edge of Flight Line", "dataType": "integer", "size": 1},
    {"name": "Classification", "dataType": "integer", "size": 8, "unsigned": True},
    {"name": "User Data", "dataType": "integer", "size": 8, "unsigned": True},
    {"name": "Scan Angle", "dataType": "integer", "size": 16},
    {"name": "Point Source ID", "dataType": "integer", "size": 16, "unsigned": True},
    {"name": "GPS Time", "dataType": "float", "size": 64},
]

INFRARED = {"name": "Infrared", "dataType": "integer", "size": 16}


PDRF_TO_SCHEMA = {
    k: Schema(v)
    for k, v in {
        0: PDRF0_SCHEMA + [],
        1: PDRF0_SCHEMA + [GPS_TIME],
        2: PDRF0_SCHEMA + RED_GREEN_BLUE,
        3: PDRF0_SCHEMA + [GPS_TIME] + RED_GREEN_BLUE,
        6: PDRF6_SCHEMA + [],
        7: PDRF6_SCHEMA + RED_GREEN_BLUE,
        8: PDRF6_SCHEMA + RED_GREEN_BLUE + [INFRARED],
    }.items()
}

# Record length in bytes:
PDRF_TO_RECORD_LENGTH = {
    0: 20,
    1: 28,
    2: 26,
    3: 34,
    6: 30,
    7: 36,
    8: 38,
}

# Make sure the schemas actually have the above sizes, otherwise there is a bug in the data above.
assert PDRF_TO_RECORD_LENGTH == {
    k: sum([d["size"] for d in v]) // 8 for k, v in PDRF_TO_SCHEMA.items()
}


def get_schema_from_pdrf_and_vlr(pdrf, extra_bytes_vlr):
    """
    Given a LAS PDRF (Point Data Record Format), get the file's schema.
    This schema is specified in Kart Dataset schema.json format, but the schema contents is as it
    would be loaded by PDAL, not necessarily what the file actually contains.
    Eg, scan angles are stored in LAS files as either integers or fixed-point numbers,
    but are always loaded by PDAL as floating point numbers, so that's what we put in the schema.
    """
    base_result = PDRF_TO_SCHEMA.get(pdrf)
    if not base_result:
        # PDAL doesn't support these either:
        raise NotYetImplemented(
            "Sorry, Kart does not support point formats with waveform data (4, 5, 9 and 10)"
        )
    if extra_bytes_vlr:
        return Schema(base_result + get_schema_from_extra_bytes_vlr(extra_bytes_vlr))
    return Schema(base_result)


def get_schema_from_extra_bytes_vlr(extra_bytes_vlr):
    result = []
    for dimension in struct.iter_unpack("<xxBB32s124x32s", extra_bytes_vlr):
        data_type, options, name, description = dimension
        name = name.strip(b"\x00").decode()
        result.append({"name": name, **_vlr_type_to_kart_type(data_type, options)})
    return Schema(result)


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


def _vlr_type_to_kart_type(vlr_datatype, options):
    assert 0 <= vlr_datatype <= 10
    if vlr_datatype == 0:
        return {"dataType": "blob", "length": options}
    return _VLR_TYPE_TO_KART_TYPE[vlr_datatype]


_VLR_TYPE_TO_KART_TYPE = {
    0: {"dataType": "blob"},
    1: {"dataType": "integer", "size": 8, "unsigned": True},
    2: {"dataType": "integer", "size": 8},
    3: {"dataType": "integer", "size": 16, "unsigned": True},
    4: {"dataType": "integer", "size": 16},
    5: {"dataType": "integer", "size": 32, "unsigned": True},
    6: {"dataType": "integer", "size": 32},
    7: {"dataType": "integer", "size": 64, "unsigned": True},
    8: {"dataType": "integer", "size": 64},
    9: {"dataType": "float", "size": 32},
    10: {"dataType": "float", "size": 64},
}
