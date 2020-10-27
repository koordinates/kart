import re

from osgeo.osr import SpatialReference

from .cli_util import StringFromFile
from .geometry import make_crs
from .serialise_util import uint32hash
from .wkt_lexer import WKTLexer


class CoordinateReferenceString(StringFromFile):
    """Click option to specify a CRS."""

    def convert(self, value, param, ctx):
        value = super().convert(value, param, ctx)

        try:
            return make_crs(value)
        except RuntimeError as e:
            self.fail(
                f"Invalid or unknown coordinate reference system: {value!r} ({e})"
            )


class _WktPatterns:
    """Regular expressions for recognizing WKT name and authority."""

    WHITESPACE = r"\s*"
    OPEN_BRACKET = r"[%s]" % re.escape("[(")
    CLOSE_BRACKET = r"[%s]" % re.escape("])")

    WKT_STR = r'"((?:""|[^"])*)"'

    ROOT_NAME_PATTERN = re.compile(
        WHITESPACE.join(["^", "[A-Z]*", OPEN_BRACKET, WKT_STR]), re.IGNORECASE
    )

    ROOT_AUTHORITY_PATTERN = re.compile(
        WHITESPACE.join(
            [
                "AUTHORITY",
                OPEN_BRACKET,
                WKT_STR,
                ",",
                WKT_STR,
                CLOSE_BRACKET,
                CLOSE_BRACKET,
                "$",
            ]
        ),
        re.IGNORECASE,
    )


def parse_name(crs):
    if isinstance(crs, str):
        m = _WktPatterns.ROOT_NAME_PATTERN.search(crs)
        if m:
            return m.group(1)
        else:
            spatial_ref = SpatialReference(crs)
    elif isinstance(crs, SpatialReference):
        spatial_ref = crs
    else:
        raise RuntimeError(f"Unrecognised CRS: {crs}")
    return spatial_ref.GetName()


def parse_authority(crs):
    if isinstance(crs, str):
        m = _WktPatterns.ROOT_AUTHORITY_PATTERN.search(crs)
        if m:
            return m.group(1), m.group(2)
        spatial_ref = SpatialReference(crs)
    elif isinstance(crs, SpatialReference):
        spatial_ref = crs
    else:
        raise RuntimeError(f"Unrecognised CRS: {crs}")

    # Use osgeo only as a fallback if the regex failed -
    # We avoid using it since it is opinionated and sometimes chooses not to return the information it parsed -
    # if it doesn't think the authority matches the CRS, it might just ignore it.
    return spatial_ref.GetAuthorityName(None), spatial_ref.GetAuthorityCode(None)


def get_identifier_str(crs, authority=None):
    """
    Given a CRS, generate a stable, unique identifier for it of type 'str'. Eg: "EPSG:2193"
    """
    if authority is not None:
        auth_name, auth_code = authority
    else:
        auth_name, auth_code = parse_authority(crs)

    if auth_name and auth_code:
        return f"{auth_name}:{auth_code}"
    code = auth_name or auth_code
    if code and code.strip() not in ("0", "EPSG"):
        return code
    return f"CUSTOM:{get_identifier_int(crs, (auth_name, auth_code))}"


def get_identifier_int(crs, authority=None):
    """
    Given a CRS, generate a stable, unique identifer for it of type 'int'. Eg: 2193
    """
    if authority is not None:
        auth_name, auth_code = authority
    else:
        auth_name, auth_code = parse_authority(crs)

    if auth_code and auth_code.isdigit() and int(auth_code) > 0:
        return int(auth_code)

    if isinstance(crs, str):
        wkt = crs.strip()
    elif isinstance(crs, SpatialReference):
        wkt = crs.ExportToPrettyWkt()
    else:
        raise RuntimeError(f"Unrecognised CRS: {crs}")

    # Stable code that fits easily in an int32 and won't collide with EPSG codes.
    return (uint32hash(wkt) & 0xFFFFFFF) + 1000000


def get_identifier_int_from_dataset(dataset, crs_name=None):
    """
    Get the CRS attached to this dataset with a particular name eg "EPSG:2193",
    and return an integer to uniquely identify it, eg 2193.
    (This still works even if the CRS is custom and doesn't have an obvious number embedded in it).
    crs_name can be ommitted if there is no more than one geometry column.
    """

    if crs_name is None:
        geom_columns = dataset.schema.geometry_columns
        num_geom_columns = len(geom_columns)
        if num_geom_columns == 0:
            return None
        elif num_geom_columns == 1:
            crs_name = geom_columns[0].extra_type_info.get("geometryCRS", None)
        else:
            raise ValueError("Dataset has more than one geometry column")

    if crs_name is None:
        return None

    definition = dataset.get_crs_definition(crs_name)
    return get_identifier_int(definition)


def normalise_wkt(wkt):
    token_iter = WKTLexer().get_tokens(wkt, pretty_print=True)
    token_value = (value for token_type, value in token_iter)
    return "".join(token_value)
