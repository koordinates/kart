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

    COMMA = ","
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
                COMMA,
                WKT_STR,
                CLOSE_BRACKET,
                CLOSE_BRACKET,
                "$",
            ]
        ),
        re.IGNORECASE,
    )

    FINAL_AXIS_PATTERN = re.compile(
        WHITESPACE.join(
            [
                "AXIS",
                OPEN_BRACKET,
                WKT_STR,
                COMMA,
                r"(?:NORTH|SOUTH|EAST|WEST)",
                CLOSE_BRACKET,
                COMMA,
                "$",
            ]
        )
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


def get_identifier_str(crs):
    """
    Given a CRS, find or generate a stable, unique identifier for it of type 'str'.
    Eg: "EPSG:2193" or "CUSTOM:201234"
    """
    result = _find_identifier_str(crs)
    if not result:
        result = f"CUSTOM:{_generate_identifier_int(crs)}"
    return result.replace("/", "_")


def _find_identifier_str(crs):
    """Given a CRS, find a sensible identifier string for it from within the WKT."""
    auth_name, auth_code = parse_authority(crs)
    # Use AUTH_NAME:AUTH_CODE if both are set:
    if auth_name and auth_code:
        return f"{auth_name}:{auth_code}"
    # Use AUTH_NAME or AUTH_CODE if one of them is set and probably a real identifier:
    code = auth_name or auth_code
    if code and code.strip() not in ("", "0", "EPSG", "ESRI"):
        return code
    # Otherwise, use the CRS name, if set.
    name = parse_name(crs)
    if name is not None:
        name = name.strip()
    return name if name else None


def get_identifier_int(crs):
    """
    Given a CRS, find or generate a stable, unique identifer for it of type 'int'.
    Eg: 2193 or 201234
    """
    # Find the auth code from the WKT if one is set and is an integer:
    result = _find_identifier_int(crs)
    # Otherwise, generate a stable ID based on the WKT authority / name / contents.
    if not result:
        result = _generate_identifier_int(crs)
    return result


def _find_identifier_int(crs):
    """Given a CRS, find a sensible identifier int for it from its authority in the WKT."""
    auth_name, auth_code = parse_authority(crs)
    if auth_code and auth_code.isdigit() and int(auth_code) > 0:
        return int(auth_code)
    return None


MIN_CUSTOM_ID = 200000
MAX_CUSTOM_ID = 209199
CUSTOM_RANGE = MAX_CUSTOM_ID - MIN_CUSTOM_ID + 1


def _generate_identifier_int(crs):
    """Given a CRS, generate a unique stable int for it - based on its authority or name, if these are present."""

    # Generate an identifier int based on the WKT authority or name if one is set:
    identifier_str = _find_identifier_str(crs)
    normalised_wkt = None
    # Otherwise, use the entire CRS to generate a unique, stable int.
    if not identifier_str:
        # This CRS has no authority or name - we fall back to generating an ID based on its contents.
        # This is undesirable since it means any change to the CRS renames it, which is confusing -
        # it will get a new auto-generated ID - but the user can name it to avoid this behaviour.
        if isinstance(crs, str):
            normalised_wkt = normalise_wkt(crs)
        elif isinstance(crs, SpatialReference):
            normalised_wkt = normalise_wkt(crs.ExportToWkt())
        else:
            raise RuntimeError(f"Unrecognised CRS: {crs}")

    # Stable code within the allowed range - MIN_CUSTOM_ID...MAX_CUSTOM_ID
    raw_hash = uint32hash(identifier_str or normalised_wkt)
    return (raw_hash % CUSTOM_RANGE) + MIN_CUSTOM_ID


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
    if not wkt:
        return wkt
    token_iter = WKTLexer().get_tokens(wkt, pretty_print=True)
    token_value = (value for token_type, value in token_iter)
    return "".join(token_value)


def ensure_axes_specified(wkt):
    # Adds DEFAULT_AXES to a definition if there are no axes present in the definition.
    # There is a non-standard requirement by MySQL that AXES are specified.
    if not wkt:
        return wkt
    DEFAULT_AXES = 'AXIS["X", EAST], AXIS["Y", NORTH], '
    m = _WktPatterns.ROOT_AUTHORITY_PATTERN.search(wkt)
    if m:
        start, end = m.span()
        wkt_without_authority = wkt[:start]
        authority = wkt[start:end]
        if not _WktPatterns.FINAL_AXIS_PATTERN.search(wkt_without_authority):
            return wkt_without_authority + DEFAULT_AXES + authority

    return wkt


def ensure_authority_specified(wkt, auth_name, auth_code):
    if not wkt:
        return wkt

    m = _WktPatterns.ROOT_AUTHORITY_PATTERN.search(wkt)
    if not m:
        index = wkt.rindex(']')
        return wkt[:index] + f'AUTHORITY["{auth_name}", "{auth_code}"]' + wkt[index:]
    return wkt
