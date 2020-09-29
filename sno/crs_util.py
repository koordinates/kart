from osgeo.osr import SpatialReference

from .cli_util import StringFromFile
from .geometry import make_crs
from .serialise_util import uint32hash


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


def get_identifier_str(crs):
    """
    Given a CRS, generate a stable, unique identifier for it of type 'str'. Eg: "EPSG:2193"
    """
    if isinstance(crs, str):
        crs = SpatialReference(crs)
    if not isinstance(crs, SpatialReference):
        raise RuntimeError(f"Unrecognised CRS: {crs}")
    auth_name = crs.GetAuthorityName(None)
    auth_code = crs.GetAuthorityCode(None)
    if auth_name and auth_code:
        return f"{auth_name}:{auth_code}"
    code = auth_name or auth_code
    if code and code.strip() not in ("0", "EPSG"):
        return code
    return f"CUSTOM:{get_identifier_int(crs)}"


def get_identifier_int(crs):
    """
    Given a CRS, generate a stable, unique identifer for it of type 'int'. Eg: 2193
    """
    if isinstance(crs, str):
        crs = SpatialReference(crs)
    if not isinstance(crs, SpatialReference):
        raise RuntimeError(f"Unrecognised CRS: {crs}")
    auth_code = crs.GetAuthorityCode(None)
    if auth_code and auth_code.isdigit() and int(auth_code) > 0:
        return int(auth_code)
    # Stable code that fits easily in an int32 and won't collide with EPSG codes.
    return (uint32hash(crs.ExportToWkt()) & 0xFFFFFFF) + 1000000


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
