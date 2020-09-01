from osgeo.osr import SpatialReference


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
    return (hash(crs.ExportToWkt()) & 0xFFFFFFF) + 1000000
