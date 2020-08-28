from osgeo.osr import SpatialReference


def get_identifier(crs):
    """
    Given a CRS, generate a unique idenfier for it. Eg: "EPSG:2193"
    """
    if isinstance(crs, str):
        crs = SpatialReference(crs)
    if isinstance(crs, SpatialReference):
        auth_name = crs.GetAuthorityName(None)
        auth_code = crs.GetAuthorityCode(None)
        return f"{auth_name}:{auth_code}"
    raise RuntimeError(f"Unrecognised CRS: {crs}")
