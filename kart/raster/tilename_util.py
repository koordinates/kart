import re


def remove_tile_extension(filename):
    """Given a tile filename, removes the suffix .tif or .tiff"""
    match = re.fullmatch(r"(.+?)\.tiff?", filename, re.IGNORECASE)
    if match:
        return match.group(1)
    return filename


def set_tile_extension(filename, ext=None, tile_format=None):
    """Changes a tile's file extension to the given extension, or to the extension appropriate for its format."""

    # Not much to do here since we only support one tile-format currently: a GeoTIFF that may or may not be COG.
    # TODO: maybe checkout as tif vs tiff should be user configurable.
    if ext is None:
        ext = ".tif"

    return remove_tile_extension(filename) + ext


def get_tile_path_pattern(
    tilename=None, *, parent_path=None, include_conflict_versions=False
):
    """
    Given a tilename eg "mytile" and a parent_path eg "myfolder",
    returns a regex that accepts "myfolder/mytile.tif", "myfolder/mytile.TIF", "myfolder/mytile.tiff", etc.
    Note that path separators need to be normalised to "/" before this is called (as they are in a Git diff).

    If tilename is not specified, the regex will match any non-empty tilename that contains no path separators, and this will
    be the first group in the result.
    If parent_path is not specified, the resulting regex will match only tiles that have no parent-path prefix ie simply "mytile.laz"
    If include_conflict_versions is True, then an "ancestor" / "ours" / "theirs" infix will also be matched if needed -
    that is, "mytile.laz", "mytile.ancestor.laz", "mytile.ours.laz" and "mytile.theirs.laz" are all matched (and so on).
    """

    parent_pattern = (
        re.escape(parent_path.rstrip("/") + "/") if parent_path is not None else ""
    )
    tile_pattern = re.escape(tilename) if tilename is not None else r"([^/]+)"
    version_pattern = (
        r"(?:\.ancestor|\.ours|\.theirs)?" if include_conflict_versions else ""
    )
    ext_pattern = r"\.[Tt][If][Ff][Ff]?"
    return re.compile(parent_pattern + tile_pattern + version_pattern + ext_pattern)
