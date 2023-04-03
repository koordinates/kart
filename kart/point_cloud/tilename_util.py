import re

from .metadata_util import is_copc
from kart.tile.tilename_util import TILE_BASENAME_PATTERN


def remove_tile_extension(filename):
    """Given a tile filename, removes the suffix .las or .laz or .copc.las or .copc.laz"""
    match = re.fullmatch(r"(.+?)(?:\.copc)?\.la[sz]", filename, re.IGNORECASE)
    if match:
        return match.group(1)
    return filename


def set_tile_extension(filename, ext=None, tile_format=None):
    """Changes a tile's file extension to the given extension, or to the extension appropriate for its format."""
    if not ext:
        assert tile_format is not None
        if isinstance(tile_format, dict):
            if "format.json" in tile_format:
                tile_format = tile_format["format.json"]
            elif "format" in tile_format:
                tile_format = tile_format["format"]

        if isinstance(tile_format, dict):
            # Format info as stored in format.json
            ext = f".{tile_format['compression']}"
        else:
            # Format summary string.
            ext = f".{tile_format[0:3]}"

        assert ext in (".las", ".laz")

        if is_copc(tile_format):
            ext = ".copc" + ext

    elif not ext.startswith("."):
        ext = f".{ext}"

    return remove_tile_extension(filename) + ext


def get_tile_path_pattern(
    tilename=None, *, parent_path=None, include_conflict_versions=False
):
    """
    Given a tilename eg "mytile" and a parent_path eg "myfolder",
    returns a regex that accepts "myfolder/mytile.laz", "myfolder/mytile.LAZ", "myfolder/mytile.copc.laz", "myfolder/mytile.las", etc.
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
    tile_pattern = (
        re.escape(tilename) if tilename is not None else TILE_BASENAME_PATTERN
    )
    version_pattern = (
        r"(?:\.ancestor|\.ours|\.theirs)?" if include_conflict_versions else ""
    )
    ext_pattern = r"(?i:\.copc\.laz|\.la[sz])"
    return re.compile(parent_pattern + tile_pattern + version_pattern + ext_pattern)
