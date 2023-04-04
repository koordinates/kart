import re

from kart.tile.tilename_util import TILE_BASENAME_PATTERN, PAM_SUFFIX


def remove_tile_extension(filename, remove_pam_suffix=False):
    """Given a tile filename, removes the suffix .tif or .tiff"""
    match = re.fullmatch(r"(.+?)(\.tiff?)?(\.aux\.xml)?", filename, re.IGNORECASE)
    if match:
        return (
            match.group(1)
            if remove_pam_suffix
            else match.group(1) + (match.group(3).lower() if match.group(3) else "")
        )
    return filename


def set_tile_extension(filename, ext=None, tile_format=None):
    """Changes a tile's file extension to the given extension, or to the extension appropriate for its format."""

    # Not much to do here since we only support one tile-format currently: a GeoTIFF that may or may not be COG.
    # TODO: maybe checkout as tif vs tiff should be user configurable.
    if ext is None:
        ext = ".tif.aux.xml" if filename.endswith(PAM_SUFFIX) else ".tif"

    return remove_tile_extension(filename, remove_pam_suffix=True) + ext


def get_tile_path_pattern(
    tilename=None, *, parent_path=None, include_conflict_versions=False, is_pam=None
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
    If is_pam is True, this will match only PAM files eg "*.aux.xml". If is_pam is False, will match only non-PAM tiles.
    If is_pam is None, this will match both PAM and non-PAM files.
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
    ext_pattern = r"(?i:\.tiff?)"
    if is_pam is True:
        ext_pattern += r"(?i:\.aux\.xml)"
    elif is_pam is None:
        ext_pattern += r"(?i:\.aux\.xml)?"

    return re.compile(parent_pattern + tile_pattern + version_pattern + ext_pattern)
