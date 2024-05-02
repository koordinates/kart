import os
import re
import subprocess

from kart import prefix, is_frozen, is_windows


def test_ogr_tools_bundled():
    # GDAL tools are normally in prefix/tools/gdal
    # Exception is pyinstaller windows build where we put them in the prefix directory,
    # alongside the libs that they need so that they still work even when run from outside of Kart.
    ogr_tools_dir = (
        prefix if (is_frozen and is_windows) else os.path.join(prefix, "tools/gdal")
    )

    # Where-ever they are, they should run without error (at least when run from within Kart).
    for toolname in ("ogrinfo", "ogr2ogr"):
        toolpath = os.path.join(ogr_tools_dir, toolname)
        output = subprocess.check_output(
            [str(toolpath), "--version"], text=True
        ).splitlines()
        match = re.match(r"GDAL (\d+)\.(\d+)\.(\d+)", output[0])
        assert match
        version_tuple = tuple(int(match.group(x)) for x in range(1, 4))
        assert version_tuple >= (3, 8, 0)
