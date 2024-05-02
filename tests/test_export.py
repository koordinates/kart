import os
import pytest
import re
import subprocess

from kart import prefix, is_frozen, is_windows
from kart.geometry import Geometry

H = pytest.helpers.helpers()


def get_ogr_tool(toolname):
    # GDAL tools are normally in prefix/tools/gdal
    # Exception is pyinstaller windows build where we put them in the prefix directory,
    # alongside the libs that they need so that they still work even when run from outside of Kart.
    ogr_tools_dir = (
        prefix if (is_frozen and is_windows) else os.path.join(prefix, "tools/gdal")
    )
    return os.path.join(ogr_tools_dir, toolname)


def test_ogr_tools_bundled():
    for toolname in ("ogrinfo", "ogr2ogr"):
        output = subprocess.check_output(
            [get_ogr_tool(toolname), "--version"], text=True
        ).splitlines()
        match = re.match(r"GDAL (\d+)\.(\d+)\.(\d+)", output[0])
        assert match
        version_tuple = tuple(int(match.group(x)) for x in range(1, 4))
        assert version_tuple >= (3, 8, 0)


@pytest.mark.parametrize(
    "archive,layer,geometry",
    [
        pytest.param("points", H.POINTS, "Point", id="points"),
        pytest.param("polygons", H.POLYGONS, "Polygon", id="polygons"),
        pytest.param("table", H.TABLE, None, id="table"),
    ],
)
def test_export_datasets(archive, layer, geometry, data_archive, cli_runner):
    with data_archive(archive) as repo_dir:
        r = cli_runner.invoke(["export", layer.LAYER, "output.shp"])
        assert r.exit_code == 0, r
        assert (repo_dir / "output.shp").exists()

        output = subprocess.check_output(
            [get_ogr_tool("ogrinfo"), "-so", "-al", "output.shp"], text=True
        )

        assert f"Feature Count: {layer.ROWCOUNT}" in output
        if geometry is not None:
            assert f"Geometry: {geometry}" in output

        for field_name, value in layer.RECORD.items():
            if isinstance(value, Geometry):
                continue

            assert field_name[0:10] in output
