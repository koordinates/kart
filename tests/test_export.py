import os
import re
import subprocess
from pathlib import Path

import pytest
from sqlalchemy.orm import sessionmaker

from kart import prefix, is_frozen, is_windows
from kart.exceptions import INVALID_ARGUMENT, UNCATEGORIZED_ERROR
from kart.geometry import Geometry
from kart.sqlalchemy.sqlite import sqlite_engine
from kart.tabular.export import get_driver_by_ext

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
        assert r.exit_code == 0, r.stderr
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


@pytest.mark.parametrize(
    "spec,driver,destination",
    [
        ("output.gpkg", "GPKG", "output.gpkg"),
        ("GPKG:output.gpkg", "GPKG", "output.gpkg"),
        ("output.shp", "ESRI Shapefile", "output.shp"),
        ("ESRI Shapefile:output.shp", "ESRI Shapefile", "output.shp"),
        (r"/abs/path/to/output.gpkg", "GPKG", r"/abs/path/to/output.gpkg"),
        (r"C:\Users\me\output.gpkg", "GPKG", r"C:\Users\me\output.gpkg"),
        (r"GPKG:C:\Users\me\output.gpkg", "GPKG", r"C:\Users\me\output.gpkg"),
        (
            "postgresql://user:password@localhost:5432/database",
            "PostgreSQL",
            "postgresql://user:password@localhost:5432/database",
        ),
    ],
)
def test_export_path_styles(spec, driver, destination, data_archive, cli_runner):
    from kart.tabular.export import get_driver

    actual_driver, actual_destination = get_driver(spec)
    assert actual_driver.GetName() == driver
    assert actual_destination == destination


@pytest.mark.parametrize("epsg", [4326, 27200])
def test_export_with_transformed_crs(epsg, data_archive, cli_runner):
    with data_archive("polygons") as repo_dir:
        r = cli_runner.invoke(
            ["export", H.POLYGONS.LAYER, "output.shp", f"--crs=EPSG:{epsg}"]
        )
        assert r.exit_code == 0, r.stderr
        assert (repo_dir / "output.shp").exists()

        output = subprocess.check_output(
            [get_ogr_tool("ogrinfo"), "-so", "-al", "output.shp"], text=True
        )

        if epsg == 4326:
            assert "World Geodetic System 1984" in output
            assert _get_extent(output) == pytest.approx(
                (172.32, -43.63, 176.99, -35.69), 0.01
            )
        else:
            assert "NZGD49 / New Zealand Map Grid" in output
            assert _get_extent(output) == pytest.approx(
                (2455030, 5730000, 2860430, 6611590), 10
            )


def _get_extent(output):
    number = r"([0-9.-]+)"
    match = re.search(
        rf"Extent: \({number}, {number}\) - \({number}, {number}\)", output
    )
    if match:
        return tuple(float(match.group(i + 1)) for i in range(4))
    return None


def test_dataset_creation_options(data_archive, cli_runner):
    extra_args = ["-dsco", "ADD_GPKG_OGR_CONTENTS=NO"]

    with data_archive("polygons") as repo_dir:
        r = cli_runner.invoke(["export", H.POLYGONS.LAYER, "output.gpkg", *extra_args])
        assert r.exit_code == 0, r.stderr
        assert (repo_dir / "output.gpkg").exists()

        engine = sqlite_engine(repo_dir / "output.gpkg")
        with sessionmaker(bind=engine)() as sess:
            count = sess.scalar(
                "SELECT COUNT(*) FROM sqlite_master WHERE name = 'gpkg_ogr_contents';"
            )
            assert count == 0


def test_layer_creation_options(data_archive, cli_runner):
    extra_args = [
        "-lco",
        "FID=id",
        "-lco",
        "GEOMETRY_NAME=geometry",
        "--no-primary-key-as-field",
    ]

    with data_archive("polygons") as repo_dir:
        r = cli_runner.invoke(["export", H.POLYGONS.LAYER, "output.gpkg", *extra_args])
        assert r.exit_code == 0, r.stderr
        assert (repo_dir / "output.gpkg").exists()

        engine = sqlite_engine(repo_dir / "output.gpkg")
        with sessionmaker(bind=engine)() as sess:
            r = sess.execute("PRAGMA table_info(nz_waca_adjustments);")
            col_names = [row[1] for row in r]
            # FID column has been named id, geometry column has been named geometry.
            assert col_names == [
                "id",
                "geometry",
                "date_adjusted",
                "survey_reference",
                "adjusted_nodes",
            ]


def test_gdal_config_options(monkeypatch, data_archive, cli_runner):
    # Make sure that GDAL config options are being honoured.
    with data_archive("points") as repo_dir:
        monkeypatch.setenv("OGR_CURRENT_DATE", "2000-01-01T00:00:00Z")
        r = cli_runner.invoke(["export", H.POINTS.LAYER, "--layer=one", "output.gpkg"])
        assert r.exit_code == 0, r.stderr

        monkeypatch.setenv("OGR_CURRENT_DATE", "2012-03-04T05:06:07Z")
        r = cli_runner.invoke(["export", H.POINTS.LAYER, "--layer=two", "output.gpkg"])
        assert r.exit_code == 0, r.stderr

        engine = sqlite_engine(repo_dir / "output.gpkg")
        with sessionmaker(bind=engine)() as sess:
            r = sess.execute("SELECT last_change FROM gpkg_contents;")
            assert set(row[0] for row in r) == {
                "2000-01-01T00:00:00Z",
                "2012-03-04T05:06:07Z",
            }


@pytest.mark.parametrize(
    "field_option", ["--primary-key-as-field", "--no-primary-key-as-field"]
)
def test_primary_key_as_field(field_option, data_archive, cli_runner):
    extra_args = [field_option] if field_option else []
    with data_archive("polygons") as repo_dir:
        r = cli_runner.invoke(["export", H.POLYGONS.LAYER, "output.gpkg", *extra_args])
        assert r.exit_code == 0, r.stderr
        assert (repo_dir / "output.gpkg").exists()

        engine = sqlite_engine(repo_dir / "output.gpkg")
        with sessionmaker(bind=engine)() as sess:
            r = sess.execute("PRAGMA table_info(nz_waca_adjustments);")
            col_names = [row[1] for row in r]
            if field_option == "--primary-key-as-field":
                # GDAL GPKG driver stores the feature FID values in a column called FID.
                # So --primary-key-as-field means that we end up with two id columns, FID and ID.
                # This is redundant here but would be useful if the PK column had string values.
                assert col_names == [
                    "fid",
                    "geom",
                    "id",
                    "date_adjusted",
                    "survey_reference",
                    "adjusted_nodes",
                ]
            else:
                # Using --no-primary-key-as-field gets rid of the redundant column here.
                assert col_names == [
                    "fid",
                    "geom",
                    "date_adjusted",
                    "survey_reference",
                    "adjusted_nodes",
                ]

            # Both the fid column and the id column (if present) should contain the primary key values.
            present_id_cols = [c for c in col_names if c in ("fid", "id")]
            for col in present_id_cols:
                min_val = sess.scalar(
                    f"SELECT {col} FROM nz_waca_adjustments ORDER by {col} ASC LIMIT 1;"
                )
                max_val = sess.scalar(
                    f"SELECT {col} FROM nz_waca_adjustments ORDER by {col} DESC LIMIT 1;"
                )
                assert (min_val, max_val) == (1424927, 4423293)


@pytest.mark.parametrize(
    "fid_option", ["--primary-key-as-fid", "--no-primary-key-as-fid", ""]
)
def test_primary_key_as_fid(fid_option, data_archive, cli_runner):
    extra_args = [fid_option] if fid_option else []
    with data_archive("polygons") as repo_dir:
        r = cli_runner.invoke(["export", H.POLYGONS.LAYER, "output.gpkg", *extra_args])
        assert r.exit_code == 0, r.stderr
        assert (repo_dir / "output.gpkg").exists()

        engine = sqlite_engine(repo_dir / "output.gpkg")
        with sessionmaker(bind=engine)() as sess:
            feature_count = sess.scalar("SELECT COUNT(*) FROM nz_waca_adjustments;")
            assert feature_count == H.POLYGONS.ROWCOUNT

            min_fid = sess.scalar(
                "SELECT fid FROM nz_waca_adjustments ORDER by fid ASC LIMIT 1;"
            )
            max_fid = sess.scalar(
                "SELECT fid FROM nz_waca_adjustments ORDER by fid DESC LIMIT 1;"
            )

            if fid_option == "--no-primary-key-as-fid":
                # OGR assigns default FIDs into its FID column.
                assert (min_fid, max_fid) == (1, 228)
            else:
                # Primary-key values copied into the FID column.
                assert (min_fid, max_fid) == (1424927, 4423293)


@pytest.mark.parametrize(
    "fid_option", ["--primary-key-as-fid", "--no-primary-key-as-fid", ""]
)
def test_primary_key_as_fid__string_pks(fid_option, data_archive, cli_runner):
    extra_args = [fid_option] if fid_option else []
    with data_archive("string-pks") as repo_dir:
        r = cli_runner.invoke(["export", H.POLYGONS.LAYER, "output.gpkg", *extra_args])
        if fid_option == "--primary-key-as-fid":
            assert r.exit_code == INVALID_ARGUMENT
            return
        else:
            assert r.exit_code == 0, r.stderr

        assert (repo_dir / "output.gpkg").exists()

        engine = sqlite_engine(repo_dir / "output.gpkg")
        with sessionmaker(bind=engine)() as sess:
            feature_count = sess.scalar("SELECT COUNT(*) FROM nz_waca_adjustments;")
            assert feature_count == H.POLYGONS.ROWCOUNT

            min_fid = sess.scalar(
                "SELECT fid FROM nz_waca_adjustments ORDER by fid ASC LIMIT 1;"
            )
            max_fid = sess.scalar(
                "SELECT fid FROM nz_waca_adjustments ORDER by fid DESC LIMIT 1;"
            )
            # OGR assigns default FIDs into its FID column.
            assert (min_fid, max_fid) == (1, 228)


@pytest.mark.parametrize("ext", ["csv", "fgb", "geojson", "gdb", "gpkg", "shp"])
def test_export_overwrite(ext, data_archive, cli_runner):
    multiple_layers_supported = ext in ("gpkg", "gdb")
    filename = f"output.{ext}"
    with data_archive("points") as repo_dir:
        r = cli_runner.invoke(["export", H.POINTS.LAYER, filename, "--layer=one"])
        assert r.exit_code == 0, r.stderr
        assert (repo_dir / filename).exists()

        r = cli_runner.invoke(["export", H.POINTS.LAYER, filename, "--layer=one"])
        assert r.exit_code != 0

        r = cli_runner.invoke(
            ["export", H.POINTS.LAYER, filename, "--layer=one", "--overwrite"]
        )
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["export", H.POINTS.LAYER, filename, "--layer=two"])
        if multiple_layers_supported:
            assert r.exit_code == 0, r.stderr
        else:
            assert r.exit_code != 0

        ds = get_driver_by_ext(filename).Open(filename)
        if multiple_layers_supported:
            assert ds.GetLayerByName("one") is not None
            assert ds.GetLayerByName("two") is not None
        else:
            assert ds.GetLayerCount() == 1
        ds = None


# These drivers support opening a directory as a dataset, such that creating a layer
# creates a file or files inside that directory.
@pytest.mark.parametrize(
    "driver,ext",
    [
        pytest.param("CSV", "csv", id="CSV"),
        pytest.param("FlatGeobuf", "fgb", id="FlatGeobuf"),
        pytest.param("ESRI Shapefile", "shp", id="ESRI Shapefile"),
    ],
)
def test_export_to_directory(driver, ext, data_archive, cli_runner):
    with data_archive("points") as repo_dir:
        r = cli_runner.invoke(
            ["export", H.POINTS.LAYER, f"{driver}:output_dir/", "--layer=one"]
        )
        assert r.exit_code == 0, r.stderr
        assert (repo_dir / "output_dir" / f"one.{ext}").exists()

        r = cli_runner.invoke(
            ["export", H.POINTS.LAYER, f"{driver}:output_dir/", "--layer=one"]
        )
        assert r.exit_code != 0

        # None of these drivers support OVERWRITE=YES, so no point testing the "--overwrite" flag to make them
        # overwrite the first layer.
        # The FlatGeobuf driver also doesn't support reopening the directory as a dataset and adding another
        # layer to that dataset, but it will let you directly open a file inside that directory and write to that.

        if driver == "FlatGeobuf":
            r = cli_runner.invoke(
                ["export", H.POINTS.LAYER, f"{driver}:output_dir/two.{ext}"]
            )
        else:
            r = cli_runner.invoke(
                ["export", H.POINTS.LAYER, f"{driver}:output_dir/", "--layer=two"]
            )
        assert r.exit_code == 0, r.stderr
        assert (repo_dir / "output_dir" / f"two.{ext}").exists()


def test_error_from_ogr(data_archive, cli_runner):
    with data_archive("points") as _:
        r = cli_runner.invoke(
            ["export", H.POINTS.LAYER, "output.csv", "-lco", "GEOMETRY=AS_MAGIC_SPELL"]
        )
        assert r.exit_code == UNCATEGORIZED_ERROR
        assert "Error running GDAL OGR driver:" in r.stderr
