import os
import pytest
import re
import subprocess

from sqlalchemy.orm import sessionmaker

from kart import prefix, is_frozen, is_windows
from kart.exceptions import INVALID_ARGUMENT
from kart.geometry import Geometry
from kart.sqlalchemy.sqlite import sqlite_engine

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
                'id',
                'geometry',
                'date_adjusted',
                'survey_reference',
                'adjusted_nodes',
            ]


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
                    'fid',
                    'geom',
                    'id',
                    'date_adjusted',
                    'survey_reference',
                    'adjusted_nodes',
                ]
            else:
                # Using --no-primary-key-as-field gets rid of the redundant column here.
                assert col_names == [
                    'fid',
                    'geom',
                    'date_adjusted',
                    'survey_reference',
                    'adjusted_nodes',
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
