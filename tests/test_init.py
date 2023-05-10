import json
import re
import shutil

import pytest

from kart import dataset_util
from kart.sqlalchemy.gpkg import Db_GPKG
from kart.repo import KartRepo
from kart.exceptions import (
    INVALID_OPERATION,
    NO_IMPORT_SOURCE,
    NO_TABLE,
    WORKING_COPY_OR_IMPORT_CONFLICT,
    InvalidOperation,
)

H = pytest.helpers.helpers()

# also in test_structure.py
GPKG_IMPORTS = (
    "archive,gpkg,table",
    [
        pytest.param(
            "gpkg-points", "nz-pa-points-topo-150k.gpkg", H.POINTS.LAYER, id="points"
        ),
        pytest.param(
            "gpkg-polygons",
            "nz-waca-adjustments.gpkg",
            H.POLYGONS.LAYER,
            id="polygons",
        ),
        pytest.param(
            "gpkg-au-census",
            "census2016_sdhca_ot_short.gpkg",
            "census2016_sdhca_ot_ra_short",
            id="au-ra-short",
        ),
        pytest.param("gpkg-spec", "sample1_2.gpkg", "counties", id="spec-counties"),
        pytest.param(
            "gpkg-spec", "sample1_2.gpkg", "countiestbl", id="spec-counties-table"
        ),
        pytest.param("gpkg-stringpk", "stringpk.gpkg", "stringpk", id="stringpk"),
    ],
)


@pytest.mark.slow
def test_init_import_single_table_source(data_archive_readonly, tmp_path, cli_runner):
    with data_archive_readonly("gpkg-points") as data:
        r = cli_runner.invoke(
            [
                "init",
                "--import",
                data / "nz-pa-points-topo-150k.gpkg",
                str(tmp_path / "emptydir"),
            ]
        )
        # You don't have to specify a table if there's only one.
        assert r.exit_code == 0, r
        lines = r.stdout.splitlines()
        assert len(lines) >= 2
        assert "to nz_pa_points_topo_150k/ ..." in lines[2]
        assert lines[-1] == "Creating GPKG working copy at emptydir.gpkg ..."


@pytest.mark.slow
def test_init_import_custom_message(data_archive_readonly, tmp_path, cli_runner, chdir):
    with data_archive_readonly("gpkg-points") as data:
        r = cli_runner.invoke(
            [
                "init",
                "-m",
                "Custom message",
                "--import",
                data / "nz-pa-points-topo-150k.gpkg",
                str(tmp_path / "emptydir"),
            ]
        )
        assert r.exit_code == 0, r
        with chdir(tmp_path / "emptydir"):
            r = cli_runner.invoke(["log", "-n 1"])
        assert r.exit_code == 0, r
        assert "Custom message" in r.stdout


def test_import_table_with_prompt(data_archive_readonly, tmp_path, cli_runner, chdir):
    with data_archive_readonly("gpkg-au-census") as data:
        repo_path = tmp_path / "emptydir"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0
        with chdir(repo_path):
            r = cli_runner.invoke(
                [
                    "import",
                    data / "census2016_sdhca_ot_short.gpkg",
                ],
                input="census2016_sdhca_ot_ced_short\n",
            )
            # Table was specified interactively via prompt
            assert r.exit_code == 0, r
        assert "Tables found:" in r.stdout
        assert (
            "  census2016_sdhca_ot_ced_short - census2016_sdhca_ot_ced_short"
            in r.stdout
        )
        assert "to census2016_sdhca_ot_ced_short/ ..." in r.stdout


def test_import_table_meta_overrides(
    data_archive_readonly, tmp_path, cli_runner, chdir
):
    with data_archive_readonly("gpkg-au-census") as data:
        repo_path = tmp_path / "emptydir"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0
        with chdir(repo_path):
            original_xml_metadata = '<gmd:MD_Metadata xmlns:gco="http://www.isotc211.org/2005/gco" xmlns:gmd="http://www.isotc211.org/2005/gmd" xmlns:gml="http://www.opengis.net/gml" xmlns:gts="http://www.isotc211.org/2005/gts" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns="http://www.isotc211.org/2005/gmd" />'
            table_info_json = json.dumps(
                {
                    "census2016_sdhca_ot_ced_short": {
                        "title": "test title",
                        "description": "test description",
                        "xmlMetadata": original_xml_metadata,
                    }
                }
            )
            r = cli_runner.invoke(
                [
                    "import",
                    data / "census2016_sdhca_ot_short.gpkg",
                    "census2016_sdhca_ot_ced_short",
                    "--table-info",
                    table_info_json,
                ],
            )
            assert r.exit_code == 0, r

            cli_runner.invoke(["checkout"])

            repo = KartRepo(repo_path)
            with repo.working_copy.tabular.session() as sess:
                title, description = sess.execute(
                    """
                    SELECT c.identifier, c.description
                    FROM gpkg_contents c
                    WHERE c.table_name = 'census2016_sdhca_ot_ced_short'
                    """
                ).fetchone()
                assert title == "test title"
                assert description == "test description"

                xml_metadata = sess.scalar(
                    """
                    SELECT m.metadata
                    FROM gpkg_metadata m JOIN gpkg_metadata_reference r
                    ON m.id = r.md_file_id
                    WHERE r.table_name = 'census2016_sdhca_ot_ced_short'
                    """
                )
                assert xml_metadata == original_xml_metadata


def test_import_table_with_prompt_with_no_input(
    data_archive_readonly, tmp_path, cli_runner, chdir
):
    with data_archive_readonly("gpkg-au-census") as data:
        repo_path = tmp_path / "emptydir"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0
        with chdir(repo_path):
            r = cli_runner.invoke(["import", data / "census2016_sdhca_ot_short.gpkg"])
            # Table was specified interactively via prompt
            assert r.exit_code == NO_TABLE, r
        assert "Tables found:" in r.stdout
        assert (
            "  census2016_sdhca_ot_ced_short - census2016_sdhca_ot_ced_short"
            in r.stdout
        )
        assert "No table specified" in r.stderr


def test_import_replace_existing(
    data_archive,
    tmp_path,
    cli_runner,
    chdir,
):
    with data_archive("gpkg-polygons") as data:
        repo_path = tmp_path / "emptydir"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0
        with chdir(repo_path):
            r = cli_runner.invoke(
                [
                    "import",
                    data / "nz-waca-adjustments.gpkg",
                    "nz_waca_adjustments:mytable",
                ]
            )
            assert r.exit_code == 0, r.stderr

            # Now modify the source GPKG
            with Db_GPKG.create_engine(
                data / "nz-waca-adjustments.gpkg"
            ).connect() as conn:
                conn.execute(
                    "UPDATE nz_waca_adjustments SET survey_reference = 'edited' WHERE id = 1424927"
                )

            r = cli_runner.invoke(
                [
                    "import",
                    "--replace-existing",
                    data / "nz-waca-adjustments.gpkg",
                    "nz_waca_adjustments:mytable",
                ]
            )
            assert r.exit_code == 0, r.stderr
            r = cli_runner.invoke(["show", "-o", "json"])
            assert r.exit_code == 0, r.stderr
            output = json.loads(r.stdout)
            assert output["kart.diff/v1+hexwkb"] == {
                "mytable": {
                    "feature": [
                        {
                            "-": {
                                "id": 1424927,
                                "geom": "01060000000100000001030000000100000012000000D2B47A3DAEEB65402E86A80212EF42C01D23796880EB6540D54A46E909EE42C03E7210197BEB6540B164332CEBED42C003ECE8DE70EB6540C99AB69AACED42C0916A8E626FEB654040F4DAAC9EED42C0615CA5D035EB6540F2B295FC50EB42C04AA3B89940EB6540D90F9D94DCEA42C00937B99972EB6540163FEB35F4E942C0B9103A5876EB65408D6D995DE5E942C008A85AD68FEB654069D2CB43DDE942C0D24A26924CEC6540C455AF6CB0EC42C0D21275304CEC6540E6CE3803B6EC42C018EA6B3714EC6540D17726991DEE42C00D91731C00EC65401BE20E8A9CEE42C0EBE45150F7EB6540D10F6A10D4EE42C01C6BD51EEDEB6540CD6886390AEF42C0FB975FA7EBEB6540DB85E63A0DEF42C0D2B47A3DAEEB65402E86A80212EF42C0",
                                "date_adjusted": "2011-03-25T07:30:45",
                                "survey_reference": None,
                                "adjusted_nodes": 1122,
                            },
                            "+": {
                                "id": 1424927,
                                "geom": "01060000000100000001030000000100000012000000D2B47A3DAEEB65402E86A80212EF42C01D23796880EB6540D54A46E909EE42C03E7210197BEB6540B164332CEBED42C003ECE8DE70EB6540C99AB69AACED42C0916A8E626FEB654040F4DAAC9EED42C0615CA5D035EB6540F2B295FC50EB42C04AA3B89940EB6540D90F9D94DCEA42C00937B99972EB6540163FEB35F4E942C0B9103A5876EB65408D6D995DE5E942C008A85AD68FEB654069D2CB43DDE942C0D24A26924CEC6540C455AF6CB0EC42C0D21275304CEC6540E6CE3803B6EC42C018EA6B3714EC6540D17726991DEE42C00D91731C00EC65401BE20E8A9CEE42C0EBE45150F7EB6540D10F6A10D4EE42C01C6BD51EEDEB6540CD6886390AEF42C0FB975FA7EBEB6540DB85E63A0DEF42C0D2B47A3DAEEB65402E86A80212EF42C0",
                                "date_adjusted": "2011-03-25T07:30:45",
                                "survey_reference": "edited",
                                "adjusted_nodes": 1122,
                            },
                        }
                    ]
                }
            }


def test_import_replace_existing_with_no_changes(
    data_archive,
    tmp_path,
    cli_runner,
    chdir,
):
    with data_archive("gpkg-polygons") as data:
        repo_path = tmp_path / "emptydir"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0
        with chdir(repo_path):
            r = cli_runner.invoke(
                [
                    "import",
                    data / "nz-waca-adjustments.gpkg",
                    "nz_waca_adjustments:mytable",
                ]
            )
            assert r.exit_code == 0, r.stderr

            # now import the same thing over the top (no changes)
            r = cli_runner.invoke(
                [
                    "import",
                    "--replace-existing",
                    data / "nz-waca-adjustments.gpkg",
                    "nz_waca_adjustments:mytable",
                ]
            )
            assert r.exit_code == 44, r.stderr


def test_import_replace_existing_with_compatible_schema_changes(
    data_archive,
    tmp_path,
    cli_runner,
    chdir,
):
    with data_archive("gpkg-polygons") as data:
        repo_path = tmp_path / "emptydir"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0
        with chdir(repo_path):
            r = cli_runner.invoke(
                [
                    "import",
                    data / "nz-waca-adjustments.gpkg",
                    "nz_waca_adjustments:mytable",
                ]
            )
            assert r.exit_code == 0, r.stderr

            # Now replace with a table which
            # * doesn't include the `survey_reference` column
            # * has the columns in a different order
            # * has a new column
            with Db_GPKG.create_engine(
                data / "nz-waca-adjustments.gpkg"
            ).connect() as conn:
                conn.execute(
                    """
                        CREATE TABLE IF NOT EXISTS "nz_waca_adjustments_2" (
                            "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                            "geom" MULTIPOLYGON,
                            "date_adjusted" DATETIME,
                            "adjusted_nodes" MEDIUMINT,
                            "newcolumn" TEXT
                        );
                    """
                )
                conn.execute(
                    """
                        INSERT INTO nz_waca_adjustments_2 (id, geom, date_adjusted, adjusted_nodes, newcolumn)
                            SELECT id, geom, date_adjusted, adjusted_nodes, NULL FROM nz_waca_adjustments;
                    """
                )
                conn.execute("""DROP TABLE nz_waca_adjustments;""")
                conn.execute(
                    """ALTER TABLE "nz_waca_adjustments_2" RENAME TO "nz_waca_adjustments";"""
                )

            r = cli_runner.invoke(
                [
                    "import",
                    "--replace-existing",
                    data / "nz-waca-adjustments.gpkg",
                    "nz_waca_adjustments:mytable",
                ]
            )
            assert r.exit_code == 0, r.stderr
            r = cli_runner.invoke(["show", "-o", "json"])
            assert r.exit_code == 0, r.stderr
            diff = json.loads(r.stdout)["kart.diff/v1+hexwkb"]["mytable"]

            # The schema changed, but the features didn't.
            assert diff["meta"]["schema.json"]
            assert not diff.get("feature")

            repo = KartRepo(repo_path)
            head_rs = repo.structure("HEAD")
            old_rs = repo.structure("HEAD^")
            assert head_rs.tree != old_rs.tree
            new_feature_tree = head_rs.tree / "mytable/.table-dataset/feature"
            old_feature_tree = old_rs.tree / "mytable/.table-dataset/feature"
            assert new_feature_tree == old_feature_tree


def test_import_replace_existing_with_column_renames(
    data_archive,
    tmp_path,
    cli_runner,
    chdir,
):
    with data_archive("gpkg-polygons") as data:
        repo_path = tmp_path / "emptydir"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0
        with chdir(repo_path):
            r = cli_runner.invoke(
                [
                    "import",
                    data / "nz-waca-adjustments.gpkg",
                    "nz_waca_adjustments:mytable",
                ]
            )
            assert r.exit_code == 0, r.stderr

            # Now rename
            # * doesn't include the `survey_reference` column
            # * has the columns in a different order
            # * has a new column
            with Db_GPKG.create_engine(
                data / "nz-waca-adjustments.gpkg"
            ).connect() as conn:
                conn.execute(
                    """
                    ALTER TABLE "nz_waca_adjustments" RENAME COLUMN "survey_reference" TO "renamed_survey_reference";
                    """
                )

            r = cli_runner.invoke(
                [
                    "import",
                    "--replace-existing",
                    data / "nz-waca-adjustments.gpkg",
                    "nz_waca_adjustments:mytable",
                ]
            )
            assert r.exit_code == 0, r.stderr
            r = cli_runner.invoke(["show", "-o", "json"])
            assert r.exit_code == 0, r.stderr
            diff = json.loads(r.stdout)["kart.diff/v1+hexwkb"]["mytable"]

            # The schema changed, but the features didn't.
            assert diff["meta"]["schema.json"]
            assert not diff.get("feature")

            repo = KartRepo(repo_path)
            head_rs = repo.structure("HEAD")
            old_rs = repo.structure("HEAD^")
            assert head_rs.tree != old_rs.tree
            new_feature_tree = head_rs.tree / "mytable/.table-dataset/feature"
            old_feature_tree = old_rs.tree / "mytable/.table-dataset/feature"
            assert new_feature_tree == old_feature_tree


def test_import_replace_ids(
    data_archive,
    tmp_path,
    cli_runner,
    chdir,
):
    with data_archive("gpkg-polygons") as data:
        repo_path = tmp_path / "emptydir"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0
        with chdir(repo_path):
            # initial import
            r = cli_runner.invoke(
                [
                    "import",
                    data / "nz-waca-adjustments.gpkg",
                    # import 5 features
                    "--replace-ids",
                    "2501588\n4413497\n4411733\n4408774\n4408145",
                    "nz_waca_adjustments:mytable",
                ]
            )
            assert r.exit_code == 0, r.stderr
            r = cli_runner.invoke(["show", "-o", "json"])
            assert r.exit_code == 0, r.stderr
            diff = json.loads(r.stdout)["kart.diff/v1+hexwkb"]["mytable"]
            features = diff.get("feature")
            assert len(features) == 5

            # Now change four features; two deletes and two updates
            with Db_GPKG.create_engine(
                data / "nz-waca-adjustments.gpkg"
            ).connect() as conn:
                conn.execute(
                    """
                    DELETE FROM "nz_waca_adjustments" WHERE id IN (4413497, 4411733)
                    """
                )
                conn.execute(
                    """
                    UPDATE "nz_waca_adjustments" SET adjusted_nodes = adjusted_nodes + 5
                    WHERE id IN (4408774, 4408145)
                    """
                )

            # import, but specify --replace-ids to match only one delete and one update
            r = cli_runner.invoke(
                [
                    "import",
                    "--replace-ids",
                    "4408774\n4413497",
                    data / "nz-waca-adjustments.gpkg",
                    "nz_waca_adjustments:mytable",
                ]
            )
            assert r.exit_code == 0, r.stderr
            r = cli_runner.invoke(["show", "-o", "json"])
            assert r.exit_code == 0, r.stderr
            diff = json.loads(r.stdout)["kart.diff/v1+hexwkb"]["mytable"]
            features = diff.get("feature")

            # one update and one delete were performed, and the other two changes ignored.
            assert len(features) == 2
            assert features == [
                {
                    "-": {
                        "id": 4408774,
                        "geom": "01060000000100000001030000000100000010000000C885FE1E988C6540B64D609F81CC45C046DF4EB25E8C6540C15BEAE03CCC45C0F188658E208C654079F5E0A49FCB45C05857056A318C6540B96F466883CB45C04D1F1058508C6540DAE0582152CB45C0E056EB54828C6540CD7CF3110BCB45C0D93E44E98A8C6540A55E707CFFCA45C0DE793DF38D8C654046D02963FBCA45C02B069EEB928C65404AF6BEA728CB45C05DBB9EB3978C6540C9E3D8DF5ACB45C0C1CA5CBA9C8C654081C6820293CB45C0E4A03F0E9D8C6540C072BA6C98CB45C03E3F4785A48C6540B7EB364329CC45C0A51E5844A38C65409A9F40F370CC45C0204899AE9A8C6540DAB64D0C80CC45C0C885FE1E988C6540B64D609F81CC45C0",
                        "date_adjusted": "2016-12-15T15:59:07",
                        "survey_reference": None,
                        "adjusted_nodes": 2300,
                    },
                    "+": {
                        "id": 4408774,
                        "geom": "01060000000100000001030000000100000010000000C885FE1E988C6540B64D609F81CC45C046DF4EB25E8C6540C15BEAE03CCC45C0F188658E208C654079F5E0A49FCB45C05857056A318C6540B96F466883CB45C04D1F1058508C6540DAE0582152CB45C0E056EB54828C6540CD7CF3110BCB45C0D93E44E98A8C6540A55E707CFFCA45C0DE793DF38D8C654046D02963FBCA45C02B069EEB928C65404AF6BEA728CB45C05DBB9EB3978C6540C9E3D8DF5ACB45C0C1CA5CBA9C8C654081C6820293CB45C0E4A03F0E9D8C6540C072BA6C98CB45C03E3F4785A48C6540B7EB364329CC45C0A51E5844A38C65409A9F40F370CC45C0204899AE9A8C6540DAB64D0C80CC45C0C885FE1E988C6540B64D609F81CC45C0",
                        "date_adjusted": "2016-12-15T15:59:07",
                        "survey_reference": None,
                        "adjusted_nodes": 2305,
                    },
                },
                {
                    "-": {
                        "id": 4413497,
                        "geom": "0106000000010000000103000000010000000F000000A51E5844A38C65409A9F40F370CC45C03BD400EF8E8C6540D6CC24AA13CB45C0DE793DF38D8C654046D02963FBCA45C0ACBE5F719D8C6540ED43F29FDBCA45C0E6453C0ED18C6540EDF0D7648DCA45C017E20260E58C654085B9388570CA45C04CCEFA24208D65407C735A9C7ACA45C0E4045C46208D654023A031F87CCA45C082F17D01268D6540F83908FAE7CA45C090D42C9B2B8D65406A8A6F8D50CB45C0C5E452BB2C8D654067D97F9380CB45C0A54D1AC92B8D65404EFDE10287CB45C0818F66D1208D65401F20A9CF9FCB45C06E75AA0CAC8C6540C74CFD1763CC45C0A51E5844A38C65409A9F40F370CC45C0",
                        "date_adjusted": "2016-12-16T11:10:05",
                        "survey_reference": None,
                        "adjusted_nodes": 1296,
                    }
                },
            ]


def test_init_import_table_gpkg_types(data_archive_readonly, tmp_path, cli_runner):
    with data_archive_readonly("gpkg-types") as data:
        repo_path = tmp_path / "repo"
        r = cli_runner.invoke(
            [
                "init",
                "--import",
                data / "types.gpkg",
                str(repo_path),
            ],
        )
        assert r.exit_code == 0, r.stderr

        # There's a bunch of wacky types in here, let's check them
        repo = KartRepo(repo_path)
        with repo.working_copy.tabular.session() as sess:
            table_info = [
                dict(row) for row in sess.execute("PRAGMA table_info('types');")
            ]
        assert table_info == [
            {
                "cid": 0,
                "name": "fid",
                "type": "INTEGER",
                "notnull": 1,
                "dflt_value": None,
                "pk": 1,
            },
            {
                "cid": 1,
                "name": "int16",
                "type": "SMALLINT",
                "notnull": 0,
                "dflt_value": None,
                "pk": 0,
            },
            {
                "cid": 2,
                "name": "int32",
                "type": "MEDIUMINT",
                "notnull": 0,
                "dflt_value": None,
                "pk": 0,
            },
            {
                "cid": 3,
                "name": "int64",
                "type": "INTEGER",
                "notnull": 0,
                "dflt_value": None,
                "pk": 0,
            },
            {
                "cid": 4,
                "name": "boolean",
                "type": "BOOLEAN",
                "notnull": 0,
                "dflt_value": None,
                "pk": 0,
            },
            {
                "cid": 5,
                "name": "double",
                "type": "REAL",
                "notnull": 0,
                "dflt_value": None,
                "pk": 0,
            },
            {
                "cid": 6,
                "name": "float32",
                "type": "FLOAT",
                "notnull": 0,
                "dflt_value": None,
                "pk": 0,
            },
            {
                "cid": 7,
                "name": "string",
                "type": "TEXT",
                "notnull": 0,
                "dflt_value": None,
                "pk": 0,
            },
            {
                "cid": 8,
                "name": "blob",
                "type": "BLOB",
                "notnull": 0,
                "dflt_value": None,
                "pk": 0,
            },
            {
                "cid": 9,
                "name": "date",
                "type": "DATE",
                "notnull": 0,
                "dflt_value": None,
                "pk": 0,
            },
            {
                "cid": 10,
                "name": "datetime",
                "type": "DATETIME",
                "notnull": 0,
                "dflt_value": None,
                "pk": 0,
            },
            {
                "cid": 11,
                "name": "time",
                "type": "TEXT",
                "notnull": 0,
                "dflt_value": None,
                "pk": 0,
            },
        ]


@pytest.mark.slow
@pytest.mark.parametrize(*GPKG_IMPORTS)
def test_init_import(
    archive,
    gpkg,
    table,
    data_archive,
    tmp_path,
    cli_runner,
    chdir,
):
    """Import the GeoPackage (eg. `kx-foo-layer.gpkg`) into a Kart repository."""
    with data_archive(archive) as data:
        # list tables
        repo_path = tmp_path / "repo"
        repo_path.mkdir()

        r = cli_runner.invoke(
            [
                "init",
                "--import",
                f"gpkg:{data / gpkg}",
                str(repo_path),
            ]
        )
        assert r.exit_code == 0, r
        assert (repo_path / ".kart" / "HEAD").exists()

        repo = KartRepo(repo_path)
        assert not repo.is_bare
        assert not repo.head_is_unborn

        assert repo.head.name == "refs/heads/main"
        assert repo.head.shorthand == "main"

        # has a single commit
        assert len([c for c in repo.walk(repo.head.target)]) == 1

        # working copy exists
        wc = repo_path / f"{repo_path.stem}.gpkg"
        assert wc.exists() and wc.is_file()
        print("workingcopy at", wc)

        assert repo.config["kart.repostructure.version"] == "3"
        assert repo.config["kart.workingcopy.location"] == f"{wc.name}"

        with repo.working_copy.tabular.session() as sess:
            assert H.row_count(sess, table) > 0

            wc_tree_id = sess.scalar(
                """SELECT value FROM "gpkg_kart_state" WHERE table_name='*' AND key='tree';"""
            )
            assert wc_tree_id == repo.head_tree.hex

            xml_metadata = sess.scalar(
                f"""
                SELECT m.metadata
                FROM gpkg_metadata m JOIN gpkg_metadata_reference r
                ON m.id = r.md_file_id
                WHERE r.table_name = '{table}'
                """
            )
            if table in ("nz_pa_points_topo_150k", "nz_waca_adjustments"):
                assert xml_metadata.startswith(
                    '<gmd:MD_Metadata xmlns:gco="http://www.isotc211.org/2005/gco"'
                )
            else:
                assert not xml_metadata

            srs_definition = sess.scalar(
                f"""
                SELECT srs.definition
                FROM gpkg_spatial_ref_sys srs JOIN gpkg_geometry_columns geom
                ON srs.srs_id = geom.srs_id
                WHERE geom.table_name = '{table}'
                """
            )
            if srs_definition:
                srs_definition = re.sub(r",\s*", ", ", srs_definition)
            if table == "nz_pa_points_topo_150k":
                assert srs_definition.startswith('GEOGCS["WGS 84", DATUM["WGS_1984"')
            elif table == "nz_waca_adjustments":
                assert srs_definition.startswith(
                    'GEOGCS["NZGD2000", DATUM["New_Zealand_Geodetic_Datum_2000"'
                )

            H.verify_gpkg_extent(sess, table)
        with chdir(repo_path):
            # check that we can view the commit we created
            cli_runner.invoke(["show", "-o", "json"])


def test_init_import_commit_headers(
    data_archive,
    tmp_path,
    cli_runner,
    chdir,
):
    with data_archive("gpkg-points") as data:
        repo_path = tmp_path / "repo"
        repo_path.mkdir()

        r = cli_runner.invoke(
            [
                "init",
                "--import",
                str(data / "nz-pa-points-topo-150k.gpkg"),
                str(repo_path),
            ],
            env={
                "GIT_AUTHOR_DATE": "2000-1-1T00:00:00Z",
                "GIT_AUTHOR_NAME": "author",
                "GIT_AUTHOR_EMAIL": "author@example.com",
                "GIT_COMMITTER_DATE": "2010-1-1T00:00:00Z",
                "GIT_COMMITTER_NAME": "committer",
                "GIT_COMMITTER_EMAIL": "committer@example.com",
            },
        )
        assert r.exit_code == 0, r.stderr
        assert (repo_path / ".kart" / "HEAD").exists()
        r = cli_runner.invoke(["-C", str(repo_path), "log", "-o", "json"])
        assert r.exit_code == 0, r.stderr
        log_entry = json.loads(r.stdout)[0]
        log_entry.pop("commit")
        log_entry.pop("abbrevCommit")
        assert log_entry == {
            "message": "Import from nz-pa-points-topo-150k.gpkg:nz_pa_points_topo_150k to nz_pa_points_topo_150k/",
            "refs": ["HEAD -> main"],
            "authorName": "author",
            "authorEmail": "author@example.com",
            "authorTime": "2000-01-01T00:00:00Z",
            "authorTimeOffset": "+00:00",
            "committerEmail": "committer@example.com",
            "committerName": "committer",
            "commitTime": "2010-01-01T00:00:00Z",
            "commitTimeOffset": "+00:00",
            "parents": [],
            "abbrevParents": [],
        }


def test_init_import_name_clash(data_archive, cli_runner):
    """Import the GeoPackage into a Kart repository of the same name, and checkout a working copy of the same name."""
    with data_archive("gpkg-editing") as data:
        r = cli_runner.invoke(["init", "--import", f"GPKG:editing.gpkg", "editing"])
        repo_path = data / "editing"

        assert r.exit_code == 0, r
        assert (repo_path / ".kart" / "HEAD").exists()

        repo = KartRepo(repo_path)
        assert not repo.is_bare
        assert not repo.head_is_unborn

        # working copy exists
        wc = repo_path / "editing.gpkg"
        assert wc.exists() and wc.is_file()
        print("workingcopy at", wc)

        assert repo.config["kart.repostructure.version"] == "3"
        assert repo.config["kart.workingcopy.location"] == "editing.gpkg"

        with Db_GPKG.create_engine(wc).connect() as db:
            wc_rowcount = H.row_count(db, "editing")
            assert wc_rowcount > 0

            wc_tree_id = db.execute(
                """SELECT value FROM "gpkg_kart_state" WHERE table_name='*' AND key='tree';"""
            ).fetchone()[0]
            assert wc_tree_id == repo.head_tree.hex

        # make sure we haven't stuffed up the original file
        with Db_GPKG.create_engine("editing.gpkg").connect() as dbo:
            r = dbo.execute("SELECT 1 FROM sqlite_master WHERE name='gpkg_kart_state';")
            assert not r.fetchone()
            source_rowcount = dbo.execute("SELECT COUNT(*) FROM editing;").fetchone()[0]
            assert source_rowcount == wc_rowcount


@pytest.mark.slow
def test_init_import_errors(data_archive, tmp_path, chdir, cli_runner):
    gpkg = "census2016_sdhca_ot_short.gpkg"

    with data_archive("gpkg-au-census") as data:
        repo_path = tmp_path / "repo"
        repo_path.mkdir()
        with chdir(repo_path):
            r = cli_runner.invoke(["init", "--import", f"fred:thingz"])
            assert r.exit_code == NO_IMPORT_SOURCE, r
            assert "fred:thingz' doesn't appear to be valid" in r.stderr

            r = cli_runner.invoke(["init", "--import", f"gpkg:thingz.gpkg"])
            assert r.exit_code == NO_IMPORT_SOURCE, r
            assert "Couldn't find 'thingz.gpkg'" in r.stderr

            # not empty
            (repo_path / "a.file").touch()
            r = cli_runner.invoke(
                ["init", "--import", f"gpkg:{data/gpkg}", str(repo_path)]
            )
            assert r.exit_code == INVALID_OPERATION, r
            assert "isn't empty" in r.stderr


def test_init_empty(tmp_path, cli_runner, chdir):
    """Create an empty Kart repository."""
    repo_path = tmp_path / "repo"
    repo_path.mkdir()

    # empty dir
    r = cli_runner.invoke(["init", str(repo_path)])
    assert r.exit_code == 0, r
    assert "templates not found" not in r.stderr
    assert (repo_path / ".kart" / "HEAD").exists()

    # makes dir tree
    repo_path = tmp_path / "foo" / "bar" / "wiz"
    r = cli_runner.invoke(["init", str(repo_path)])
    assert r.exit_code == 0, r
    assert (repo_path / ".kart" / "HEAD").exists()

    # current dir
    repo_path = tmp_path / "planet"
    repo_path.mkdir()
    with chdir(repo_path):
        r = cli_runner.invoke(["init"])
        assert r.exit_code == 0, r
        assert (repo_path / ".kart" / "HEAD").exists()

    # dir isn't empty
    repo_path = tmp_path / "tree"
    repo_path.mkdir()
    (repo_path / "a.file").touch()
    r = cli_runner.invoke(["init", str(repo_path)])
    assert r.exit_code == INVALID_OPERATION, r
    assert not (repo_path / ".kart" / "HEAD").exists()

    # current dir isn't empty
    with chdir(repo_path):
        r = cli_runner.invoke(["init"])
        assert r.exit_code == INVALID_OPERATION, r
        assert not (repo_path / ".kart" / "HEAD").exists()


@pytest.mark.slow
def test_init_import_alt_names(data_archive, tmp_path, cli_runner, chdir):
    """Import the GeoPackage (eg. `kx-foo-layer.gpkg`) into a Kart repository."""
    repo_path = tmp_path / "repo"
    repo_path.mkdir()

    gpkg_path = repo_path / "wc.gpkg"

    r = cli_runner.invoke(["init", str(repo_path), f"--workingcopy-path={gpkg_path}"])
    assert r.exit_code == 0, r

    ARCHIVE_PATHS = (
        (
            "gpkg-points",
            "nz-pa-points-topo-150k.gpkg",
            "nz_pa_points_topo_150k",
            "pa_sites",
        ),
        (
            "gpkg-polygons",
            "nz-waca-adjustments.gpkg",
            "nz_waca_adjustments",
            "misc/waca",
        ),
        (
            "gpkg-polygons",
            "nz-waca-adjustments.gpkg",
            "nz_waca_adjustments",
            "other/waca2",
        ),
    )

    for archive, source_gpkg, source_table, import_path in ARCHIVE_PATHS:
        with data_archive(archive) as source_path:
            with chdir(repo_path):
                r = cli_runner.invoke(
                    [
                        "import",
                        f"{source_path / source_gpkg}",
                        f"{source_table}:{import_path}",
                    ]
                )
                assert r.exit_code == 0, r

    with chdir(repo_path):
        # working copy exists
        with Db_GPKG.create_engine("wc.gpkg").connect() as conn:
            expected_tables = set(a[3].replace("/", "__") for a in ARCHIVE_PATHS)
            r = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
            db_tables = set(row[0] for row in r)
            assert expected_tables <= db_tables

            for gpkg_t in (
                "gpkg_contents",
                "gpkg_geometry_columns",
                "gpkg_metadata_reference",
            ):
                r = conn.execute(f"SELECT DISTINCT table_name FROM {gpkg_t};")
                table_list = set(row[0] for row in r)
                assert expected_tables >= table_list, gpkg_t

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == []


@pytest.mark.slow
def test_init_import_home_resolve(
    data_archive, tmp_path, cli_runner, chdir, monkeypatch, git_user_config
):
    """Import from a ~-specified gpkg path"""
    repo_path = tmp_path / "repo"
    repo_path.mkdir()

    r = cli_runner.invoke(["init", str(repo_path)])
    assert r.exit_code == 0, r

    with data_archive("gpkg-points") as source_path:
        with chdir(repo_path):
            monkeypatch.setenv("HOME", str(source_path))
            monkeypatch.setenv("USERPROFILE", str(source_path))

            # make sure we have a .gitconfig file in $HOME,
            # otherwise kart can't find the user information for the commit
            orig_home = git_user_config[2]
            shutil.copy2(orig_home / ".gitconfig", source_path)

            r = cli_runner.invoke(
                [
                    "import",
                    "GPKG:~/nz-pa-points-topo-150k.gpkg",
                    "nz_pa_points_topo_150k",
                ]
            )
            assert r.exit_code == 0, r


@pytest.mark.slow
def test_import_existing_wc(
    data_archive,
    data_working_copy,
    cli_runner,
    insert,
    tmp_path,
    request,
    chdir,
):
    """Import a new dataset into a repo with an existing working copy. Dataset should get checked out"""
    with data_working_copy("points") as (repo_path, wcdb):
        with data_archive("gpkg-polygons") as source_path, chdir(repo_path):
            # create a new directory for the source
            import_dir = tmp_path / "import"
            import_dir.mkdir()
            # copy the source to the new directory
            shutil.copy2(
                source_path / "nz-waca-adjustments.gpkg",
                import_dir / "nz-waca-adjustments.gpkg",
            )
            r = cli_runner.invoke(
                [
                    "import",
                    f"{str(import_dir / 'nz-waca-adjustments.gpkg')}",
                    H.POLYGONS.LAYER,
                ]
            )
            print(f"Source path: {source_path}")
            print(f"Import path: {import_dir}")
            assert r.exit_code == 0, r.stderr

        repo = KartRepo(repo_path)
        table_wc = repo.working_copy.tabular
        with table_wc.session() as sess:
            assert H.row_count(sess, "nz_waca_adjustments") > 0
        assert table_wc.get_tree_id() == repo.head_tree.id.hex

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines()[-1] == "Nothing to commit, working copy clean"

        with table_wc.session() as sess:
            r = sess.execute(
                "DELETE FROM nz_waca_adjustments WHERE rowid IN (SELECT rowid FROM nz_waca_adjustments ORDER BY id LIMIT 10);"
            )
            assert r.rowcount == 10

        with data_archive("gpkg-polygons") as source_path, chdir(repo_path):
            r = cli_runner.invoke(
                [
                    "import",
                    f"{source_path / 'nz-waca-adjustments.gpkg'}",
                    f"{H.POLYGONS.LAYER}:waca2",
                ]
            )
            assert r.exit_code == 0, r

        with table_wc.session() as sess:
            assert H.row_count(sess, "waca2") > 0
        assert table_wc.get_tree_id() == repo.head_tree.id.hex

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines()[-3:] == [
            "  nz_waca_adjustments:",
            "    feature:",
            "      10 deletes",
        ]


def test_init_import_detached_head(data_working_copy, data_archive, chdir, cli_runner):
    with data_working_copy("points") as (repo_path, wcdb):
        with data_archive("gpkg-polygons") as source_path, chdir(repo_path):
            r = cli_runner.invoke(["checkout", "HEAD^"])
            repo = KartRepo(repo_path)
            assert repo.head_is_detached
            initial_head = repo.head.target.hex

            r = cli_runner.invoke(
                [
                    "import",
                    f"{source_path / 'nz-waca-adjustments.gpkg'}",
                    H.POLYGONS.LAYER,
                ]
            )
            assert r.exit_code == 0, r
            assert repo.head_is_detached
            assert repo.head.target.hex != initial_head
            assert repo.revparse_single("HEAD^").hex == initial_head


def test_import_list_formats(data_archive_readonly, cli_runner):
    with data_archive_readonly("points") as _:
        r = cli_runner.invoke(["import", "--list-formats"])
        assert r.exit_code == 0, r.stderr
        prefixes = [l.split(":")[0].strip() for l in r.stdout.splitlines()]
        assert prefixes == [
            "GeoPackage",
            "PostgreSQL",
            "Microsoft SQL Server",
            "MySQL",
            "ESRI Shapefile",
            "LAS (LASer)",
            "GeoTIFF",
        ]


@pytest.mark.parametrize(
    "names,is_okay",
    [
        pytest.param(("",), False, id="empty-string"),
        pytest.param(("a:b",), False, id="special-ascii-chars"),
        pytest.param(("a\nb",), False, id="ascii-control-chars"),
        pytest.param(("a/b/c",), True, id="slash-separated"),
        pytest.param(("/a",), False, id="leading-slash"),
        pytest.param(("a/b/",), False, id="trailing-slash"),
        pytest.param(("a//b",), False, id="empty-component"),
        pytest.param(("1a",), True, id="leading-numeral"),
        pytest.param(("a1",), True, id="trailing-numeral"),
        pytest.param(("_1",), True, id="leading-underscore"),
        pytest.param((".a",), False, id="leading-dot"),
        pytest.param(("a.",), False, id="trailing-dot"),
        pytest.param(("a.b",), True, id="contains-dot"),
        pytest.param((" b",), True, id="leading-space"),
        pytest.param(("a ",), False, id="trailing-space"),
        pytest.param(("a b",), True, id="contains-space"),
        pytest.param(("a/ b",), True, id="component-leading-space"),
        pytest.param(("a /b",), False, id="component-trailing-space"),
        pytest.param(("COM1",), False, id="windows-reserved-filename"),
        pytest.param(("com1",), False, id="windows-reserved-filename-ignoring-case"),
        pytest.param(("ⅶ",), True, id="leading-numeral-unicode"),
        pytest.param(("Ἢⅶ",), True, id="trailing-numeral-unicode"),
        pytest.param(("a", "a"), False, id="identical-names"),
        pytest.param(("a", "A"), False, id="identical-names-ignoring-case"),
        pytest.param(("a", "b"), True, id="disparate-names"),
        pytest.param(
            ("K", "\u212a"), False, id="identical-names-ignoring-case-unicode"
        ),
    ],
)
def test_validate_dataset_paths(names, is_okay):
    if is_okay:
        dataset_util.validate_dataset_paths(names)
    else:
        with pytest.raises(InvalidOperation):
            dataset_util.validate_dataset_paths(names)


def test_import_bad_dataset_path(data_archive, data_archive_readonly, cli_runner):
    with data_archive_readonly("gpkg-polygons") as data:
        with data_archive("points"):
            # importing as 'a.' isn't allowed. See test_validate_dataset_paths for full coverage of these cases.
            r = cli_runner.invoke(
                [
                    "import",
                    str(data / "nz-waca-adjustments.gpkg"),
                    f"{H.POLYGONS.LAYER}:a.",
                ]
            )
            assert r.exit_code == 20, r.stderr

            # importing a path that's already in the repo (after case folding) isn't allowed, without --replace-existing
            r = cli_runner.invoke(
                [
                    "import",
                    str(data / "nz-waca-adjustments.gpkg"),
                    f"{H.POLYGONS.LAYER}:NZ_PA_POINTS_TOPO_150K",
                ]
            )
            assert r.exit_code == WORKING_COPY_OR_IMPORT_CONFLICT, r.stderr


def test_import_backslash_in_dataset_path(
    data_archive, data_archive_readonly, cli_runner
):
    with data_archive_readonly("gpkg-polygons") as data:
        with data_archive("points"):
            # See test_validate_dataset_paths for full coverage of these cases.
            r = cli_runner.invoke(
                [
                    "import",
                    str(data / "nz-waca-adjustments.gpkg"),
                    f"{H.POLYGONS.LAYER}:a\\b\\c",
                ]
            )
            assert r.exit_code == 0, r.stderr

            r = cli_runner.invoke(["data", "ls"])
            assert r.exit_code == 0, r.stderr
            assert "a/b/c" in r.stdout.splitlines()


def test_replace_existing_with_primary_key_change(
    data_archive, data_archive_readonly, cli_runner
):
    with data_archive_readonly("gpkg-polygons") as data:
        with data_archive("string-pks"):
            r = cli_runner.invoke(
                [
                    "import",
                    str(data / "nz-waca-adjustments.gpkg"),
                    "--replace-existing",
                    "--replace-ids=1,2,3",
                ]
            )
            assert r.exit_code == INVALID_OPERATION
            assert (
                "--replace-ids is not supported when the primary key column is being changed"
                in r.stderr
            )

            r = cli_runner.invoke(
                [
                    "import",
                    str(data / "nz-waca-adjustments.gpkg"),
                    "--replace-existing",
                ]
            )
            assert r.exit_code == 0, r.stderr

            r = cli_runner.invoke(["show"])
            assert r.exit_code == 0, r.stderr
            lines = r.stdout.splitlines()
            assert (
                sum(
                    1 for l in lines if l.startswith("+++ nz_waca_adjustments:feature:")
                )
                == 228
            )
            assert (
                sum(
                    1
                    for l in lines
                    if l.startswith("--- nz_waca_adjustments:feature:POLY")
                )
                == 228
            )

            r = cli_runner.invoke(
                ["meta", "get", "nz_waca_adjustments", "schema.json", "-o", "json"]
            )
            assert r.exit_code == 0, r.stderr
            jdict = json.loads(r.stdout)
            col0 = jdict["nz_waca_adjustments"]["schema.json"][0]
            assert col0["primaryKeyIndex"] == 0
            assert col0["dataType"] == "integer"


def test_init_import_with_no_crs(
    data_archive,
    tmp_path,
    cli_runner,
):
    repo_path = tmp_path / "repo"
    repo_path.mkdir()

    with data_archive("gpkg-points-no-crs") as data:
        r = cli_runner.invoke(
            [
                "init",
                "--bare",
                "--import",
                f"gpkg:{data / 'nz-pa-points-topo-150k.gpkg'}",
                str(repo_path),
            ]
        )
        assert r.exit_code == 0, r.stderr
