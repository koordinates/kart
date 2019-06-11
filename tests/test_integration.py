import subprocess

import pytest  # noqa

import pygit2

""" Simple integration/E2E tests """

POINTS_LAYER = "nz_pa_points_topo_150k"

POINTS_INSERT = f"""
    INSERT INTO {POINTS_LAYER}
                    (fid, geom, t50_fid, name_ascii, macronated, name)
                VALUES
                    (:fid, AsGPB(GeomFromEWKT(:geom)), :t50_fid, :name_ascii, :macronated, :name);
"""
POINTS_RECORD = {
    'fid': 9999,
    'geom': 'SRID=4326;POINT(0 0)',
    't50_fid': 9999999,
    'name_ascii': 'Te Motu-a-kore',
    'macronated': False,
    'name': 'Te Motu-a-kore',
}


def test_import_geopackage(data_archive, tmp_path, cli_runner):
    """ Import the GeoPackage (eg. `kx-foo-layer.gpkg`) into a kxgit repository. """
    with data_archive("gpkg-points") as data:
        repo_path = tmp_path / "data.git"
        r = cli_runner.invoke(
            [
                f"--repo={repo_path}",
                "import-gpkg",
                data / "nz-pa-points-topo-150k.gpkg",
                POINTS_LAYER,
            ]
        )
        assert r.exit_code == 0, r
        assert (repo_path / "HEAD").exists()


def test_checkout_workingcopy(data_archive, tmp_path, cli_runner, geopackage):
    """ Checkout a working copy to edit """
    with data_archive("points.git"):
        wc = tmp_path / "data.gpkg"
        r = cli_runner.invoke(
            ["checkout", f"--layer={POINTS_LAYER}", f"--working-copy={wc}"]
        )
        assert r.exit_code == 0, r

    assert wc.exists()
    db = geopackage(wc)
    nrows = db.execute(f"SELECT COUNT(*) FROM {POINTS_LAYER};").fetchone()[0]
    assert nrows > 0


def test_diff(data_working_copy, geopackage, cli_runner):
    """ diff the working copy against the repository (no index!) """
    with data_working_copy("points.git") as (repo, wc):
        db = geopackage(wc)
        with db:
            db.execute(POINTS_INSERT, POINTS_RECORD)

        r = cli_runner.invoke(['diff'])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "+++ {new feature}",
            "+                                      fid = 9999",
            "+                                     geom = POINT(...)",
            "+                                  t50_fid = 9999999",
            "+                               name_ascii = Te Motu-a-kore",
            "+                               macronated = 0",
            "+                                     name = Te Motu-a-kore",
        ]


def test_commit(data_working_copy, geopackage, cli_runner):
    """ commit outstanding changes from the working copy """
    with data_working_copy("points.git") as (repo, wc):
        db = geopackage(wc)
        with db:
            db.execute(POINTS_INSERT, POINTS_RECORD)

        r = cli_runner.invoke(['commit', '-m', 'test-commit-1'])
        assert r.exit_code == 0, r
        commit_id = r.stdout.splitlines()[-1].split(": ")[1]
        print("commit:", commit_id)

        r = pygit2.Repository(str(repo))
        assert str(r.head.target) == commit_id


def test_log(data_archive, cli_runner):
    """ review commit history """
    with data_archive("points.git"):
        r = cli_runner.invoke(['log'])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "commit edd5a4b02a7d2ce608f1839eea5e3a8ddb874e00",
            "Author: Robert Coup <robert@coup.net.nz>",
            "Date:   Tue Jun 11 12:03:58 2019 +0100",
            "",
            "    Import from nz-pa-points-topo-150k.gpkg",
        ]


def test_push(data_archive, tmp_path, cli_runner):
    with data_archive("points.git") as repo:
        subprocess.run(['git', 'init', '--bare', tmp_path], check=True)
        subprocess.run(['git', 'remote', 'add', 'myremote', tmp_path], check=True)

        r = cli_runner.invoke(['push', '--set-upstream', 'myremote', 'master'])
        assert r.exit_code == 0, r


# TODO:
# * `kxgit branch` & `kxgit checkout -b` branch management
# * `kxgit fetch` fetch upstream changes.
# * `kxgit merge` merge. Supports `--ff`/`--no-ff`/`--ff-only` from one merge source.
# * `git reset --soft {commitish}`
# * `git tag ...`
