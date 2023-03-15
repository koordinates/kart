from .fixtures import requires_gdal_info  # noqa


def test_working_copy_edit(
    cli_runner, data_archive, requires_gdal_info, requires_git_lfs
):
    with data_archive("raster/aerial.tgz") as repo_path:
        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 0

        from osgeo import gdal

        # Drop the 4th band:
        translate_options = gdal.TranslateOptions(bandList=[1, 2, 3])

        gdal.Translate(
            str(repo_path / "aerial/aerial.new.tif"),
            str(repo_path / "aerial/aerial.tif"),
            options=translate_options,
        )

        (repo_path / "aerial/aerial.tif").unlink()
        (repo_path / "aerial/aerial.new.tif").rename(repo_path / "aerial/aerial.tif")

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0
        assert r.stdout.splitlines() == [
            "On branch main",
            "",
            "Changes in working copy:",
            '  (use "kart commit" to commit)',
            '  (use "kart restore" to discard changes)',
            "",
            "  aerial:",
            "    meta:",
            "      1 updates",
            "    tile:",
            "      1 updates",
        ]

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0
        # NOTE - this test might prove brittle - if so, we can relax the hashes.
        assert r.stdout.splitlines() == [
            "--- aerial:meta:schema.json",
            "+++ aerial:meta:schema.json",
            "  [",
            "    {",
            '      "dataType": "integer",',
            '      "size": 8,',
            '      "interpretation": "red",',
            '      "unsigned": true',
            "    },",
            "    {",
            '      "dataType": "integer",',
            '      "size": 8,',
            '      "interpretation": "green",',
            '      "unsigned": true',
            "    },",
            "    {",
            '      "dataType": "integer",',
            '      "size": 8,',
            '      "interpretation": "blue",',
            '      "unsigned": true',
            "    },",
            "-   {",
            '-     "dataType": "integer",',
            '-     "size": 8,',
            '-     "interpretation": "alpha",',
            '-     "unsigned": true',
            "-   },",
            "  ]",
            "--- aerial:tile:aerial",
            "+++ aerial:tile:aerial",
            "-                                      oid = sha256:e6cbc8210f9cae3c8b72985e553e97af51fb9c20d17f5a06b7579943fed57b2c",
            "+                                      oid = sha256:60d8c02dbff57aaebd5eccd51f7cdf0f0234d6507591a122f1a683817d8f59e3",
            "-                                     size = 516216",
            "+                                     size = 533940",
        ]
