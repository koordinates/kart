import json

from kart import subprocess_util as subprocess


def pdal_execute_pipeline(pipeline, *, env_overrides=None):
    """
    Executes the given PDAL pipeline. Should be a list of dicts/strings, each representing a PDAL stage.
    Returns a list of metadata output from each stage.
    """

    # NOTE: Kart itself doesn't currently use env_overrides, but don't remove it.
    # PDAL uses environment variables for various purposes, and this is helpful for `kart ext-run` scripts.

    output = subprocess.check_output(
        ["pdal", "pipeline", "--stdin", "--metadata=STDOUT"],
        input=json.dumps(pipeline),
        encoding="utf-8",
        env_overrides=env_overrides,
    )

    metadata = json.loads(output)

    return metadata["stages"]
