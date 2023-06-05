import json
from pathlib import Path
import tempfile

from kart import is_windows
from kart import subprocess_util as subprocess


def pdal_execute_pipeline(pipeline, *, env_overrides=None):
    """
    Executes the given PDAL pipeline. Should be a list of dicts/strings, each representing a PDAL stage.
    Returns a list of metadata output from each stage.
    """

    env = subprocess.tool_environment()
    # NOTE: Kart itself doesn't currently use env_overrides, but don't remove it.
    # PDAL uses environment variables for various purposes, and this is helpful for `kart ext-run` scripts.
    env.update(env_overrides or {})

    if is_windows:
        # On windows we can't keep the metadata file open while pdal writes to it:
        with tempfile.NamedTemporaryFile(delete=False) as f_metadata:
            metadata_path = Path(f_metadata.name)

        subprocess.run(
            ["pdal", "pipeline", "--stdin", f"--metadata={metadata_path}"],
            check=True,
            input=json.dumps(pipeline),
            encoding="utf-8",
            capture_output=True,
            env=env,
        )

        with metadata_path.open(encoding="utf-8") as f:
            metadata = json.load(f)

        metadata_path.unlink()
        return metadata["stages"]

    else:
        with tempfile.NamedTemporaryFile() as f_metadata:

            subprocess.run(
                ["pdal", "pipeline", "--stdin", f"--metadata={f_metadata.name}"],
                check=True,
                input=json.dumps(pipeline),
                encoding="utf-8",
                capture_output=True,
                env=env,
            )
            f_metadata.seek(0)
            metadata = json.load(f_metadata)
            return metadata["stages"]
