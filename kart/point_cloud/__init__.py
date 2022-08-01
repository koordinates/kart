import json
import subprocess
import tempfile

from kart.cli_util import tool_environment


def pdal_execute_pipeline(pipeline):
    with tempfile.NamedTemporaryFile() as f_metadata:
        env = tool_environment()
        # Until we incorporate a vendored PDAL into our build, it might be built against a libproj that
        # uses a different proj database than the one we're vendoring.
        # So we carefully don't tell it where to look for the proj database.
        env.pop("PROJ_LIB", None)
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
