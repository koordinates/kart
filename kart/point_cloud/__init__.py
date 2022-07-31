import json
import subprocess
import tempfile

from .cli_util import tool_environment


def pdal_execute_pipeline(pipeline):
    with tempfile.NamedTemporaryFile() as f_metadata:
        subprocess.run(
            ["pdal", "pipeline", "--stdin", f"--metadata={f_metadata.name}"],
            check=True,
            input=json.dumps(pipeline),
            encoding="utf-8",
            capture_output=True,
            env=tool_environment(),
        )
        f_metadata.seek(0)
        metadata = json.load(f_metadata)
        return metadata["stages"]
