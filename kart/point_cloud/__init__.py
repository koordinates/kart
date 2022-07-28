import json
import subprocess
import tempfile


def pdal_execute_pipeline(pipeline):
    with tempfile.NamedTemporaryFile() as f_metadata:
        subprocess.run(
            ["pdal", "pipeline", "--stdin", f"--metadata={f_metadata.name}"],
            check=True,
            input=json.dumps(pipeline),
            encoding="utf-8",
            capture_output=True,
        )
        f_metadata.seek(0)
        metadata = json.load(f_metadata)
        return metadata["stages"]
