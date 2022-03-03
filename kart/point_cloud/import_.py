import json
from pathlib import Path
import sys

import click

from kart.exceptions import (
    InvalidOperation,
    NotFound,
    NO_IMPORT_SOURCE,
    INVALID_FILE_FORMAT,
)
from kart.output_util import format_wkt_for_output


@click.command("point-cloud-import", hidden=True)
@click.pass_context
@click.argument("sources", metavar="SOURCES", nargs=-1, required=True)
def point_cloud_import(ctx, sources):
    """
    Experimental command for importing point cloud datasets. Work-in-progress.
    Will eventually be merged with the main `import` command.

    SOURCES should be one or more LAZ or LAS files (or wildcards that match multiple LAZ or LAS files).
    """
    import pdal

    for source in sources:
        if not (Path() / source).is_file():
            raise NotFound(f"No data found at {source}", exit_code=NO_IMPORT_SOURCE)

    version_set = ListBasedSet()
    copc_version_set = ListBasedSet()
    pdrf_set = ListBasedSet()
    crs_set = ListBasedSet()

    for source in sources:
        click.echo(f"Checking {source}...          \r", nl=False)
        config = [
            {
                "type": "readers.las",
                "filename": source,
                "count": 0,  # Don't read any individual points.
            }
        ]
        pipeline = pdal.Pipeline(json.dumps(config))
        try:
            pipeline.execute()
        except RuntimeError:
            raise InvalidOperation(
                f"Error reading {source}", exit_code=INVALID_FILE_FORMAT
            )

        info = json.loads(pipeline.metadata)["metadata"]["readers.las"]

        version = f"{info['major_version']}.{info['minor_version']}"
        version_set.add(version)
        if len(version_set) > 1:
            raise _non_homogenous_error("ersion", version_set)

        copc_version_set.add(get_copc_version(info))
        if len(copc_version_set) > 1:
            raise _non_homogenous_error("COPC version", copc_version_set)

        pdrf_set.add(info["dataformat_id"])
        if len(pdrf_set) > 1:
            raise _non_homogenous_error("Point Data Record Format", pdrf_set)

        crs_set.add(info["srs"]["wkt"])
        if len(crs_set) > 1:
            raise _non_homogenous_error(
                "CRS",
                "\n vs \n".join(
                    (format_wkt_for_output(wkt, sys.stderr) for wkt in crs_set)
                ),
            )

    click.secho("\nVersion:", bold=True)
    click.echo(version_set[0])

    click.secho("\nCOPC Version:", bold=True)
    click.echo(copc_version_set[0])

    click.secho("\nPoint Data Record Format:", bold=True)
    click.echo(pdrf_set[0])

    click.secho("\nCRS:", bold=True)
    click.echo(format_wkt_for_output(crs_set[0], sys.stdout))

    # TODO - actually import these files.


# The COPC version number we use for any LAZ / LAS file that is not actually COPC.
NOT_COPC = "NOT COPC"


def get_copc_version(info):
    vlr_0 = info.get("vlr_0")
    if vlr_0:
        user_id = vlr_0.get("user_id")
        if user_id == "copc":
            return vlr_0.get("record_id")
    return NOT_COPC


def _non_homogenous_error(attribute_name, detail):
    if not isinstance(detail, str):
        detail = " vs ".join(str(d) for d in detail)

    click.echo()  # Go to next line to get past the progress output.
    click.echo("Only the import of homogenous datasets is supported.", err=True)
    click.echo(f"The input files have more than one {attribute_name}:", err=True)
    click.echo(detail, err=True)
    raise InvalidOperation(
        "Non-homogenous dataset supplied", exit_code=INVALID_FILE_FORMAT
    )


class ListBasedSet:
    """
    A basic set that doesn't use hashing, so it can contain dicts.
    Very inefficient for lots of elements, perfect for one or two elements.
    """

    def __init__(self):
        self.list = []

    def add(self, element):
        if element not in self.list:
            self.list.append(element)

    def __contains__(self, element):
        return element in self.list

    def __len__(self):
        return len(self.list)

    def __iter__(self):
        return iter(self.list)

    def __getitem__(self, key):
        return self.list[key]
