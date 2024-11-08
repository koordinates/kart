import click
import os
from pathlib import Path
import re
from kart import subprocess_util as subprocess


def get_version():
    import kart

    with open(Path(kart.package_data_path) / "VERSION") as version_file:
        return version_file.read().strip()


def get_version_tuple():
    return tuple(get_version().split("."))


def get_version_info_text():
    import osgeo
    import psycopg2
    import pysqlite3
    import pygit2
    import sqlalchemy
    from kart.sqlalchemy.gpkg import Db_GPKG

    output = [f"Kart v{get_version()}, Copyright (c) Kart Contributors"]

    git_version = (
        subprocess.check_output(["git", "--version"])
        .decode("ascii")
        .strip()
        .split()[-1]
    )

    gitlfs_version = re.match(
        r"git-lfs/([^ ]+) \(",
        subprocess.check_output(["git-lfs", "version"], text=True),
    ).group(1)

    pdal_version = (
        subprocess.check_output(["pdal", "--version"])
        .decode("ascii")
        .strip()
        .split()[2]
    )

    engine = Db_GPKG.create_engine(":memory:")
    with engine.connect() as conn:
        spatialite_version = conn.scalar("SELECT spatialite_version();")

    pq_version = psycopg2.__libpq_version__
    pq_version = "{}.{}.{}".format(
        *[int(k) for k in re.findall(r"\d\d", str(psycopg2.__libpq_version__))]
    )

    proj_version = "{}.{}.{}".format(
        osgeo.osr.GetPROJVersionMajor(),
        osgeo.osr.GetPROJVersionMinor(),
        osgeo.osr.GetPROJVersionMicro(),
    )

    output += [
        f"» GDAL v{osgeo._gdal.__version__}; "
        f"PROJ v{proj_version}; "
        f"PDAL v{pdal_version}",
        f"» PyGit2 v{pygit2.__version__}; "
        f"Libgit2 v{pygit2.LIBGIT2_VERSION}; "
        f"Git v{git_version}; "
        f"Git LFS v{gitlfs_version}",
        f"» SQLAlchemy v{sqlalchemy.__version__}; "
        f"pysqlite3 v{pysqlite3.version}/v{pysqlite3.sqlite_version}; "
        f"SpatiaLite v{spatialite_version}; "
        f"Libpq v{pq_version}"
    ]

    # report on whether this was run through helper mode
    helper_pid = os.environ.get("KART_HELPER_PID")
    if helper_pid:
        output.append(f"Executed via helper, SID={os.getsid(0)} PID={helper_pid}")

    return output
