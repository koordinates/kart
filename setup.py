import os
from setuptools import setup, find_packages

with open(os.path.join("sno", "VERSION")) as version_file:
    version = version_file.read().strip()

setup(
    name="kart",
    version=version,
    description="Distributed version-control for datasets",
    url="http://github.com/koordinates/kart",
    author="Koordinates Limited & Kart Contributors",
    author_email="support@koordinates.com",
    license="GPLv2 with linking exception",
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    entry_points={
        "console_scripts": [
            "kart = sno.cli:entrypoint",
            "sno = sno.cli:entrypoint",
        ],
    },
)
