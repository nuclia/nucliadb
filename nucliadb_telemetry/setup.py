import re
from pathlib import Path

from setuptools import find_packages, setup  # type: ignore

_dir = Path(__file__).resolve().parent
VERSION = _dir.parent.joinpath("VERSION").open().read().strip()
README = open("README.md").read()


def load_reqs(filename):
    with open(filename) as reqs_file:
        return [
            line.strip()
            for line in reqs_file.readlines()
            if not (
                re.match(r"\s*#", line) or re.match("-e", line) or re.match("-r", line)
            )
        ]


requirements = load_reqs("requirements.txt")

setup(
    name="nucliadb_telemetry",
    version=VERSION,
    author="nucliadb Authors",
    author_email="nucliadb@nuclia.com",
    description="NucliaDB Telemetry Library Python process",
    long_description=README,
    long_description_content_type="text/markdown",
    license="MIT",
    url="https://github.com/nuclia/nucliadb",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)",
        "Framework :: AsyncIO",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: System :: Monitoring",
    ],
    python_requires=">=3.9",
    include_package_data=True,
    package_data={"": ["*.txt", "*.md"], "nucliadb_telemetry": ["py.typed"]},
    packages=find_packages(),
    install_requires=requirements,
)
