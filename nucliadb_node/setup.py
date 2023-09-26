import re
from pathlib import Path

from setuptools import find_packages, setup

_dir = Path(__file__).resolve().parent
VERSION = _dir.parent.joinpath("VERSION").open().read().strip()
README = _dir.joinpath("README.md").open().read()


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
    name="nucliadb_node",
    version=VERSION,
    long_description=README,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    url="https://nuclia.com",
    python_requires=">=3.9, <4",
    license="BSD",
    setup_requires=[
        "pytest-runner",
    ],
    zip_safe=True,
    include_package_data=True,
    packages=find_packages(),
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "node_sidecar = nucliadb_node.app:run",
        ]
    },
)
