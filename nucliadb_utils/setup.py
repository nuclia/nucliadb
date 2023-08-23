# -*- coding: utf-8 -*-
import re
from pathlib import Path
from typing import List

from setuptools import find_packages, setup

_dir = Path(__file__).resolve().parent
VERSION = _dir.parent.joinpath("VERSION").open().read().strip()
README = _dir.joinpath("README.md").open().read()


def load_reqs(filename):
    with open(filename) as reqs_file:
        return [
            # pin nucliadb-xxx to the same version as nucliadb
            line.strip() + f"=={VERSION}"
            if line.startswith("nucliadb-") and "=" not in line
            else line.strip()
            for line in reqs_file.readlines()
            if not (
                re.match(r"\s*#", line) or re.match("-e", line) or re.match("-r", line)
            )
        ]


def load_extra(sections: List[str]):
    result = {}
    for section in sections:
        with open(f"requirements-{section}.txt") as reqs_file:
            result[section] = [
                line
                for line in reqs_file.readlines()
                if not (
                    re.match(r"\s*#", line)  # noqa
                    or re.match("-e", line)
                    or re.match("-r", line)
                )
            ]
    return result


requirements = load_reqs("requirements.txt")

extra_requirements = load_extra(["cache", "storages", "fastapi", "postgres"])

setup(
    name="nucliadb_utils",
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
    package_data={"": ["*.txt", "*.md"], "nucliadb_utils": ["py.typed"]},
    packages=find_packages(),
    install_requires=requirements,
    extras_require=extra_requirements,
)
