# -*- coding: utf-8 -*-
import re
from platform import system
from typing import List

from setuptools import Extension, find_packages, setup

VERSION = open("../VERSION").read().strip()
README = open("README.md").read()


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

extra_requirements = load_extra(["cache", "storages", "fastapi"])

lru_module = Extension(
    "nucliadb_utils.cache.lru", sources=["nucliadb_utils/cache/lru.c"]
)


extensions = []
if system() != "Windows":
    extensions.append(lru_module)


setup(
    name="nucliadb_utils",
    version=VERSION,
    long_description=README,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3.7",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    url="https://nuclia.com",
    license="BSD",
    setup_requires=[
        "pytest-runner",
    ],
    zip_safe=True,
    include_package_data=True,
    ext_modules=extensions,
    packages=find_packages(),
    install_requires=requirements,
    extras_require=extra_requirements,
)
