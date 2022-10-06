# -*- coding: utf-8 -*-
import re
from platform import system
from typing import List

from setuptools import Extension, find_packages, setup

with open("README.md") as f:
    readme = f.read()


def build_readme():
    readme = ""
    with open("README.md") as f:
        readme += f.read()

    readme += "\n"

    with open("CHANGELOG.rst") as f:
        readme += f.read()

    return readme


def load_reqs(filename):
    with open(filename) as reqs_file:
        return [
            line
            for line in reqs_file.readlines()
            if not (
                re.match(r"\s*#", line)  # noqa
                or re.match("-e", line)
                or re.match("-r", line)
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
    version=open("VERSION").read().strip(),
    long_description=build_readme(),
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
