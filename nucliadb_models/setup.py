# -*- coding: utf-8 -*-
import re

from setuptools import find_packages, setup

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


requirements = load_reqs("requirements.txt")


setup(
    name="nucliadb_models",
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
    packages=find_packages(),
    install_requires=requirements,
)
