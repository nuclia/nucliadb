# -*- coding: utf-8 -*-
import re

from setuptools import find_packages, setup


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
    name="nucliadb_writer",
    version=open("VERSION").read().strip(),
    long_description=(open("README.md").read() + "\n" + open("CHANGELOG").read()),
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
    entry_points={
        "console_scripts": [
            "extract-openapi = nucliadb_writer.openapi:command_extract_openapi",
            "writer-metrics = nucliadb_writer.run:run_with_metrics",
        ]
    },
)
