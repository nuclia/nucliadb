# -*- coding: utf-8 -*-
import re

from setuptools import find_packages, setup


def load_reqs(filename):
    with open(filename) as reqs_file:
        return [
            line
            for line in reqs_file.readlines()
            if not (
                re.match(r"\s*#", line) or re.match("-e", line) or re.match("-r", line)
            )
        ]


requirements = load_reqs("requirements.txt")

setup(
    name="nucliadb",
    version=open("VERSION").read().strip(),
    long_description=(open("README.md").read() + "\n" + open("CHANGELOG").read()),
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3 :: Only",
    ],
    url="https://nucliadb.com",
    author="NucliaDB Community",
    keywords="search, semantic, AI",
    author_email="nucliadb@nuclia.com",
    python_requires=">=3.7, <4",
    license="BSD",
    zip_safe=True,
    include_package_data=True,
    packages=find_packages(),
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "nucliadb = nucliadb.run:run",
            "nucliadb_purge = nucliadb.purge:purge",
            "ndb_ingest = nucliadb.ingest.app:run",
            "ndb_purge = nucliadb.ingest.purge:run",
            "ndb_curator = nucliadb.ingest.curator:run",
            "nucliadb_one = nucliadb.one:run",
            "extract-openapi-reader = nucliadb.reader.openapi:command_extract_openapi",
            "reader-metrics = nucliadb.reader.run:run_with_metrics",
            "extract-openapi-search = nucliadb.search.openapi:command_extract_openapi",
            "search-metrics = nucliadb.search.run:run_with_metrics",
            "extract-openapi-writer = nucliadb.writer.openapi:command_extract_openapi",
            "writer-metrics = nucliadb.writer.run:run_with_metrics",
            "ndb_train = nucliadb.train.app:run",
            "nuclia_dataset_upload = nucliadb.train.upload:run",
        ]
    },
    project_urls={
        "Nuclia": "https://nuclia.com",
        "Github": "https://github.com/nuclia/nucliadb",
        "Discord": "https://discord.gg/8EvQwmsbzf",
    },
)
