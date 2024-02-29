# -*- coding: utf-8 -*-
import re
from pathlib import Path

from setuptools import find_packages, setup

_dir = Path(__file__).resolve().parent
VERSION = _dir.joinpath("VERSION").open().read().strip()
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
    name="nucliadb",
    version=VERSION,
    long_description=README,
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3 :: Only",
    ],
    url="https://docs.nuclia.dev/docs/guides/nucliadb/intro",
    author="NucliaDB Community",
    keywords="search, semantic, AI",
    author_email="nucliadb@nuclia.com",
    python_requires=">=3.9, <4",
    license="BSD",
    zip_safe=True,
    include_package_data=True,
    package_data={"": ["*.txt", "*.md"], "nucliadb": ["py.typed"]},
    packages=find_packages(),
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            # Service commands
            # Standalone
            "nucliadb = nucliadb.standalone.run:run",
            # Ingest
            #   - This command runs pull workers + ingest write consumer
            "nucliadb-ingest = nucliadb.ingest.app:run_consumer",
            #   - Only runs processed resources write consumer
            "nucliadb-ingest-processed-consumer = nucliadb.ingest.app:run_processed_consumer",
            #   - Only runs GRPC Service
            "nucliadb-ingest-orm-grpc = nucliadb.ingest.app:run_orm_grpc",
            #   - Subscriber workers: auditing and shard creator
            "nucliadb-ingest-subscriber-workers = nucliadb.ingest.app:run_subscriber_workers",
            # Reader
            "nucliadb-reader = nucliadb.reader.run:run",
            # Writer
            "nucliadb-writer = nucliadb.writer.run:run",
            # Search
            "nucliadb-search = nucliadb.search.run:run",
            # Train
            "nucliadb-train = nucliadb.train.run:run",
            # utilities
            "nucliadb-purge = nucliadb.purge:run",
            "nucliadb-orphan-shards = nucliadb.purge.orphan_shards:run",
            "nucliadb-migrate = nucliadb.migrator.command:main",
            "nucliadb-migration-runner = nucliadb.migrator.command:main_forever",
            "nucliadb-metrics-exporter = nucliadb.metrics_exporter:main",
            "nucliadb-rollover-kbid = nucliadb.common.cluster.rollover:rollover_kbid_command",
            "nucliadb-extract-openapi-reader = nucliadb.reader.openapi:command_extract_openapi",
            "nucliadb-extract-openapi-search = nucliadb.search.openapi:command_extract_openapi",
            "nucliadb-extract-openapi-writer = nucliadb.writer.openapi:command_extract_openapi",
            "nucliadb-dataset-upload = nucliadb.train.upload:run",
        ]
    },
    project_urls={
        "Nuclia": "https://nuclia.com",
        "Github": "https://github.com/nuclia/nucliadb",
        "Discord": "https://discord.gg/8EvQwmsbzf",
        "API Reference": "https://docs.nuclia.dev/docs/api",
    },
    extras_require={
        "redis": ["redis>=4.3.4"],
    },
)
