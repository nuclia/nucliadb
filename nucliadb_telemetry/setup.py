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

requirements_otel = [
    "opentelemetry-sdk==1.21.0",
    "opentelemetry-api==1.21.0",
    "opentelemetry-proto==1.21.0",
    "opentelemetry-exporter-jaeger==1.21.0",
    "opentelemetry-propagator-b3==1.21.0",
    "opentelemetry-instrumentation-fastapi>=0.42b0",
    "opentelemetry-instrumentation-aiohttp-client>=0.42b0",
    "opentelemetry-semantic-conventions>=0.42b0",
]
requirements_grpc = [
    "grpcio>=1.44.0",
    "grpcio-health-checking>=1.44.0",
    "grpcio-channelz>=1.44.0",
    "grpcio-status>=1.44.0",
    "grpcio-tools>=1.44.0",
    "grpcio-testing>=1.44.0",
    "grpcio-reflection>=1.44.0",
] + requirements_otel
requirements_nats = ["nats-py[nkeys]>=2.5.0"] + requirements_otel
requirements_fastapi = ["fastapi"] + requirements_otel
requirements_tikv = ["tikv-client>=0.0.3"] + requirements_otel

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
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: System :: Monitoring",
    ],
    python_requires=">=3.8",
    include_package_data=True,
    extras_require={
        "otel": requirements_otel,
        "grpc": requirements_grpc,
        "nats": requirements_nats,
        "fastapi": requirements_fastapi,
        "tikv": requirements_tikv,
        "all": list(
            set(
                requirements_otel
                + requirements_grpc
                + requirements_nats
                + requirements_fastapi
                + requirements_tikv
            )
        ),
    },
    package_data={"": ["*.txt", "*.md"], "nucliadb_telemetry": ["py.typed"]},
    packages=find_packages(),
    install_requires=requirements,
)
