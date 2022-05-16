import re

from setuptools import find_packages, setup  # type: ignore

with open("README.md") as f:
    readme = f.read()


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


def load_version():
    with open("VERSION", "r") as f:
        version = f.read().strip()
    return version


requirements = load_reqs("requirements.txt")

setup(
    name="nucliadb_telemetry",
    version=load_version(),
    author="nucliadb Authors",
    author_email="nucliadb@nuclia.com",
    description="NucliaDB Telemetry Library Python process",
    long_description=readme,
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
        "Topic :: System :: Monitoring",
    ],
    python_requires="~=3.9",
    include_package_data=True,
    packages=find_packages(),
    install_requires=requirements,
)
