import re
from pathlib import Path

from setuptools import find_packages, setup  # type: ignore

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
    name="nucliadb_dataset",
    version=VERSION,
    author="nucliadb Authors",
    author_email="nucliadb@nuclia.com",
    description="NucliaDB Train Python client",
    long_description=README,
    long_description_content_type="text/markdown",
    license="MIT",
    url="https://github.com/nuclia/nucliadb",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: GNU Affero General Public License v3",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3 :: Only",
    ],
    python_requires=">=3.8",
    include_package_data=True,
    package_data={"": ["*.txt", "*.md"], "nucliadb_dataset": ["py.typed"]},
    packages=find_packages(),
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "dataset_export = nucliadb_dataset.run:run",
        ]
    },
)
