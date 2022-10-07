import re

from setuptools import find_packages, setup  # type: ignore

with open("README.md") as f:
    readme = f.read()

with open("VERSION") as f:
    version = f.read().strip()


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
    name="nucliadb_train",
    version=version,
    author="nucliadb Authors",
    author_email="nucliadb@nuclia.com",
    description="NucliaDB Train Python process",
    long_description=readme,
    long_description_content_type="text/markdown",
    license="MIT",
    url="https://github.com/nuclia/nucliadb",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.9",
    ],
    python_requires=">=3.7",
    include_package_data=True,
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "ndb_train = nucliadb_train.app:run",
            "nuclia_dataset_upload = nucliadb_train.upload:run",
        ]
    },
    install_requires=requirements,
)
