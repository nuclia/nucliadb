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


requirements = load_reqs("requirements.txt")

setup(
    name="nucliadb_search",
    version=open("VERSION").read().strip(),
    author="nucliadb Authors",
    author_email="nucliadb@nuclia.com",
    description="NucliaDB Reader Python process",
    long_description=readme,
    long_description_content_type="text/markdown",
    license="MIT",
    url="https://github.com/nuclia/nucliadb",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: AGPL License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.9",
    ],
    python_requires=">=3.7",
    include_package_data=True,
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "extract-openapi = nucliadb_search.openapi:command_extract_openapi",
            "search-metrics = nucliadb_search.run:run_with_metrics",
        ]
    },
    install_requires=requirements,
)
