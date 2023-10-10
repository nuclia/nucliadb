import re

from setuptools import find_packages, setup  # type: ignore


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
    name="nucliadb_performance",
    version="0.0.1",
    author="nucliadb Authors",
    author_email="nucliadb@nuclia.com",
    description="NucliaDB Performace Library Python process",
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
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: System :: Monitoring",
    ],
    python_requires=">=3.9",
    include_package_data=True,
    package_data={"": ["*.txt", "*.md"], "nucliadb_performance": ["py.typed"]},
    packages=find_packages(),
    install_requires=requirements,
)
