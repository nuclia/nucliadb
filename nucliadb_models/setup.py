import re

from setuptools import find_packages, setup  # type: ignore


def build_readme():
    readme = ""
    with open("README.md") as f:
        readme += f.read()

    readme += "\n"

    with open("CHANGELOG") as f:
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
    version="0.1.0",
    author="nucliadb Authors",
    author_email="nucliadb@nuclia.com",
    description="NucliaDB Models",
    long_description=build_readme(),
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
    python_requires="~=3.9",
    include_package_data=True,
    packages=find_packages(),
    install_requires=requirements,
)
