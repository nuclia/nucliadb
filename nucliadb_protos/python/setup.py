from setuptools import find_packages, setup
import re

VERSION = open("../../VERSION").read().strip()
README = open("README.rst").read()


def load_reqs(filename):
    with open(filename) as reqs_file:
        return [
            # pin nucliadb-xxx to the same version as nucliadb
            line.strip() + f"=={VERSION}"
            if line.startswith("nucliadb-")
            else line.strip()
            for line in reqs_file.readlines()
            if not (
                re.match(r"\s*#", line) or re.match("-e", line) or re.match("-r", line)
            )
        ]


requirements = load_reqs("requirements.txt")

setup(
    name="nucliadb_protos",
    version=VERSION,
    description="protos for nucliadb",  # noqa
    long_description=README,
    setup_requires=["pytest-runner"],
    zip_safe=True,
    include_package_data=True,
    packages=find_packages(),
    install_requires=requirements,
)
