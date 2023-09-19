from setuptools import find_packages, setup
import re
from pathlib import Path


_dir = Path(__file__).resolve().parent
VERSION = _dir.parent.parent.joinpath("VERSION").open().read().strip()
README = _dir.joinpath("README.rst").open().read()


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
    package_data={"": ["*.txt", "*.rst"], "nucliadb_protos": ["py.typed"]},
    packages=find_packages(),
    install_requires=[
        "protobuf >= 4.22.3, < 5",
        "grpcio >= 1.44.0",
        "grpcio-tools >= 1.44.0",
        "mypy-protobuf >= 3.4.0",
    ],
)
