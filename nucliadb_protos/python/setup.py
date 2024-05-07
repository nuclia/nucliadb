from setuptools import find_packages, setup
from pathlib import Path


_dir = Path(__file__).resolve().parent
VERSION = _dir.parent.parent.joinpath("VERSION").open().read().strip()
README = _dir.joinpath("README.rst").open().read()


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
        "protobuf >= 5.26.1, < 6",
        "grpcio >= 1.63.0",
        "grpcio-tools >= 1.63.0",
        "mypy-protobuf >= 3.6.0",
        "types-protobuf >= 5.26.0, < 6"
    ],
)
