from setuptools import find_packages, setup
import re

long_description = open("README.rst").read() + "\n"
changelog = open("CHANGELOG.rst").read()
found = 0
for line in changelog.splitlines():
    if len(line) > 15 and line[-1] == ")" and line[-4] == "-":
        found += 1
        if found >= 20:
            break
    long_description += "\n" + line


long_description += """

...

You are seeing a truncated changelog.

You can read the `changelog file <https://github.com/nuclia/nucliadb/blob/master/nucliadb_protos/python/CHANGELOG.rst>`_
for a complete list.

"""


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
    name="nucliadb_protos",
    version=open("VERSION").read().strip(),
    description="protos for nucliadb",  # noqa
    long_description=long_description,
    setup_requires=["pytest-runner"],
    zip_safe=True,
    include_package_data=True,
    packages=find_packages(),
    install_requires=requirements
)
