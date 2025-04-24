import argparse
import re

parser = argparse.ArgumentParser(description="Bump version")
parser.add_argument("--build", type=int, help="build number")
parser.add_argument("--sem", type=str, help="Semantic version part")
parser.add_argument("--minor", type=bool, help="Minor")
parser.add_argument("--bug", type=bool, help="Bug")


def run(args):
    with open("VERSION", "r") as f:
        version = f.read().strip()

    major, minor, bug = version.split(".")
    major = int(major)
    minor = int(minor)
    bug = int(bug)
    version_post = ""

    if args.build:
        version_post = f"-post{args.build}"
    elif args.sem == "major":
        major += 1
        minor = 0
        bug = 0
    elif args.sem == "minor":
        minor += 1
        bug = 0
    elif args.sem == "bug":
        bug += 1

    version = f"{major}.{minor}.{bug}{version_post}"
    # rust does not like `.` in post but python requires is
    python_version = version.replace("-", ".")

    with open("VERSION", "w") as f:
        f.write(python_version)

    VERSION_UPDATES = [
        "nidx/nidx_binding/pyproject.toml",
        "nidx/nidx_protos/pyproject.toml",
        "nucliadb/pyproject.toml",
        "nucliadb_dataset/pyproject.toml",
        "nucliadb_models/pyproject.toml",
        "nucliadb_protos/pyproject.toml",
        "nucliadb_sdk/pyproject.toml",
        "nucliadb_telemetry/pyproject.toml",
        "nucliadb_utils/pyproject.toml",
        "pyproject.toml",
    ]
    for filename in VERSION_UPDATES:
        update_version(filename, python_version)

    # we only freeze dependency versions if we are not bumping a sem version
    if args.sem:
        return

    REQUIREMENT_UPDATES = [
        "nucliadb/pyproject.toml",
        "nucliadb_utils/pyproject.toml",
        "nucliadb_sdk/pyproject.toml",
        "nucliadb_dataset/pyproject.toml",
    ]
    for filename in REQUIREMENT_UPDATES:
        update_requirements(filename, python_version)


def update_version(filename, version):
    with open(filename, "r") as f:
        toml = f.read()

    new_toml = []
    for line in toml.splitlines():
        if line.startswith("version ="):
            line = f'version = "{version}"'
        new_toml.append(line)

    with open(filename, "w") as f:
        f.write("\n".join(new_toml) + "\n")


# Matches packages starting with nucliadb- or nidx-, without a version specifier (like >= 1.1.1)
REQUIREMENT_REGEX = re.compile(r'( *"(?:nucliadb|nidx)-[a-z\[\],]+)(",?)')


def update_requirements(filename, version):
    with open(filename, "r") as f:
        toml = f.read()

    new_toml = []
    for line in toml.splitlines():
        new_toml.append(REQUIREMENT_REGEX.sub(f"\\1>={version}\\2", line))

    with open(filename, "w") as f:
        f.write("\n".join(new_toml) + "\n")


if __name__ == "__main__":
    args = parser.parse_args()
    run(args)
