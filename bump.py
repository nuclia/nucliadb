import argparse

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

    if args.sem:
        # we only set other versions if we are not bumping a sem version
        return

    # replace nidx binding toml version as well
    VERSION_UPDATES = [
        "nidx/nidx_binding/Cargo.toml",
        "nidx/nidx_binding/pyproject.toml",
        "nidx/nidx_protos/pyproject.toml",
        "nucliadb_dataset/pyproject.toml",
        "nucliadb_models/pyproject.toml",
        "nucliadb_protos/python/pyproject.toml",
        "nucliadb_sdk/pyproject.toml",
        "nucliadb_telemetry/pyproject.toml",
        "nucliadb_utils/pyproject.toml",
        "pyproject.toml",
    ]
    for filename in VERSION_UPDATES:
        update_version(filename, version)

    REQUIREMENT_UPDATES = [
        "nucliadb/pyproject.toml",
        "nucliadb_utils/pyproject.toml",
        "nucliadb_sdk/pyproject.toml",
        "nucliadb_dataset/pyproject.toml",
    ]
    # go through each requirements.txt and update the version to the new bump
    for req_filepath in (
        "nucliadb/requirements.txt",
        "nucliadb_utils/requirements.txt",
        "nucliadb_sdk/requirements.txt",
        "nucliadb_dataset/requirements.txt",
    ):
        with open(req_filepath, "r") as f:
            req_lines = []
            for line in f.read().splitlines():
                if line.startswith("nucliadb-") and (
                    "=" not in line and ">" not in line and "~" not in line
                ):
                    line = f"{line}>={python_version}"
                req_lines.append(line)

        with open(req_filepath, "w") as f:
            f.write("\n".join(req_lines))


def update_version(filename, version):
    with open(filename, "r") as f:
        toml = f.read()

    new_toml = []
    for line in toml.splitlines():
        if line.startswith("version ="):
            line = f'version = "{version}"'
        new_toml.append(line)

    with open(filename, "w") as f:
        f.write("\n".join(new_toml))


if __name__ == "__main__":
    args = parser.parse_args()
    run(args)
