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
    with open("VERSION", "w") as f:
        f.write(version)

    # replace node binding toml version as well
    with open("nucliadb_node_binding/Cargo.toml", "r") as f:
        cargo = f.read()

    new_cargo = []
    for line in cargo.splitlines():
        if line.startswith("version ="):
            line = f'version = "{version}"'
        new_cargo.append(line)

    with open("nucliadb_node_binding/Cargo.toml", "w") as f:
        f.write("\n".join(new_cargo))

    # go through each requirements.txt and update the version to the new bump
    for req_filepath in (
        "nucliadb/requirements.txt",
        "nucliadb_utils/requirements.txt",
        "nucliadb_sdk/requirements.txt",
        "nucliadb_models/requirements.txt",
        "nucliadb_dataset/requirements.txt",
    ):
        with open(req_filepath, "r") as f:
            req_lines = []
            for line in f.read().splitlines():
                if line.startswith("nucliadb-") and (
                    "=" not in line and ">" not in line and "~" not in line
                ):
                    line = f"{line}>={version}"
                req_lines.append(line)

        with open(req_filepath, "w") as f:
            f.write("\n".join(req_lines))


if __name__ == "__main__":
    args = parser.parse_args()
    run(args)
