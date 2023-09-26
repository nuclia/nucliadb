import argparse

parser = argparse.ArgumentParser(description="Bump version")
parser.add_argument("--build", type=int, help="build number")
parser.add_argument("--sem", type=str, help="Semantic version part")
parser.add_argument("--minor", type=bool, help="Minor")
parser.add_argument("--bug", type=bool, help="Bug")
parser.add_argument(
    "--version_file", type=str, help="Version file to update", default="VERSION"
)


def run(args):
    with open(args.version_file, "r") as f:
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
    with open(args.version_file, "w") as f:
        f.write(version)


if __name__ == "__main__":
    args = parser.parse_args()
    run(args)
