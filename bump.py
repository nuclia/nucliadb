import argparse

parser = argparse.ArgumentParser(description="Bump version")
parser.add_argument("--build", type=int, help="build number")


def run(build_num: int):
    with open("VERSION", "r") as f:
        version = f.read().strip()

    version = f"{version}-post{build_num}"

    with open("VERSION", "w") as f:
        f.write(version)


if __name__ == "__main__":
    args = parser.parse_args()
    run(args.build)
