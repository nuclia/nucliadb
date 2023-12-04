import argparse
import os
import random
import time

LOCK_FILE = os.environ.get("LOCK_FILE", "lock.txt")


def acquire():
    while not _try_acquire():
        check_in = random.randint(1, 30)
        print(f"Waiting for lock. Checking in {check_in} seconds...")
        time.sleep(check_in)


def _try_acquire():
    try:
        # The 'x' mode will raise an error if the file already exists
        with open(LOCK_FILE, mode="x"):
            print("Lock acquired!")
            return True
    except FileExistsError:
        return False


def release():
    try:
        os.remove(LOCK_FILE)
        print("Lock released")
    except FileNotFoundError:
        pass


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Acquire or release a file system lock"
    )
    parser.add_argument("-a", "--action", choices=["acquire", "release"])
    args = parser.parse_args()
    return args


def main():
    args = parse_arguments()
    if args.action == "acquire":
        acquire()
    elif args.action == "release":
        release()
    else:
        raise ValueError(f"Invalid action: {args.action}")


if __name__ == "__main__":
    main()
