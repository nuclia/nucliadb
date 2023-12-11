from dataclasses import dataclass


@dataclass
class Error:
    kbid: str
    endpoint: str
    status_code: int
    error: str


ERRORS: list[Error] = []


def append_error(kbid: str, endpoint: str, status_code: str, error: str):
    ERRORS.append(
        Error(
            kbid=kbid,
            endpoint=endpoint,
            status_code=-1,
            error=error,
        )
    )


def print_errors():
    print("Errors summary:")
    for error in ERRORS:
        print(error)
    print("=" * 50)
