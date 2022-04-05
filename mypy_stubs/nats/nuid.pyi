from typing import Any

DIGITS: bytes
BASE: int
PREFIX_LENGTH: int
SEQ_LENGTH: int
MAX_SEQ: int
MIN_INC: int
MAX_INC: int
INC: Any
TOTAL_LENGTH: Any

class NUID:
    def __init__(self) -> None: ...
    def next(self) -> bytearray: ...
    def randomize_prefix(self) -> None: ...
    def reset_sequential(self) -> None: ...
