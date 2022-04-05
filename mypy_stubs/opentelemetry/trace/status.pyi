import enum
import typing
from typing import Any

logger: Any

class StatusCode(enum.Enum):
    UNSET: int
    OK: int
    ERROR: int

class Status:
    def __init__(self, status_code: StatusCode = ..., description: typing.Optional[str] = ...) -> None: ...
    @property
    def status_code(self) -> StatusCode: ...
    @property
    def description(self) -> typing.Optional[str]: ...
    @property
    def is_ok(self) -> bool: ...
    @property
    def is_unset(self) -> bool: ...
