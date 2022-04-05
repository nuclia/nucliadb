from nats.errors import ProtocolError as ProtocolError
from typing import Any

MSG_RE: Any
HMSG_RE: Any
OK_RE: Any
ERR_RE: Any
PING_RE: Any
PONG_RE: Any
INFO_RE: Any
INFO_OP: bytes
CONNECT_OP: bytes
PUB_OP: bytes
MSG_OP: bytes
HMSG_OP: bytes
SUB_OP: bytes
UNSUB_OP: bytes
PING_OP: bytes
PONG_OP: bytes
OK_OP: bytes
ERR_OP: bytes
MSG_END: bytes
OK: Any
PING: Any
PONG: Any
CRLF_SIZE: Any
OK_SIZE: Any
PING_SIZE: Any
PONG_SIZE: Any
MSG_OP_SIZE: Any
ERR_OP_SIZE: Any
AWAITING_CONTROL_LINE: int
AWAITING_MSG_PAYLOAD: int
MAX_CONTROL_LINE_SIZE: int
STALE_CONNECTION: str
AUTHORIZATION_VIOLATION: str
PERMISSIONS_ERR: str

class Parser:
    nc: Any
    def __init__(self, nc: Any | None = ...) -> None: ...
    buf: Any
    state: Any
    needed: int
    header_needed: int
    msg_arg: Any
    def reset(self) -> None: ...
    async def parse(self, data: bytes = ...): ...

class ErrProtocol(ProtocolError): ...
