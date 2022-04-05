from nats.errors import Error as Error, MsgAlreadyAckdError as MsgAlreadyAckdError, NotJSMessageError as NotJSMessageError
from typing import Any

class Msg:
    class Ack:
        Ack: bytes
        Nak: bytes
        Progress: bytes
        Term: bytes
        Prefix0: str
        Prefix1: str
        Domain: int
        AccHash: int
        Stream: int
        Consumer: int
        NumDelivered: int
        StreamSeq: int
        ConsumerSeq: int
        Timestamp: int
        NumPending: int
        V1TokenCount: int
        V2TokenCount: int
    subject: Any
    reply: Any
    data: Any
    sid: Any
    headers: Any
    def __init__(self, subject: str = ..., reply: str = ..., data: bytes = ..., sid: int = ..., client: Any | None = ..., headers: dict = ...) -> None: ...
    @property
    def header(self): ...
    async def respond(self, data: bytes): ...
    async def ack(self) -> None: ...
    async def ack_sync(self, timeout: float = ...): ...
    async def nak(self) -> None: ...
    async def in_progress(self) -> None: ...
    async def term(self) -> None: ...
    @property
    def metadata(self): ...
    class Metadata:
        class SequencePair:
            consumer: int
            stream: int
            def __init__(self, consumer, stream) -> None: ...
        sequence: Any
        num_pending: Any
        num_delivered: Any
        timestamp: Any
        stream: Any
        consumer: Any
        def __init__(self, sequence: Any | None = ..., num_pending: int = ..., num_delivered: int = ..., timestamp: str = ..., stream: str = ..., consumer: str = ...) -> None: ...
