from nats.errors import *
from nats.aio.errors import *
from nats.js.headers import *
from nats.aio.subscription import *
from nats.protocol.parser import *
import asyncio
import ssl
from nats.aio.msg import Msg as Msg
from nats.js import (
    JetStreamContext as JetStreamContext,
    JetStreamManager as JetStreamManager,
)
from nats.nuid import NUID as NUID
from typing import Any, Awaitable, Callable, List, Optional, Tuple, Union

__lang__: str
PROTOCOL: int
EMPTY: str
PING_PROTO: Any
PONG_PROTO: Any
INBOX_PREFIX: Any
INBOX_PREFIX_LEN: Any
DEFAULT_PENDING_SIZE: Any
DEFAULT_BUFFER_SIZE: int
DEFAULT_RECONNECT_TIME_WAIT: int
DEFAULT_MAX_RECONNECT_ATTEMPTS: int
DEFAULT_PING_INTERVAL: int
DEFAULT_MAX_OUTSTANDING_PINGS: int
DEFAULT_MAX_PAYLOAD_SIZE: int
DEFAULT_MAX_FLUSHER_QUEUE_SIZE: int
DEFAULT_CONNECT_TIMEOUT: int
DEFAULT_DRAIN_TIMEOUT: int
NATS_HDR_LINE: Any
NATS_HDR_LINE_SIZE: Any
NO_RESPONDERS_STATUS: str
CTRL_STATUS: str
STATUS_MSG_LEN: int
CTRL_LEN: Any

class Srv:
    uri: str
    reconnects: int
    last_attempt: Optional[float]
    did_connect: bool
    discovered: bool
    tls_name: Optional[str]
    def __init__(
        self, uri, reconnects, last_attempt, did_connect, discovered, tls_name
    ) -> None: ...

class Client:
    msg_class: Any
    DISCONNECTED: int
    CONNECTED: int
    CLOSED: int
    RECONNECTING: int
    CONNECTING: int
    DRAINING_SUBS: int
    DRAINING_PUBS: int
    options: Any
    stats: Any
    def __init__(self) -> None: ...
    async def connect(
        self,
        servers: List[str] = ...,
        error_cb: Optional[Callable[[Exception], Awaitable[None]]] = ...,
        disconnected_cb: Optional[Callable[[], Awaitable[None]]] = ...,
        closed_cb: Optional[Callable[[], Awaitable[None]]] = ...,
        discovered_server_cb: Optional[Callable[[], None]] = ...,
        reconnected_cb: Optional[Callable[[], Awaitable[None]]] = ...,
        name: Optional[str] = ...,
        pedantic: bool = ...,
        verbose: bool = ...,
        allow_reconnect: bool = ...,
        connect_timeout: int = ...,
        reconnect_time_wait: int = ...,
        max_reconnect_attempts: int = ...,
        ping_interval: int = ...,
        max_outstanding_pings: int = ...,
        dont_randomize: bool = ...,
        flusher_queue_size: int = ...,
        no_echo: bool = ...,
        tls: Optional[ssl.SSLContext] = ...,
        tls_hostname: Optional[str] = ...,
        user: Optional[str] = ...,
        password: Optional[str] = ...,
        token: Optional[str] = ...,
        drain_timeout: int = ...,
        signature_cb: Any | None = ...,
        user_jwt_cb: Optional[Callable[[], str]] = ...,
        user_credentials: Optional[Union[str, Tuple[str, str]]] = ...,
        nkeys_seed: Optional[str] = ...,
    ): ...
    async def close(self) -> None: ...
    async def drain(self) -> None: ...
    async def publish(
        self, subject: str, payload: bytes = ..., reply: str = ..., headers: dict = ...
    ): ...
    async def subscribe(
        self,
        subject: str,
        queue: str = ...,
        cb: Optional[Callable[[Msg], Awaitable[None]]] = ...,
        future: Optional[asyncio.Future] = ...,
        max_msgs: int = ...,
        pending_msgs_limit: int = ...,
        pending_bytes_limit: int = ...,
    ) -> Subscription: ...
    async def request(
        self,
        subject: str,
        payload: bytes = ...,
        timeout: float = ...,
        old_style: bool = ...,
        headers: dict = ...,
    ) -> Msg: ...
    def new_inbox(self) -> str: ...
    async def flush(self, timeout: int = ...): ...
    @property
    def connected_url(self) -> Optional[str]: ...
    @property
    def servers(self) -> List[str]: ...
    @property
    def discovered_servers(self) -> List[str]: ...
    @property
    def max_payload(self) -> int: ...
    @property
    def client_id(self) -> Optional[str]: ...
    @property
    def last_error(self) -> Optional[Exception]: ...
    @property
    def pending_data_size(self) -> int: ...
    @property
    def is_closed(self) -> bool: ...
    @property
    def is_reconnecting(self) -> bool: ...
    @property
    def is_connected(self) -> bool: ...
    @property
    def is_connecting(self) -> bool: ...
    @property
    def is_draining(self) -> bool: ...
    @property
    def is_draining_pubs(self) -> bool: ...
    async def __aenter__(self): ...
    async def __aexit__(self, *exc_info) -> None: ...
    def jetstream(self, **opts): ...
    def jsm(self, **opts): ...
