import nats.errors
from nats.js import api as api
from typing import Any

class Error(nats.errors.Error):
    description: Any
    def __init__(self, description: Any | None = ...) -> None: ...

class APIError(Error):
    code: int
    err_code: int
    description: str
    stream: str
    seq: int
    def __init__(self, code: Any | None = ..., description: Any | None = ..., err_code: Any | None = ..., stream: Any | None = ..., seq: Any | None = ...) -> None: ...
    @classmethod
    def from_msg(cls, msg) -> None: ...
    @classmethod
    def from_error(cls, err) -> None: ...

class ServiceUnavailableError(APIError): ...
class ServerError(APIError): ...
class NotFoundError(APIError): ...
class BadRequestError(APIError): ...
class NoStreamResponseError(Error): ...

class ConsumerSequenceMismatchError(Error):
    stream_resume_sequence: Any
    consumer_sequence: Any
    last_consumer_sequence: Any
    def __init__(self, stream_resume_sequence: Any | None = ..., consumer_sequence: Any | None = ..., last_consumer_sequence: Any | None = ...) -> None: ...

class BucketNotFoundError(NotFoundError): ...
class BadBucketError(Error): ...

class KeyDeletedError(Error):
    entry: Any
    op: Any
    def __init__(self, entry: Any | None = ..., op: Any | None = ...) -> None: ...
