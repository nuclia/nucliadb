from base64 import b64decode, b64encode
from enum import Enum
from typing import Tuple, Union

from pydantic import BaseModel

SERVER_HOST = "localhost"
SERVER_PORT = 8000

LINE_SEPARATOR = b"\n"
BINARY_SEPARATOR = b"__X__"


class Resource(BaseModel):
    id: str
    data: str

class Codec(str, Enum):
    BINARY = "BIN:"
    RESOURCE = "RES:"


def encode_binary(binary: bytes) -> bytes:
    return Codec.BINARY.encode("utf-8") + b64encode(binary)


def encode_resource(resource: Resource) -> bytes:
    serialized = resource.json().encode("utf-8")
    return Codec.RESOURCE.encode("utf-8") + b64encode(serialized)




def decode_resource(line: Union[bytes, str]) -> Resource:
    if isinstance(line, bytes):
        line_separator = LINE_SEPARATOR
    else:
        line_separator = LINE_SEPARATOR.decode("utf-8")
    line = line.rstrip(line_separator)
    decoded = b64decode(line)
    return Resource.parse_raw(decoded)


def decode_binary(line: Union[bytes, str]) -> Tuple[str, bytes]:
    if isinstance(line, bytes):
        line_separator = LINE_SEPARATOR
        binary_separator = BINARY_SEPARATOR
        line = line.rstrip(line_separator)
        rid, data = line.split(binary_separator)
        rid = rid.decode("utf-8")
        return rid, data
    else:
        line_separator = LINE_SEPARATOR.decode("utf-8")
        binary_separator = BINARY_SEPARATOR.decode("utf-8")
        line = line.rstrip(line_separator)
        rid, data = line.split(binary_separator)
        data = data.encode("utf-8")
        return rid, data
