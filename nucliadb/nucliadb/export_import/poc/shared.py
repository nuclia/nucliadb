import base64
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


def encode_binary(rid: str, binary: bytes) -> bytes:
    codec = Codec.BINARY.encode("utf-8")
    rid = rid.encode("utf-8")
    binary = b64encode(binary)
    return codec + rid + BINARY_SEPARATOR + binary


def encode_resource(resource: Resource) -> bytes:
    serialized = resource.json().encode("utf-8")
    return Codec.RESOURCE.encode("utf-8") + b64encode(serialized)


def parse_codec(encoded: bytes) -> Tuple[Codec, bytes]:
    if encoded.startswith(Codec.BINARY.encode("utf-8")):
        return Codec.BINARY, encoded[4:]
    elif encoded.startswith(Codec.RESOURCE.encode("utf-8")):
        return Codec.RESOURCE, encoded[4:]
    else:
        raise ValueError(f"Unknown codec: {encoded[:4]}")


def decode_resource(encoded: bytes) -> Resource:
    decoded = b64decode(encoded)
    return Resource.parse_raw(decoded)


def decode_binary(encoded: bytes) -> Tuple[str, bytes]:
    binary_separator = BINARY_SEPARATOR
    rid, data = encoded.split(binary_separator)
    rid = rid.decode("utf-8")
    return rid, data
