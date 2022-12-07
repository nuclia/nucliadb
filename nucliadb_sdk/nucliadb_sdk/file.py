from dataclasses import dataclass


@dataclass
class File:
    data: bytes
    filename: str
    content_type: str = "application/octet-stream"
