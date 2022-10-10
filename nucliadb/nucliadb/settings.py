from enum import Enum
from typing import Optional

import pydantic


class LogLevel(str, Enum):
    INFO = "INFO"
    ERROR = "ERROR"
    DEBUG = "DEBUG"


class Driver(str, Enum):
    REDIS = "REDIS"
    LOCAL = "LOCAL"


class Settings(pydantic.BaseSettings):
    driver: Driver = pydantic.Field(Driver.LOCAL, description="Main DB Path string")
    maindb: str = pydantic.Field(description="Main DB Path string")
    blob: str = pydantic.Field(description="Blob Path string")
    key: Optional[str] = pydantic.Field(
        description="Nuclia Understanding API Key string"
    )
    node: str = pydantic.Field(description="Node Path string")
    zone: Optional[str] = pydantic.Field(description="Nuclia Understanding API Zone ID")
    http: int = pydantic.Field(8080, description="HTTP Port int")
    grpc: int = pydantic.Field(8030, description="GRPC Port int")
    train: int = pydantic.Field(8031, description="Train GRPC Port int")
    log: LogLevel = pydantic.Field(
        LogLevel.ERROR, description="Log level [DEBUG,INFO,ERROR] string"
    )
