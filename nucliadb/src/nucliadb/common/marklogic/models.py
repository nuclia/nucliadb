from dataclasses import dataclass
from typing import Any, TypedDict

from typing_extensions import NotRequired


@dataclass
class MarkLogicDatabaseLocator:
    server_id: str
    database: str
    project_id: str | None = None


class MraRetrieveRequest(TypedDict, total=False):
    text: str
    topk: int
    labels: dict[str, dict[str, str]]
    filters: dict[str, dict[str, Any]]
    vectors: dict[str, list[float]]
    entities: list[str]
    relations: list[str]
    metadata: list[str]


class MraRetrieveResponse(TypedDict):
    matches: NotRequired[list[dict[str, Any]]]
    totalMatches: NotRequired[int]
    warnings: NotRequired[list[str]]


class MraRetrieveDefinitionResponse(TypedDict, total=False):
    labels: list[dict[str, Any]]
    filters: dict[str, dict[str, Any]]
    vectorMetadata: list[dict[str, Any]]
    metadataFields: list[str]
