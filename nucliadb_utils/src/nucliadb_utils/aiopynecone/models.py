# Copyright (C) 2021 Bosutech XXI S.L.
#
# nucliadb is offered under the AGPL v3.0 and as commercial software.
# For commercial licensing, contact us at info@nuclia.com.
#
# AGPL:
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
import json
from typing import Any, Optional

import pydantic
from pydantic import BaseModel, Field, field_validator
from typing_extensions import Annotated

from nucliadb_utils.aiopynecone.exceptions import MetadataTooLargeError

KILO_BYTE = 1024
MAX_METADATA_SIZE = 40 * KILO_BYTE
MAX_INDEX_NAME_LENGTH = 45
MAX_VECTOR_ID_LENGTH = 512


# Requests

IndexNamePattern = r"^[a-z0-9-]+$"


def validate_index_name(value, handler, info):
    try:
        return handler(value)
    except pydantic.ValidationError as e:
        if any(x["type"] == "string_pattern_mismatch" for x in e.errors()):
            raise ValueError(
                f"Invalid field_id: '{value}'. Pinecone index names must be a string with only "
                "lowercase letters, numbers and dashes."
            )
        else:
            raise e


IndexNameStr = Annotated[
    str,
    pydantic.StringConstraints(pattern=IndexNamePattern, min_length=1, max_length=MAX_INDEX_NAME_LENGTH),
    pydantic.WrapValidator(validate_index_name),
]


class CreateIndexRequest(BaseModel):
    name: IndexNameStr
    dimension: int
    metric: str
    spec: dict[str, Any] = {}


class Vector(BaseModel):
    id: str = Field(min_length=1, max_length=MAX_VECTOR_ID_LENGTH)
    values: list[float]
    metadata: dict[str, Any] = {}

    @field_validator("metadata", mode="after")
    @classmethod
    def validate_metadata_size(cls, value):
        json_value = json.dumps(value)
        if len(json_value) > MAX_METADATA_SIZE:
            raise MetadataTooLargeError(f"metadata size is too large: {len(json_value)} bytes")
        return value


class UpsertRequest(BaseModel):
    vectors: list[Vector]


# Responses


class CreateIndexResponse(BaseModel):
    host: str


class VectorId(BaseModel):
    id: str


class Pagination(BaseModel):
    next: str


class ListResponse(BaseModel):
    vectors: list[VectorId]
    pagination: Optional[Pagination] = None


class VectorMatch(BaseModel):
    id: str
    score: float
    # Only populated if `includeValues` is set to `True
    values: Optional[list[float]] = None
    # Only populated if `includeMetadata` is set to `True
    metadata: Optional[dict[str, Any]] = None


class QueryResponse(BaseModel):
    matches: list[VectorMatch]


class IndexNamespaceStats(BaseModel):
    vectorCount: int


class IndexStats(BaseModel):
    dimension: int
    namespaces: dict[str, IndexNamespaceStats] = {}
    totalVectorCount: int


class IndexStatus(BaseModel):
    ready: bool
    state: str


class IndexDescription(BaseModel):
    dimension: int
    host: str
    metric: str
    name: str
    spec: dict[str, Any]
    status: IndexStatus
