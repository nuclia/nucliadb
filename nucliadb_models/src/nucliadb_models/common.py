# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import base64
import hashlib
import re
from enum import Enum
from typing import Any

from pydantic import (
    BaseModel,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)
from typing_extensions import Self

from nucliadb_models import content_types

FIELD_TYPE_CHAR_MAP = {
    "c": "conversation",
    "f": "file",
    "u": "link",
    "t": "text",
    # "a": "generic",
}

STORAGE_FILE_MATCH = re.compile(
    r"/?kbs/(?P<kbid>[^/]+)/r/(?P<rid>[^/]+)/(?P<download_type>[fe])/(?P<field_type>\w)/(?P<field_id>[^/]+)/?(?P<key>.*)?"
)
DOWNLOAD_TYPE_MAP = {"f": "field", "e": "extracted"}
DOWNLOAD_URI = "/kb/{kbid}/resource/{rid}/{field_type}/{field_id}/download/{download_type}/{key}"

_NOT_SET = object()


class ParamDefault(BaseModel):
    default: Any = None
    title: str
    description: str
    le: float | None = None
    gt: float | None = None
    max_items: int | None = None
    deprecated: bool = False

    def to_pydantic_field(self, default=_NOT_SET, **kw) -> Field:  # type: ignore
        """
        :param default: to be able to override default value - as some params
        are reused but they will have different default values depending on the endpoint.
        """
        deprecated = kw.pop("deprecated", self.deprecated)
        return Field(
            default=self.default if default is _NOT_SET else default,
            title=self.title,
            description=self.description,
            gt=self.gt,
            le=self.le,
            max_length=self.max_items,
            deprecated=deprecated,
            **kw,
        )


class FieldID(BaseModel):
    class FieldType(Enum):
        FILE = "file"
        LINK = "link"
        TEXT = "text"
        GENERIC = "generic"
        CONVERSATION = "conversation"

    field_type: FieldType
    field: str


class File(BaseModel):
    filename: str | None = None
    content_type: str = "application/octet-stream"
    payload: str | None = Field(default=None, description="Base64 encoded file content")
    md5: str | None = None
    # These are to be used for external files
    uri: str | None = None
    extra_headers: dict[str, str] = {}

    @model_validator(mode="after")
    def _check_internal_file_fields(self) -> Self:
        if not content_types.valid(self.content_type):
            raise ValueError(f"Unsupported content type: {self.content_type}")
        if self.uri:
            # Externally hosted file
            return self
        if self.filename is None:
            raise ValueError(f"'filename' field is required")
        if self.payload is None:
            raise ValueError(f"'payload' field is required")
        if self.md5 is None:
            # In case md5 is not supplied, compute it
            try:
                result = hashlib.md5(base64.b64decode(self.payload), usedforsecurity=False)
                self.md5 = result.hexdigest()
            except Exception:
                raise ValueError("MD5 could not be computed")

        return self

    @property
    def is_external(self) -> bool:
        return self.uri is not None


class FileB64(BaseModel):
    filename: str
    content_type: str = "application/octet-stream"
    payload: str
    md5: str

    @field_validator("content_type")
    def check_content_type(cls, v):
        if not content_types.valid(v):
            raise ValueError(f"Unsupported content type: {v}")
        return v


class CloudFile(BaseModel):
    uri: str | None = None
    size: int | None = None
    content_type: str | None = None
    bucket_name: str | None = None

    class Source(Enum):
        FLAPS = "FLAPS"
        GCS = "GCS"
        S3 = "S3"
        LOCAL = "LOCAL"
        EXTERNAL = "EXTERNAL"

    source: Source | None
    filename: str | None
    resumable_uri: str | None
    offset: int | None
    upload_uri: str | None
    parts: list[str] | None
    old_uri: str | None
    old_bucket: str | None
    md5: str | None


class CloudLink(BaseModel):
    uri: str | None = None
    size: int | None = None
    content_type: str | None = None
    filename: str | None = None
    md5: str | None = None

    @staticmethod
    def format_reader_download_uri(uri: str) -> str:
        match = STORAGE_FILE_MATCH.match(uri)
        if not match:
            return uri

        url_params = match.groupdict()
        url_params["download_type"] = DOWNLOAD_TYPE_MAP[url_params["download_type"]]
        url_params["field_type"] = FIELD_TYPE_CHAR_MAP[url_params["field_type"]]
        return DOWNLOAD_URI.format(**url_params).rstrip("/")

    @field_serializer("uri")
    def serialize_uri(self, uri: str):
        return CloudLink.format_reader_download_uri(uri)


class FieldTypeName(str, Enum):
    """
    This map assumes that both values and extracted data field containers
    use the same names for its fields. See models.ResourceFieldValues and
    models.ResourceFieldExtractedData
    """

    TEXT = "text"
    FILE = "file"
    LINK = "link"
    CONVERSATION = "conversation"
    GENERIC = "generic"

    @classmethod
    def from_abbreviation(cls, abbr: str) -> "FieldTypeName":
        return {
            "t": FieldTypeName.TEXT,
            "f": FieldTypeName.FILE,
            "u": FieldTypeName.LINK,
            "c": FieldTypeName.CONVERSATION,
            "a": FieldTypeName.GENERIC,
        }[abbr]

    def abbreviation(self) -> str:
        return {
            FieldTypeName.TEXT: "t",
            FieldTypeName.FILE: "f",
            FieldTypeName.LINK: "u",
            FieldTypeName.CONVERSATION: "c",
            FieldTypeName.GENERIC: "a",
        }[self]


class FieldRef(BaseModel):
    field_type: FieldTypeName
    field_id: str
    split: str | None = None


class Classification(BaseModel):
    labelset: str
    label: str


class UserClassification(Classification):
    cancelled_by_user: bool = False


class Sentence(BaseModel):
    start: int | None = None
    end: int | None = None
    key: str | None = None


class PageInformation(BaseModel):
    page: int | None = Field(default=None, title="Page Information Page")
    page_with_visual: bool | None = None


class Representation(BaseModel):
    is_a_table: bool | None = None
    reference_file: str | None = None


class ParagraphRelations(BaseModel):
    parents: list[str] = []
    siblings: list[str] = []
    replacements: list[str] = []


class Paragraph(BaseModel):
    start: int | None = None
    end: int | None = None
    start_seconds: list[int] | None = None
    end_seconds: list[int] | None = None

    class TypeParagraph(str, Enum):
        TEXT = "TEXT"
        OCR = "OCR"
        INCEPTION = "INCEPTION"
        DESCRIPTION = "DESCRIPTION"
        TRANSCRIPT = "TRANSCRIPT"
        TITLE = "TITLE"
        TABLE = "TABLE"

    kind: TypeParagraph | None = None
    classifications: list[Classification] | None = None
    sentences: list[Sentence] | None = None
    key: str | None = None
    page: PageInformation | None = None
    representation: Representation | None = None
    relations: ParagraphRelations | None = None


class Shards(BaseModel):
    shards: list[str] | None = None


class Question(BaseModel):
    text: str
    language: str | None = None
    ids_paragraphs: list[str]


class Answer(BaseModel):
    text: str
    language: str | None = None
    ids_paragraphs: list[str]


class QuestionAnswer(BaseModel):
    question: Question
    answers: list[Answer]


class QuestionAnswers(BaseModel):
    question_answer: list[QuestionAnswer]
