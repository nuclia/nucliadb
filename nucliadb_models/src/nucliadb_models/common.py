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
import base64
import hashlib
import re
from enum import Enum
from typing import Any, Dict, List, Optional

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
    r"/?kbs/(?P<kbid>[^/]+)/r/(?P<rid>[^/]+)/(?P<download_type>[fe])/(?P<field_type>\w)/(?P<field_id>[^/]+)/?(?P<key>.*)?"  # noqa
)
DOWNLOAD_TYPE_MAP = {"f": "field", "e": "extracted"}
DOWNLOAD_URI = "/kb/{kbid}/resource/{rid}/{field_type}/{field_id}/download/{download_type}/{key}"

_NOT_SET = object()


class ParamDefault(BaseModel):
    default: Any = None
    title: str
    description: str
    le: Optional[float] = None
    gt: Optional[float] = None
    max_items: Optional[int] = None
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
    filename: Optional[str] = None
    content_type: str = "application/octet-stream"
    payload: Optional[str] = Field(None, description="Base64 encoded file content")
    md5: Optional[str] = None
    # These are to be used for external files
    uri: Optional[str] = None
    extra_headers: Dict[str, str] = {}

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
                result = hashlib.md5(base64.b64decode(self.payload))
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
    uri: Optional[str] = None
    size: Optional[int] = None
    content_type: Optional[str] = None
    bucket_name: Optional[str] = None

    class Source(Enum):
        FLAPS = "FLAPS"
        GCS = "GCS"
        S3 = "S3"
        LOCAL = "LOCAL"
        EXTERNAL = "EXTERNAL"

    source: Optional[Source]
    filename: Optional[str]
    resumable_uri: Optional[str]
    offset: Optional[int]
    upload_uri: Optional[str]
    parts: Optional[List[str]]
    old_uri: Optional[str]
    old_bucket: Optional[str]
    md5: Optional[str]


class CloudLink(BaseModel):
    uri: Optional[str] = None
    size: Optional[int] = None
    content_type: Optional[str] = None
    filename: Optional[str] = None
    md5: Optional[str] = None

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
    def serialize_uri(uri):
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


class FieldRef(BaseModel):
    field_type: FieldTypeName
    field_id: str
    split: Optional[str] = None


class Classification(BaseModel):
    labelset: str
    label: str


class UserClassification(Classification):
    cancelled_by_user: bool = False


class Sentence(BaseModel):
    start: Optional[int] = None
    end: Optional[int] = None
    key: Optional[str] = None


class PageInformation(BaseModel):
    page: Optional[int] = None
    page_with_visual: Optional[bool] = None


class Representation(BaseModel):
    is_a_table: Optional[bool] = None
    reference_file: Optional[str] = None


class ParagraphRelations(BaseModel):
    parents: list[str] = []
    siblings: list[str] = []
    replacements: list[str] = []


class Paragraph(BaseModel):
    start: Optional[int] = None
    end: Optional[int] = None
    start_seconds: Optional[List[int]] = None
    end_seconds: Optional[List[int]] = None

    class TypeParagraph(str, Enum):
        TEXT = "TEXT"
        OCR = "OCR"
        INCEPTION = "INCEPTION"
        DESCRIPTION = "DESCRIPTION"
        TRANSCRIPT = "TRANSCRIPT"
        TITLE = "TITLE"
        TABLE = "TABLE"

    kind: Optional[TypeParagraph] = None
    classifications: Optional[List[Classification]] = None
    sentences: Optional[List[Sentence]] = None
    key: Optional[str] = None
    page: Optional[PageInformation] = None
    representation: Optional[Representation] = None
    relations: Optional[ParagraphRelations] = None


class Shards(BaseModel):
    shards: Optional[List[str]] = None


class Question(BaseModel):
    text: str
    language: Optional[str] = None
    ids_paragraphs: List[str]


class Answer(BaseModel):
    text: str
    language: Optional[str] = None
    ids_paragraphs: List[str]


class QuestionAnswer(BaseModel):
    question: Question
    answers: List[Answer]


class QuestionAnswers(BaseModel):
    question_answer: List[QuestionAnswer]
