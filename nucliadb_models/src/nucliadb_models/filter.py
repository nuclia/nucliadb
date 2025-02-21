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

from enum import Enum
from typing import Generic, Literal, Optional, TypeVar, Union

import pydantic
from pydantic import BaseModel, model_validator
from typing_extensions import Self

from .common import FieldTypeName, Paragraph
from .utils import DateTime

F = TypeVar("F")


class And(BaseModel, Generic[F]):
    """AND of other expressions"""

    operands: list[F] = pydantic.Field(alias="and")


class Or(BaseModel, Generic[F]):
    """OR of other expressions"""

    operands: list[F] = pydantic.Field(alias="or")


class Not(BaseModel, Generic[F]):
    """NOT another expression"""

    operand: F = pydantic.Field(alias="not")


class Resource(BaseModel):
    """Matches all fields of a resource given its id or slug"""

    prop: Literal["resource"]
    id: Optional[str] = pydantic.Field(default=None, description="ID of the resource to match")
    slug: Optional[str] = pydantic.Field(default=None, description="Slug of the resource to match")

    @model_validator(mode="after")
    def single_field(self) -> Self:
        if self.id is not None and self.slug is not None:
            raise ValueError("Must set only one of `id` and `slug`")
        if self.id is None and self.slug is None:
            raise ValueError("Must set `id` or `slug`")
        return self


class Field(BaseModel):
    """Matches a field or set of fields"""

    prop: Literal["field"]
    type: FieldTypeName = pydantic.Field(description="Type of the field to match, ")
    name: Optional[str] = pydantic.Field(
        default=None,
        description="Name of the field to match. If blank, matches all fields of the given type",
    )


class Keyword(BaseModel):
    """Matches all fields that contain a keyword"""

    prop: Literal["keyword"]
    word: str = pydantic.Field(description="Keyword to find")


class DateCreated(BaseModel):
    """Matches all fields created in a date range"""

    prop: Literal["created"]
    since: Optional[DateTime] = pydantic.Field(
        default=None, description="Start of the date range. Leave blank for unbounded"
    )
    until: Optional[DateTime] = pydantic.Field(
        default=None, description="End of the date range. Leave blank for unbounded"
    )

    @model_validator(mode="after")
    def some_set(self) -> Self:
        if self.since is None and self.until is None:
            raise ValueError("Must set `since` or `until` (or both)")
        return self


class DateModified(BaseModel):
    """Matches all fields modified in a date range"""

    prop: Literal["modified"]
    since: Optional[DateTime] = pydantic.Field(
        default=None, description="Start of the date range. Leave blank for unbounded"
    )
    until: Optional[DateTime] = pydantic.Field(
        default=None, description="End of the date range. Leave blank for unbounded"
    )

    @model_validator(mode="after")
    def some_set(self) -> Self:
        if self.since is None and self.until is None:
            raise ValueError("Must set `since` or `until` (or both)")
        return self


class OriginTag(BaseModel):
    """Matches all fields with a given origin tag"""

    prop: Literal["origin_tag"]
    tag: str = pydantic.Field(description="The tag to match")


class Label(BaseModel):
    """Matches fields/paragraphs with a label (or labelset)"""

    prop: Literal["label"]
    labelset: str = pydantic.Field(description="The labelset to match")
    label: Optional[str] = pydantic.Field(
        default=None,
        description="The label to match. If blank, matches all labels in the given labelset",
    )


class ResourceMimetype(BaseModel):
    """Matches resources with a mimetype.

    The mimetype of a resource can be assigned independently of the mimetype of its fields.
    In resources with multiple fields, you may prefer to use `field_mimetype`"""

    prop: Literal["resource_mimetype"]
    type: str = pydantic.Field(
        description="Type of the mimetype to match. e.g: In image/jpeg, type is image"
    )
    subtype: Optional[str] = pydantic.Field(
        default=None,
        description=(
            "Type of the mimetype to match. e.g: In image/jpeg, subtype is jpeg."
            "Leave blank to match all mimetype of the type"
        ),
    )


class FieldMimetype(BaseModel):
    """Matches fields with a mimetype"""

    prop: Literal["field_mimetype"]
    type: str = pydantic.Field(
        description="Type of the mimetype to match. e.g: In image/jpeg, type is image"
    )
    subtype: Optional[str] = pydantic.Field(
        default=None,
        description=(
            "Type of the mimetype to match. e.g: In image/jpeg, subtype is jpeg."
            "Leave blank to match all mimetype of the type"
        ),
    )


class Entity(BaseModel):
    """Matches fields that contains a detected entity"""

    prop: Literal["entity"]
    subtype: str = pydantic.Field(description="Type of the entity. e.g: PERSON")
    value: Optional[str] = pydantic.Field(
        default=None,
        description="Value of the entity. e.g: Anna. If blank, matches any entity of the given type",
    )


class Language(BaseModel):
    """Matches the language of the field"""

    prop: Literal["language"]
    only_primary: bool = pydantic.Field(
        default=False,
        description="Match only the primary language of the document. By default, matches any language that appears in the document",
    )
    language: str = pydantic.Field(description="The code of the language to match, e.g: en")


class OriginMetadata(BaseModel):
    """Matches metadata from the origin"""

    prop: Literal["origin_metadata"]
    field: str = pydantic.Field(description="Metadata field")
    value: Optional[str] = pydantic.Field(
        default=None,
        description="Value of the metadata field. If blank, matches any document with the given metadata field set (to any value)",
    )


class OriginPath(BaseModel):
    """Matches the origin path"""

    prop: Literal["origin_path"]
    prefix: str = pydantic.Field(
        description=(
            "Prefix of the path, matches all paths under this prefix"
            "e.g: `prefix=/dir/` matches `/dir` and `/dir/a/b` but not `/dirrrr`"
        )
    )


class Generated(BaseModel):
    """Matches if the field was generated by the given source"""

    prop: Literal["generated"]
    by: Literal["data-augmentation"] = pydantic.Field(
        description="Generator for this field. Currently, only data-augmentation is supported"
    )
    da_task: Optional["str"] = pydantic.Field(
        default=None, description="Matches field generated by an specific DA task, given its prefix"
    )


class Kind(BaseModel):
    """Matches paragraphs of a certain kind"""

    prop: Literal["kind"]
    kind: Paragraph.TypeParagraph = pydantic.Field(description="The kind of paragraph to match")


FieldFilterExpression = Union[
    And["FieldFilterExpression"],
    Or["FieldFilterExpression"],
    Not["FieldFilterExpression"],
    Resource,
    Field,
    Keyword,
    DateCreated,
    DateModified,
    OriginTag,
    Label,
    ResourceMimetype,
    FieldMimetype,
    Entity,
    Language,
    OriginMetadata,
    OriginPath,
    Generated,
]

ParagraphFilterExpression = Union[
    And["ParagraphFilterExpression"],
    Or["ParagraphFilterExpression"],
    Not["ParagraphFilterExpression"],
    Label,
    Kind,
]


class FilterExpression(BaseModel):
    """Returns only documents that match this filter expression.
    Filtering examples can be found here: https://docs.nuclia.dev/docs/rag/advanced/search/#filters

    This allows building complex filtering expressions and replaces the following parameters:
    `fields`, `filters`, `range_*`, `resource_filters`, `keyword_filters`.
    """

    class Operator(str, Enum):
        AND = "and"
        OR = "or"

    field: Optional[FieldFilterExpression] = pydantic.Field(
        default=None, description="Filter to apply to fields"
    )
    paragraph: Optional[ParagraphFilterExpression] = pydantic.Field(
        default=None, description="Filter to apply to each text block"
    )
    operator: Operator = pydantic.Field(
        default=Operator.AND,
        description=(
            "How to combine field and paragraph filters (default is AND)."
            "AND returns text blocks that match both filters."
            "OR returns text_blocks that match one of the two filters"
        ),
    )
