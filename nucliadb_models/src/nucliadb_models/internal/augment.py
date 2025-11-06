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
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, Discriminator, Field, StringConstraints, Tag, model_validator
from typing_extensions import Self

from nucliadb_models import hydration


class SelectProp(BaseModel):
    prop: Any

    @model_validator(mode="after")
    def set_discriminator(self) -> Self:
        # Ensure discriminator is explicitly set so it's always serialized
        self.prop = self.prop
        return self


class VirtualSelectProp(SelectProp): ...


def prop_discriminator(v: Any) -> str | None:
    if isinstance(v, dict):
        return v.get("prop", None)
    else:
        return getattr(v, "prop", None)


def from_discriminator(v: Any) -> str | None:
    if isinstance(v, dict):
        return v.get("from", None)
    else:
        return getattr(v, "from", None)


# Ids

ResourceId = UUID

FieldId = Annotated[
    str,
    StringConstraints(
        pattern=r"^[0-9a-f]{32}/[acftu]/[a-zA-Z0-9:_-]+(/[^/]{1,128})?$",
        min_length=32 + 1 + 1 + 1 + 1 + 0 + 0,
        # max field id of 250
        max_length=32 + 1 + 1 + 1 + 250 + 1 + 218,
    ),
]

ParagraphId = Annotated[
    str,
    StringConstraints(
        # resource-uuid/field-type/field-id/[split-id/]paragraph-id
        pattern=r"^[0-9a-f]{32}/[acftu]/[a-zA-Z0-9:_-]+(/[^/]{1,128})?/[0-9]+-[0-9]+$",
        min_length=32 + 1 + 1 + 1 + 1 + 0 + 0 + 1 + 3,
        # max field id of 250 and 10 digit paragraphs. More than enough
        max_length=32 + 1 + 1 + 1 + 250 + 1 + 128 + 1 + 21,
    ),
]

# SELECT props


class ParagraphText(SelectProp):
    prop: Literal["text"] = "text"


class ParagraphImage(SelectProp):
    prop: Literal["image"] = "image"


class ParagraphTable(SelectProp):
    prop: Literal["table"] = "table"


class RelatedParagraphs(SelectProp):
    prop: Literal["related"] = "related"
    neighbours: hydration.NeighbourParagraphHydration


ParagraphProp = Annotated[
    (
        Annotated[ParagraphText, Tag("text")]
        | Annotated[ParagraphImage, Tag("image")]
        | Annotated[ParagraphTable, Tag("table")]
        | Annotated[RelatedParagraphs, Tag("related")]
    ),
    Discriminator(prop_discriminator),
]


class FieldText(SelectProp):
    prop: Literal["text"] = "text"


class FieldValue(SelectProp):
    prop: Literal["value"] = "value"


FieldProp = Annotated[
    (Annotated[FieldText, Tag("text")] | Annotated[FieldValue, Tag("value")]),
    Discriminator(prop_discriminator),
]


class ConversationAttachments(SelectProp):
    prop: Literal["attachments"] = "attachments"
    text: bool
    image: bool


class ResourceTitle(SelectProp):
    prop: Literal["title"] = "title"


class ResourceSummary(SelectProp):
    prop: Literal["summary"] = "summary"


class ResourceOrigin(SelectProp):
    prop: Literal["origin"] = "origin"


class ResourceSecurity(SelectProp):
    prop: Literal["security"] = "security"


class ResourceBasic(SelectProp):
    prop: Literal["basic"] = "basic"


class ResourceExtra(SelectProp):
    prop: Literal["extra"] = "extra"


class ResourceFieldsFilter(BaseModel):
    ids: list[str]


class ResourceFields(VirtualSelectProp):
    """Virtual property to access resource fields"""

    prop: Literal["fields"] = "fields"
    select: list[FieldProp]
    filter: ResourceFieldsFilter | None = None


ResourceProp = Annotated[
    (
        Annotated[ResourceTitle, Tag("title")]
        | Annotated[ResourceSummary, Tag("summary")]
        | Annotated[ResourceBasic, Tag("basic")]
        | Annotated[ResourceOrigin, Tag("origin")]
        | Annotated[ResourceExtra, Tag("extra")]
        | Annotated[ResourceSecurity, Tag("security")]
        | Annotated[ResourceFields, Tag("fields")]
    ),
    Discriminator(prop_discriminator),
]


# Augmentations


class ResourceAugment(BaseModel, extra="forbid"):
    given: list[ResourceId | FieldId | ParagraphId]
    select: list[ResourceProp]
    from_: Literal["resources"] = Field("resources", alias="from")


class ConversationAugmentLimits(BaseModel):
    max_messages: int | None = Field(default=15, ge=0)


class ConversationAugment(BaseModel, extra="forbid"):
    given: list[FieldId | ParagraphId]
    select: list[FieldProp | ConversationAttachments]
    from_: Literal["conversations"] = Field("conversations", alias="from")
    limits: ConversationAugmentLimits | None = Field(default_factory=ConversationAugmentLimits)


class FieldAugment(BaseModel, extra="forbid"):
    given: list[FieldId | ParagraphId]
    select: list[FieldProp]
    from_: Literal["fields"] = Field("fields", alias="from")


class ParagraphAugment(BaseModel, extra="forbid"):
    given: list[ParagraphId]
    select: list[ParagraphProp]
    from_: Literal["paragraphs"] = Field("paragraphs", alias="from")


class AugmentationLimits(BaseModel, extra="forbid"):
    # TODO: global augmentation limits (max chars, images, image size...)
    ...


Augment = Annotated[
    (
        Annotated[ResourceAugment, Tag("resources")]
        | Annotated[FieldAugment, Tag("fields")]
        | Annotated[ConversationAugment, Tag("conversations")]
        | Annotated[ParagraphAugment, Tag("paragraphs")]
    ),
    Discriminator(from_discriminator),
]


class AugmentRequest(BaseModel, extra="forbid"):
    augmentations: list[Augment] = Field(
        default_factory=list,
        description="List of augmentations to be performed",
    )

    limits: AugmentationLimits | None = Field(
        default=None,
        description="Global hydration limits applied to the whole request",
    )
