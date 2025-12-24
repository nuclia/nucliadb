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
import json

from pydantic import BaseModel, Field, field_validator
from pydantic.json_schema import SkipJsonSchema

from nucliadb_models import content_types
from nucliadb_models.conversation import InputConversationField
from nucliadb_models.file import FileField
from nucliadb_models.link import LinkField
from nucliadb_models.metadata import (
    Extra,
    InputMetadata,
    InputOrigin,
    UserFieldMetadata,
    UserMetadata,
)
from nucliadb_models.processing import PushProcessingOptions
from nucliadb_models.security import ResourceSecurity
from nucliadb_models.text import TextField
from nucliadb_models.utils import FieldIdPattern, FieldIdString, SlugString


class FieldDefaults:
    title = Field(None, title="Title", max_length=2048)
    summary = Field(None, title="Summary")
    slug = Field(
        None,
        title="Slug",
        description="The slug is the user-defined id for the resource",
    )
    icon = Field(
        None,
        title="Icon",
        description="The icon should be a media type string: https://www.iana.org/assignments/media-types/media-types.xhtml",
    )

    files: dict[FieldIdString, FileField] = Field(
        {},
        title="Files",
        description=f"Dictionary of file fields to be added to the resource. The keys correspond to the field id, and must comply with the regex: {FieldIdPattern}",
    )
    links: dict[FieldIdString, LinkField] = Field(
        {},
        title="Links",
        description=f"Dictionary of link fields to be added to the resource. The keys correspond to the field id, and must comply with the regex: {FieldIdPattern}",
    )
    texts: dict[FieldIdString, TextField] = Field(
        {},
        title="Texts",
        description=f"Dictionary of text fields to be added to the resource. The keys correspond to the field id, and must comply with the regex: {FieldIdPattern}",
    )
    conversations: dict[FieldIdString, InputConversationField] = Field(
        {},
        title="Conversations",
        description=f"Dictionary of conversation fields to be added to the resource. The keys correspond to the field id, and must comply with the regex: {FieldIdPattern}",
    )


class CreateResourcePayload(BaseModel):
    title: str | None = FieldDefaults.title
    summary: str | None = FieldDefaults.summary
    slug: SlugString | None = FieldDefaults.slug
    icon: str | None = FieldDefaults.icon
    thumbnail: str | None = None
    metadata: InputMetadata | None = Field(
        default=None,
        title="Metadata",
        description="Generic metadata for the resource. It can be used to store structured information about the resource that later is serialized on retrieval results, however this metadata can not be used for searching or filtering.",
    )
    usermetadata: UserMetadata | None = None
    fieldmetadata: list[UserFieldMetadata] | None = None
    origin: InputOrigin | None = Field(
        default=None,
        title="Origin",
        description="Origin metadata for the resource. Used to store information about the resource on the origin system. Most of its fields can later be used to filter at search time.",
    )
    extra: Extra | None = Field(
        default=None,
        title="Extra",
        description="Extra metadata for the resource. It can be used to store structured information about the resource that can't be used to query at retrieval time.",
    )
    hidden: bool | None = Field(
        default=None,
        title="Hidden",
        description="Set the hidden status of the resource. If not set, the default value for new resources in the KnowledgeBox will be used.",
    )

    files: dict[FieldIdString, FileField] = FieldDefaults.files
    links: dict[FieldIdString, LinkField] = FieldDefaults.links
    texts: dict[FieldIdString, TextField] = FieldDefaults.texts
    conversations: dict[FieldIdString, InputConversationField] = FieldDefaults.conversations
    processing_options: PushProcessingOptions | None = Field(
        default=PushProcessingOptions(),
        description="Options for processing the resource. If not set, the default options will be used.",
    )
    security: ResourceSecurity | None = Field(
        default=None,
        title="Security",
        description="Security metadata for the resource. It can be used to have fine-grained control over who can access the resource.",
    )
    wait_for_commit: SkipJsonSchema[bool] = Field(
        default=True,
        title="Wait for commit",
        description="Wait until the new resource have been properly commited to the database (not processed). Setting this to false allow lower latency but new resources may not be accessible right away",
    )

    @field_validator("icon")
    @classmethod
    def icon_check(cls, v):
        if v is None:
            return v
        if not content_types.valid(v):
            raise ValueError(f"Icon is not a valid MIME string: {v}")
        return v

    @field_validator("extra")
    @classmethod
    def extra_check(cls, value):
        limit = 400_000
        if value and value.metadata and len(json.dumps(value.metadata)) > limit:
            raise ValueError(f"metadata should be less than {limit} bytes when serialized to JSON")
        return value


class UpdateResourcePayload(BaseModel):
    title: str | None = FieldDefaults.title
    summary: str | None = FieldDefaults.summary
    slug: SlugString | None = FieldDefaults.slug
    thumbnail: str | None = None
    metadata: InputMetadata | None = None
    usermetadata: UserMetadata | None = None
    fieldmetadata: list[UserFieldMetadata] | None = None
    origin: InputOrigin | None = None
    extra: Extra | None = Field(
        default=None,
        title="Extra",
        description="Extra metadata for the resource. It can be used to store structured information about the resource that can't be used to query at retrieval time. If not set, the existing extra metadata will not be modified.",
    )
    files: dict[FieldIdString, FileField] = FieldDefaults.files
    links: dict[FieldIdString, LinkField] = FieldDefaults.links
    texts: dict[FieldIdString, TextField] = FieldDefaults.texts
    conversations: dict[FieldIdString, InputConversationField] = FieldDefaults.conversations
    processing_options: PushProcessingOptions | None = Field(
        default=PushProcessingOptions(),
        description="Options for processing the resource. If not set, the default options will be used.",
    )
    security: ResourceSecurity | None = Field(
        default=None,
        title="Security",
        description="Security metadata for the resource. It can be used to have fine-grained control over who can access the resource.",
    )
    hidden: bool | None = Field(
        default=None,
        title="Hidden",
        description="Modify the hidden status of the resource. If not set, the hidden status will not be modified.",
    )


class ResourceCreated(BaseModel):
    uuid: str
    elapsed: float | None = None
    seqid: int | None = None


class ResourceUpdated(BaseModel):
    seqid: int | None = None


class ResourceFieldAdded(BaseModel):
    seqid: int | None = None


class ResourceDeleted(BaseModel):
    seqid: int | None = None


ComingResourcePayload = CreateResourcePayload | UpdateResourcePayload


class ResourceFileUploaded(BaseModel):
    seqid: int | None = None
    uuid: str | None = None
    field_id: FieldIdString | None = None
