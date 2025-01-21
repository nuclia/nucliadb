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
from typing import Dict, List, Optional, Union

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
    title = Field(None, title="Title")
    summary = Field(None, title="Summary")
    slug = Field(
        None,
        title="Slug",
        description="The slug is the user-defined id for the resource",
    )
    icon = Field(
        None,
        title="Icon",
        description="The icon should be a media type string: https://www.iana.org/assignments/media-types/media-types.xhtml",  # noqa
    )

    files: Dict[FieldIdString, FileField] = Field(
        {},
        title="Files",
        description=f"Dictionary of file fields to be added to the resource. The keys correspond to the field id, and must comply with the regex: {FieldIdPattern}",  # noqa
    )
    links: Dict[FieldIdString, LinkField] = Field(
        {},
        title="Links",
        description=f"Dictionary of link fields to be added to the resource. The keys correspond to the field id, and must comply with the regex: {FieldIdPattern}",  # noqa
    )
    texts: Dict[FieldIdString, TextField] = Field(
        {},
        title="Texts",
        description=f"Dictionary of text fields to be added to the resource. The keys correspond to the field id, and must comply with the regex: {FieldIdPattern}",  # noqa
    )
    conversations: Dict[FieldIdString, InputConversationField] = Field(
        {},
        title="Conversations",
        description=f"Dictionary of conversation fields to be added to the resource. The keys correspond to the field id, and must comply with the regex: {FieldIdPattern}",  # noqa
    )


class CreateResourcePayload(BaseModel):
    title: Optional[str] = FieldDefaults.title
    summary: Optional[str] = FieldDefaults.summary
    slug: Optional[SlugString] = FieldDefaults.slug
    icon: Optional[str] = FieldDefaults.icon
    thumbnail: Optional[str] = None
    metadata: Optional[InputMetadata] = Field(
        default=None,
        title="Metadata",
        description="Generic metadata for the resource. It can be used to store structured information about the resource that later is serialized on retrieval results, however this metadata can not be used for searching or filtering.",
    )
    usermetadata: Optional[UserMetadata] = None
    fieldmetadata: Optional[List[UserFieldMetadata]] = None
    origin: Optional[InputOrigin] = Field(
        default=None,
        title="Origin",
        description="Origin metadata for the resource. Used to store information about the resource on the origin system. Most of its fields can later be used to filter at search time.",  # noqa
    )
    extra: Optional[Extra] = Field(
        default=None,
        title="Extra",
        description="Extra metadata for the resource. It can be used to store structured information about the resource that can't be used to query at retrieval time.",
    )
    hidden: Optional[bool] = Field(
        default=None,
        title="Hidden",
        description="Set the hidden status of the resource. If not set, the default value for new resources in the KnowledgeBox will be used.",
    )

    files: Dict[FieldIdString, FileField] = FieldDefaults.files
    links: Dict[FieldIdString, LinkField] = FieldDefaults.links
    texts: Dict[FieldIdString, TextField] = FieldDefaults.texts
    conversations: Dict[FieldIdString, InputConversationField] = FieldDefaults.conversations
    processing_options: Optional[PushProcessingOptions] = Field(
        default=PushProcessingOptions(),
        description="Options for processing the resource. If not set, the default options will be used.",
    )
    security: Optional[ResourceSecurity] = Field(
        default=None,
        title="Security",
        description="Security metadata for the resource. It can be used to have fine-grained control over who can access the resource.",  # noqa
    )
    wait_for_commit: SkipJsonSchema[bool] = Field(
        default=True,
        title="Wait for commit",
        description="Wait until the new resource have been properly commited to the database (not processed). Setting this to false allow lower latency but new resources may not be accessible right away",  # noqa: E501
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
    title: Optional[str] = FieldDefaults.title
    summary: Optional[str] = FieldDefaults.summary
    slug: Optional[SlugString] = FieldDefaults.slug
    thumbnail: Optional[str] = None
    metadata: Optional[InputMetadata] = None
    usermetadata: Optional[UserMetadata] = None
    fieldmetadata: Optional[List[UserFieldMetadata]] = None
    origin: Optional[InputOrigin] = None
    extra: Optional[Extra] = Field(
        default=None,
        title="Extra",
        description="Extra metadata for the resource. It can be used to store structured information about the resource that can't be used to query at retrieval time. If not set, the existing extra metadata will not be modified.",
    )
    files: Dict[FieldIdString, FileField] = FieldDefaults.files
    links: Dict[FieldIdString, LinkField] = FieldDefaults.links
    texts: Dict[FieldIdString, TextField] = FieldDefaults.texts
    conversations: Dict[FieldIdString, InputConversationField] = FieldDefaults.conversations
    processing_options: Optional[PushProcessingOptions] = Field(
        default=PushProcessingOptions(),
        description="Options for processing the resource. If not set, the default options will be used.",
    )
    security: Optional[ResourceSecurity] = Field(
        default=None,
        title="Security",
        description="Security metadata for the resource. It can be used to have fine-grained control over who can access the resource.",  # noqa
    )
    hidden: Optional[bool] = Field(
        default=None,
        title="Hidden",
        description="Modify the hidden status of the resource. If not set, the hidden status will not be modified.",  # noqa
    )


class ResourceCreated(BaseModel):
    uuid: str
    elapsed: Optional[float] = None
    seqid: Optional[int] = None


class ResourceUpdated(BaseModel):
    seqid: Optional[int] = None


class ResourceFieldAdded(BaseModel):
    seqid: Optional[int] = None


class ResourceDeleted(BaseModel):
    seqid: Optional[int] = None


ComingResourcePayload = Union[CreateResourcePayload, UpdateResourcePayload]


class ResourceFileUploaded(BaseModel):
    seqid: Optional[int] = None
    uuid: Optional[str] = None
    field_id: Optional[FieldIdString] = None
