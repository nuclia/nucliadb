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

from nucliadb_models.conversation import InputConversationField
from nucliadb_models.file import FileField
from nucliadb_models.link import LinkField
from nucliadb_models.metadata import (
    Extra,
    InputMetadata,
    Origin,
    UserFieldMetadata,
    UserMetadata,
)
from nucliadb_models.processing import PushProcessingOptions
from nucliadb_models.security import ResourceSecurity
from nucliadb_models.text import TextField
from nucliadb_models.utils import FieldIdPattern, FieldIdString, SlugString

GENERIC_MIME_TYPE = "application/generic"


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

    files = Field(
        {},
        title="Files",
        description=f"Dictionary of file fields to be added to the resource. The keys correspond to the field id, and must comply with the regex: {FieldIdPattern}",  # noqa
    )
    links = Field(
        {},
        title="Links",
        description=f"Dictionary of link fields to be added to the resource. The keys correspond to the field id, and must comply with the regex: {FieldIdPattern}",  # noqa
    )
    texts = Field(
        {},
        title="Texts",
        description=f"Dictionary of text fields to be added to the resource. The keys correspond to the field id, and must comply with the regex: {FieldIdPattern}",  # noqa
    )
    conversations = Field(
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
    metadata: Optional[InputMetadata] = None
    usermetadata: Optional[UserMetadata] = None
    fieldmetadata: Optional[List[UserFieldMetadata]] = None
    origin: Optional[Origin] = None
    extra: Optional[Extra] = None

    files: Dict[FieldIdString, FileField] = FieldDefaults.files
    links: Dict[FieldIdString, LinkField] = FieldDefaults.links
    texts: Dict[FieldIdString, TextField] = FieldDefaults.texts
    conversations: Dict[FieldIdString, InputConversationField] = FieldDefaults.conversations
    processing_options: Optional[PushProcessingOptions] = PushProcessingOptions()
    security: Optional[ResourceSecurity] = Field(
        default=None,
        title="Security",
        description="Security metadata for the resource. It can be used to have fine-grained control over who can access the resource.",  # noqa
    )

    @field_validator("icon")
    @classmethod
    def icon_check(cls, v):
        if v is None:
            return v

        if "/" not in v:
            raise ValueError("Icon should be a MIME string")

        if len(v.split("/")) != 2:
            raise ValueError("Icon needs two parts of MIME string")

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
    origin: Optional[Origin] = None
    extra: Optional[Extra] = None
    files: Dict[FieldIdString, FileField] = FieldDefaults.files
    links: Dict[FieldIdString, LinkField] = FieldDefaults.links
    texts: Dict[FieldIdString, TextField] = FieldDefaults.texts
    conversations: Dict[FieldIdString, InputConversationField] = FieldDefaults.conversations
    processing_options: Optional[PushProcessingOptions] = PushProcessingOptions()
    security: Optional[ResourceSecurity] = Field(
        default=None,
        title="Security",
        description="Security metadata for the resource. It can be used to have fine-grained control over who can access the resource.",  # noqa
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
