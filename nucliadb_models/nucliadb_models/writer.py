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

from pydantic import BaseModel, Field, validator

from nucliadb_models.conversation import InputConversationField
from nucliadb_models.datetime import FieldDatetime
from nucliadb_models.file import FileField
from nucliadb_models.keywordset import FieldKeywordset
from nucliadb_models.layout import InputLayoutField
from nucliadb_models.link import LinkField
from nucliadb_models.metadata import (
    Extra,
    InputMetadata,
    Origin,
    UserFieldMetadata,
    UserMetadata,
)
from nucliadb_models.processing import PushProcessingOptions
from nucliadb_models.text import TextField
from nucliadb_models.utils import FieldIdString, SlugString
from nucliadb_models.vectors import UserVectorsWrapper

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
        description=f"Dictionary of file fields to be added to the resource. The keys correspond to the field id, and must comply with the regex: {FieldIdString.regex.pattern}",  # noqa
    )
    links = Field(
        {},
        title="Links",
        description=f"Dictionary of link fields to be added to the resource. The keys correspond to the field id, and must comply with the regex: {FieldIdString.regex.pattern}",  # noqa
    )
    texts = Field(
        {},
        title="Texts",
        description=f"Dictionary of text fields to be added to the resource. The keys correspond to the field id, and must comply with the regex: {FieldIdString.regex.pattern}",  # noqa
    )
    layouts = Field(
        {},
        title="Layouts",
        description=f"Dictionary of layout fields to be added to the resource. The keys correspond to the field id, and must comply with the regex: {FieldIdString.regex.pattern}",  # noqa
    )
    conversations = Field(
        {},
        title="Conversations",
        description=f"Dictionary of conversation fields to be added to the resource. The keys correspond to the field id, and must comply with the regex: {FieldIdString.regex.pattern}",  # noqa
    )
    keywordsets = Field(
        {},
        title="Keywordsets",
        description=f"Dictionary of keywordset fields to be added to the resource. The keys correspond to the field id, and must comply with the regex: {FieldIdString.regex.pattern}",  # noqa
    )
    datetimes = Field(
        {},
        title="Datetimes",
        description=f"Dictionary of datetime fields to be added to the resource. The keys correspond to the field id, and must comply with the regex: {FieldIdString.regex.pattern}",  # noqa
    )


class CreateResourcePayload(BaseModel):
    title: Optional[str] = FieldDefaults.title
    summary: Optional[str] = FieldDefaults.summary
    slug: Optional[SlugString] = FieldDefaults.slug
    icon: Optional[str] = FieldDefaults.icon
    thumbnail: Optional[str] = None
    layout: Optional[str] = None
    metadata: Optional[InputMetadata] = None
    usermetadata: Optional[UserMetadata] = None
    fieldmetadata: Optional[List[UserFieldMetadata]] = None
    uservectors: Optional[UserVectorsWrapper] = None
    origin: Optional[Origin] = None
    extra: Optional[Extra] = None

    files: Dict[FieldIdString, FileField] = FieldDefaults.files
    links: Dict[FieldIdString, LinkField] = FieldDefaults.links
    texts: Dict[FieldIdString, TextField] = FieldDefaults.texts
    layouts: Dict[FieldIdString, InputLayoutField] = FieldDefaults.layouts
    conversations: Dict[
        FieldIdString, InputConversationField
    ] = FieldDefaults.conversations
    keywordsets: Dict[FieldIdString, FieldKeywordset] = FieldDefaults.keywordsets
    datetimes: Dict[FieldIdString, FieldDatetime] = FieldDefaults.datetimes

    # Processing options
    processing_options: Optional[PushProcessingOptions] = PushProcessingOptions()

    @validator("icon")
    def icon_check(cls, v):
        if v is None:
            return v

        if "/" not in v:
            raise ValueError("Icon should be a MIME string")

        if len(v.split("/")) != 2:
            raise ValueError("Icon needs two parts of MIME string")

        return v

    @validator("extra")
    def extra_check(cls, value):
        limit = 400_000
        if value and value.metadata and len(json.dumps(value.metadata)) > limit:
            raise ValueError(
                f"metadata should be less than {limit} bytes when serialized to JSON"
            )
        return value


class UpdateResourcePayload(BaseModel):
    title: Optional[str] = FieldDefaults.title
    summary: Optional[str] = FieldDefaults.summary
    slug: Optional[SlugString] = FieldDefaults.slug
    icon: Optional[str] = FieldDefaults.icon
    thumbnail: Optional[str] = None
    layout: Optional[str] = None
    usermetadata: Optional[UserMetadata] = None
    uservectors: Optional[UserVectorsWrapper] = None
    fieldmetadata: Optional[List[UserFieldMetadata]] = None
    origin: Optional[Origin] = None
    extra: Optional[Extra] = None
    files: Dict[FieldIdString, FileField] = FieldDefaults.files
    links: Dict[FieldIdString, LinkField] = FieldDefaults.links
    texts: Dict[FieldIdString, TextField] = FieldDefaults.texts
    layouts: Dict[FieldIdString, InputLayoutField] = FieldDefaults.layouts
    conversations: Dict[
        FieldIdString, InputConversationField
    ] = FieldDefaults.conversations
    keywordsets: Dict[FieldIdString, FieldKeywordset] = FieldDefaults.keywordsets
    datetimes: Dict[FieldIdString, FieldDatetime] = FieldDefaults.datetimes

    # Processing options
    processing_options: Optional[PushProcessingOptions] = PushProcessingOptions()


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
