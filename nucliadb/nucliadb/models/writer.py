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
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, validator

from nucliadb.models.conversation import InputConversationField
from nucliadb.models.datetime import FieldDatetime
from nucliadb.models.file import FileField
from nucliadb.models.keywordset import FieldKeywordset
from nucliadb.models.layout import InputLayoutField
from nucliadb.models.link import LinkField
from nucliadb.models.metadata import (
    InputMetadata,
    Origin,
    UserFieldMetadata,
    UserMetadata,
)
from nucliadb.models.processing import PushProcessingOptions
from nucliadb.models.text import TextField
from nucliadb.models.utils import FieldIdString, SlugString


class CreateResourcePayload(BaseModel):
    title: Optional[str] = None
    summary: Optional[str] = None
    slug: Optional[SlugString] = None
    icon: Optional[str] = None
    thumbnail: Optional[str] = None
    layout: Optional[str] = None
    metadata: Optional[InputMetadata] = None
    usermetadata: Optional[UserMetadata] = None
    fieldmetadata: Optional[List[UserFieldMetadata]] = None
    origin: Optional[Origin] = None

    files: Dict[FieldIdString, FileField] = {}
    links: Dict[FieldIdString, LinkField] = {}
    texts: Dict[FieldIdString, TextField] = {}
    layouts: Dict[FieldIdString, InputLayoutField] = {}
    conversations: Dict[FieldIdString, InputConversationField] = {}
    keywordsets: Dict[FieldIdString, FieldKeywordset] = {}
    datetimes: Dict[FieldIdString, FieldDatetime] = {}

    # Processing options
    processing_options: Optional[PushProcessingOptions] = PushProcessingOptions()

    @validator("icon")
    def icon_check(cls, v):

        if "/" not in v:
            raise ValueError("Icon should be a MIME string")

        if len(v.split("/")) != 2:
            raise ValueError("Icon needs two parts of MIME string")

        return v


class UpdateResourcePayload(BaseModel):
    title: Optional[str] = None
    summary: Optional[str] = None
    slug: Optional[SlugString] = None
    icon: Optional[str] = None
    thumbnail: Optional[str] = None
    layout: Optional[str] = None
    usermetadata: Optional[UserMetadata] = None
    fieldmetadata: Optional[List[UserFieldMetadata]] = None

    files: Dict[FieldIdString, FileField] = {}
    links: Dict[FieldIdString, LinkField] = {}
    texts: Dict[FieldIdString, TextField] = {}
    layouts: Dict[FieldIdString, InputLayoutField] = {}
    conversations: Dict[FieldIdString, InputConversationField] = {}
    keywordsets: Dict[FieldIdString, FieldKeywordset] = {}
    datetimes: Dict[FieldIdString, FieldDatetime] = {}

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


ComminResourcePayload = Union[CreateResourcePayload, UpdateResourcePayload]


class ResourceFileUploaded(BaseModel):
    seqid: Optional[int] = None
    uuid: Optional[str] = None
    field_id: Optional[FieldIdString] = None
