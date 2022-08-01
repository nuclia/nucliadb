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

import nucliadb_models as models
from nucliadb_ingest.processing import PushProcessingOptions


class CreateResourcePayload(BaseModel):
    title: Optional[str] = None
    summary: Optional[str] = None
    slug: Optional[models.SlugString] = None
    icon: Optional[str] = None
    layout: Optional[str] = None
    metadata: Optional[models.InputMetadata] = None
    usermetadata: Optional[models.UserMetadata] = None
    fieldmetadata: Optional[List[models.UserFieldMetadata]] = None
    origin: Optional[models.Origin] = None

    files: Dict[str, models.FileField] = {}
    links: Dict[str, models.LinkField] = {}
    texts: Dict[str, models.TextField] = {}
    layouts: Dict[str, models.InputLayoutField] = {}
    conversations: Dict[str, models.InputConversationField] = {}
    keywordsets: Dict[str, models.FieldKeywordset] = {}
    datetimes: Dict[str, models.FieldDatetime] = {}

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
    slug: Optional[models.SlugString] = None
    icon: Optional[str] = None
    layout: Optional[str] = None
    usermetadata: Optional[models.UserMetadata] = None
    fieldmetadata: Optional[List[models.UserFieldMetadata]] = None

    files: Dict[str, models.FileField] = {}
    links: Dict[str, models.LinkField] = {}
    texts: Dict[str, models.TextField] = {}
    layouts: Dict[str, models.InputLayoutField] = {}
    conversations: Dict[str, models.InputConversationField] = {}
    keywordsets: Dict[str, models.FieldKeywordset] = {}
    datetimes: Dict[str, models.FieldDatetime] = {}

    # Processing options
    processing_options: Optional[PushProcessingOptions] = PushProcessingOptions()


class ResourceCreated(BaseModel):
    uuid: str
    seqid: Optional[int] = None


class ResourceUpdated(BaseModel):
    seqid: Optional[int] = None


class ResourceFieldAdded(BaseModel):
    seqid: Optional[int] = None


class ResourceDeleted(BaseModel):
    seqid: Optional[int] = None


ComminResourcePayload = Union[CreateResourcePayload, UpdateResourcePayload]
