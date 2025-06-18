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
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class NotificationType(str, Enum):
    RESOURCE_WRITTEN = "resource_written"
    RESOURCE_PROCESSED = "resource_processed"
    RESOURCE_INDEXED = "resource_indexed"


class Notification(BaseModel):
    type: NotificationType = Field(
        ...,
        title="Notification Type",
        description="Type of notification.",
    )
    data: Any = Field(
        ...,
        title="Notification Data",
        description="Notification data.",
    )


class ResourceOperationType(str, Enum):
    CREATED = "created"
    MODIFIED = "modified"
    DELETED = "deleted"


class ResourceIndexed(BaseModel):
    resource_uuid: str = Field(..., title="Resource UUID", description="UUID of the resource.")
    resource_title: str = Field(..., title="Resource Title", description="Title of the resource.")
    seqid: int = Field(
        ...,
        title="Sequence ID",
        description="Sequence ID of the resource operation. This can be used to track completion of specific operations.",  # noqa: E501
    )


class ResourceWritten(BaseModel):
    resource_uuid: str = Field(..., title="Resource UUID", description="UUID of the resource.")
    resource_title: str = Field(..., title="Resource Title", description="Title of the resource.")
    seqid: int = Field(
        ...,
        title="Sequence ID",
        description="Sequence ID of the resource operation. This can be used to track completion of specific operations.",  # noqa: E501
    )
    operation: ResourceOperationType = Field(
        ..., title="Operation", description="Type of resource write operation."
    )
    error: bool = Field(
        default=False,
        title="Error",
        description="Indicates whether there were errors while pushing the resource to Nuclia processing",
    )


class ResourceProcessed(BaseModel):
    resource_uuid: str = Field(..., title="Resource UUID", description="UUID of the resource.")
    resource_title: str = Field(..., title="Resource Title", description="Title of the resource.")
    seqid: int = Field(
        ...,
        title="Sequence ID",
        description="Sequence ID of the resource operation. This can be used to track completion of specific operations.",  # noqa: E501
    )
    ingestion_succeeded: bool = Field(
        default=True,
        title="Succeeded",
        description="Indicates whether the processing results were successfully ingested by NucliaDB.",
    )
    processing_errors: bool = Field(
        default=False,
        title="Processing Errors",
        description="Indicates whether there were errors while Nuclia was processing the resource.",
    )


class ResourceWrittenNotification(Notification):
    type: NotificationType = NotificationType.RESOURCE_WRITTEN
    data: ResourceWritten = Field(
        ...,
        title="Data",
        description="Resource written notification payload",
    )


class ResourceProcessedNotification(Notification):
    type: NotificationType = NotificationType.RESOURCE_PROCESSED
    data: ResourceProcessed = Field(
        ...,
        title="Data",
        description="Resource processed notification payload",
    )


class ResourceIndexedNotification(Notification):
    type: NotificationType = NotificationType.RESOURCE_INDEXED
    data: ResourceIndexed = Field(
        ...,
        title="Data",
        description="Resource indexed notification payload",
    )
