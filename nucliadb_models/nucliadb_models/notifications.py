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
    data: BaseModel = Field(
        ...,
        title="Notification Data",
        description="Notification data.",
    )


class ResourceOperationType(str, Enum):
    CREATED = "created"
    MODIFIED = "modified"
    DELETED = "deleted"


class ResourceIndexed(BaseModel):
    resource_uuid: str = Field(
        ..., title="Resource UUID", description="UUID of the resource."
    )
    resource_title: str = Field(
        ..., title="Resource Title", description="Title of the resource."
    )
    seqid: int = Field(
        ...,
        title="Sequence ID",
        description="Sequence ID of the resource operation. This can be used to track completion of specific operations.",  # noqa: E501
    )


class ResourceWritten(BaseModel):
    resource_uuid: str = Field(
        ..., title="Resource UUID", description="UUID of the resource."
    )
    resource_title: str = Field(
        ..., title="Resource Title", description="Title of the resource."
    )
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
    resource_uuid: str = Field(
        ..., title="Resource UUID", description="UUID of the resource."
    )
    resource_title: str = Field(
        ..., title="Resource Title", description="Title of the resource."
    )
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
