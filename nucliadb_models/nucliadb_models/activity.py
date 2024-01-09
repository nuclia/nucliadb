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
from typing import Optional

from pydantic import BaseModel, Field


class NotificationType(str, Enum):
    RESOURCE = "resource"


class NotificationData(BaseModel):
    kbid: str = Field(
        title="KnowledgeBox ID",
        description="Id of the KnowledgeBox that the notification belongs to.",
    )


class Notification(BaseModel):
    type: NotificationType = Field(
        ...,
        title="Notification Type",
        description="Type of notification.",
    )
    data: NotificationData = Field(
        ...,
        title="Notification Data",
        description="Notification data.",
    )


class ResourceOperationType(str, Enum):
    CREATED = "created"
    MODIFIED = "modified"
    DELETED = "deleted"


class ResourceActionType(str, Enum):
    COMMIT = "commit"
    INDEXED = "indexed"
    ABORT = "abort"


class ResourceNotificationSource(str, Enum):
    WRITER = "writer"
    PROCESSOR = "processor"


class ResourceNotificationData(NotificationData):
    resource_uuid: str = Field(
        ..., title="Resource UUID", description="UUID of the resource."
    )
    seqid: int = Field(
        ...,
        title="Sequence ID",
        description="Sequence ID of the resource operation. This can be used to track completion of specific operations.",  # noqa: E501
    )
    operation: Optional[ResourceOperationType] = Field(
        ...,
        title="Operation",
        description="Type of CRUD operation performed on the resource.",
    )
    action: ResourceActionType = Field(
        ...,
        title="Action",
        description="Notification action. Allows to distinguish between a notification of a resource being committed, indexed or aborted.",  # noqa: E501
    )
    source: Optional[ResourceNotificationSource] = Field(
        ...,
        title="Source",
        description="Source of the notification. Allows to distinguish between notifications coming from the writer (i.e: originated by HTTP interactions of the user) or from the processor (i.e: internal process pulling from Nuclia's processing queue).",  # noqa: E501
    )


class ResourceNotification(Notification):
    type: NotificationType = NotificationType.RESOURCE
    data: ResourceNotificationData = Field(
        ...,
        title="Notification Data",
        description="Notification data.",
    )
