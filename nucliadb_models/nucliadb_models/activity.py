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


class ResourceOperationType(str, Enum):
    CREATED = "created"
    MODIFIED = "modified"
    DELETED = "deleted"


class NotificationAction(str, Enum):
    COMMIT = "commit"
    INDEXED = "indexed"
    ABORTED = "aborted"


class ResourceNotification(BaseModel):
    kbid: str = Field(
        ...,
        title="KnowledgeBox ID",
        description="Id of the KnowledgeBox that the resource belongs to.",
    )
    resource_uuid: str = Field(
        ..., title="Resource UUID", description="UUID of the resource."
    )
    seqid: int = Field(
        ...,
        title="Sequence ID",
        description="Sequence ID of the resource operation. This can be used to track completion of specific operations.",  # noqa: E501
    )
    operation_type: ResourceOperationType = Field(
        ...,
        title="Operation Type",
        description="Type of operation performed on the resource.",
    )
    action: NotificationAction = Field(
        ...,
        title="Action",
        description="Notification action. Allows to distinguish between a notification of a resource being committed, indexed or aborted.",  # noqa: E501
    )
