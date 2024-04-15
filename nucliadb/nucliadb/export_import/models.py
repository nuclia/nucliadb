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
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel

from nucliadb_models.export_import import Status


class ExportedItemType(str, Enum):
    """
    Enum for the different types of items that can be exported.
    This is used when generating and parsing the bytes of an export.
    """

    RESOURCE = "RES"
    LABELS = "LAB"
    ENTITIES = "ENT"
    BINARY = "BIN"
    LEARNING_CONFIG = "LEA"


ExportItem = tuple[ExportedItemType, Any]


class TaskMetadata(BaseModel):
    status: Status
    retries: int = 0


class Metadata(BaseModel):
    """
    Model for the state metadata of the exports and imports that is stored on maindb
    """

    kbid: str
    id: str
    task: TaskMetadata = TaskMetadata(status=Status.SCHEDULED)
    total: int = 0
    processed: int = 0
    created: datetime = datetime.utcnow()
    modified: datetime = datetime.utcnow()


class ExportMetadata(Metadata):
    resources_to_export: list[str] = list()
    exported_resources: list[str] = list()


class ImportMetadata(Metadata): ...


class NatsTaskMessage(BaseModel):
    """
    Model for the messages sent through NATS to start exports and imports
    """

    kbid: str
    id: str
