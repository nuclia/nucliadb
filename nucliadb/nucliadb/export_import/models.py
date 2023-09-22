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
from typing import Any, Optional

from pydantic import BaseModel


class ExportedItemType(str, Enum):
    RESOURCE = "RES"
    LABELS = "LAB"
    ENTITIES = "ENT"
    BINARY = "BIN"


class Status(str, Enum):
    SCHEDULED = "scheduled"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"
    ERRORED = "errored"


class TaskMetadata(BaseModel):
    id: str
    status: Status
    result: dict[str, Any] = {}
    tries: int = 0


class ExportMetadata(TaskMetadata):
    kbid: str
    resources_to_export: list[str] = []
    exported_resources: list[str] = []
    serialized_cloud_file: Optional[bytes] = None


class ImportMetadata(TaskMetadata):
    kbid: str


class ExportMessage(BaseModel):
    kbid: str
    export_id: str


class ImportMessage(BaseModel):
    kbid: str
    import_id: str
