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


class Status(str, Enum):
    SCHEDULED = "scheduled"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"
    ERRORED = "errored"
    CANCELLED = "cancelled"


class Task(BaseModel):
    """
    Basic metadata for the async tasks
    """

    kbid: str
    task_id: str
    status: Status
    tries: int = 0
    created_at: datetime = datetime.utcnow()
    updated_at: datetime = datetime.utcnow()


class TaskNatsMessage(BaseModel):
    kbid: str
    task_id: str
    args: tuple[Any, ...] = tuple()
    kwargs: dict[str, Any] = dict()
