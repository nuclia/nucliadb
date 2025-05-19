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

from pydantic import BaseModel


class CreateExportResponse(BaseModel):
    export_id: str


class CreateImportResponse(BaseModel):
    import_id: str


class NewImportedKbResponse(BaseModel):
    kbid: str
    slug: str


class Status(str, Enum):
    SCHEDULED = "scheduled"
    RUNNING = "running"
    FINISHED = "finished"
    ERRORED = "errored"


class StatusResponse(BaseModel):
    status: Status
    total: int = 0
    processed: int = 0
    retries: int = 0
