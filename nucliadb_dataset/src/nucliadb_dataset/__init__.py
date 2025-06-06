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

from enum import Enum
from typing import Dict

from nucliadb_dataset.dataset import NucliaDBDataset, Task, download_all_partitions
from nucliadb_dataset.nuclia import NucliaDriver

NUCLIA_GLOBAL: Dict[str, NucliaDriver] = {}

CLIENT_ID = "CLIENT"


class ExportType(str, Enum):
    DATASETS = "DATASETS"
    FILESYSTEM = "FILESYSTEM"


__all__ = (
    "NucliaDBDataset",
    "Task",
    "download_all_partitions",
    "NUCLIA_GLOBAL",
    "CLIENT_ID",
    "ExportType",
)
