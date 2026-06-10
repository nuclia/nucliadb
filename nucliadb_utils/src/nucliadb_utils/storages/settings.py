# Copyright 2021 Bosutech XXI S.L.
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

import os

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    gcs_deadletter_bucket: str | None = None
    gcs_indexing_bucket: str | None = None

    gcs_threads: int = 3
    gcs_labels: dict[str, str] = {}

    s3_deadletter_bucket: str | None = None
    s3_indexing_bucket: str | None = None

    azure_deadletter_bucket: str | None = None
    azure_indexing_bucket: str | None = None

    local_testing_files: str = os.path.dirname(__file__)


settings = Settings()
