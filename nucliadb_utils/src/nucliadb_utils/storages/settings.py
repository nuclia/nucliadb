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
