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
from typing import List, Optional

from pydantic import BaseSettings


class Settings(BaseSettings):
    host_key_path: str = "node.key"
    force_host_id: Optional[str] = None

    writer_listen_address: str = "0.0.0.0:10000"
    reader_listen_address: str = "0.0.0.0:10001"
    sidecar_listen_address: str = "0.0.0.0:10002"

    data_path: Optional[str] = None


settings = Settings()


class IndexingSettings(BaseSettings):
    index_jetstream_target: Optional[str] = "node.{node}"
    index_jetstream_group: Optional[str] = "node-{node}"
    index_jetstream_stream: Optional[str] = "node"
    index_jetstream_servers: List[str] = []
    index_jetstream_auth: Optional[str] = None

    indexed_jetstream_target: str = "indexed.{partition}"
    indexed_jetstream_stream: str = "indexed"


indexing_settings = IndexingSettings()


class RunningSettings(BaseSettings):
    debug: bool = True
    log_level: str = "DEBUG"


running_settings = RunningSettings()
