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
from typing import Optional

from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    data_path: str = "./data/node"
    standalone_mode: bool = False

    node_replicas: int = 2

    chitchat_binding_host: str = "0.0.0.0"
    chitchat_binding_port: int = 31337

    logging_config: Optional[str] = None

    node_writer_port: int = 10000
    node_reader_port: int = 10001
    node_sidecar_port: int = 10002

    # Only for testing purposes
    writer_port_map: dict[str, int] = {}
    reader_port_map: dict[str, int] = {}
    sidecar_port_map: dict[str, int] = {}

    # Node limits
    max_shard_paragraphs: int = Field(
        500_000,
        title="Max shard paragraphs",
        description="Maximum number of paragraphs to target per shard",
    )
    max_node_replicas: int = Field(
        650,
        title="Max node replicas",
        description="Maximum number of shard replicas a single node will manage",
    )

    local_reader_threads: int = 5
    local_writer_threads: int = 5

    # if you're running locally without chitchat but want
    # to load cluster remote nodes manually, set this to True
    manual_load_cluster_nodes: bool = False


settings = Settings()
