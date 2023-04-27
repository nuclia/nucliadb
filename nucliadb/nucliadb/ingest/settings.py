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
from typing import Dict, List, Optional

from pydantic import BaseSettings, Field


class DriverConfig(str, Enum):
    REDIS = "redis"
    TIKV = "tikv"
    PG = "pg"
    LOCAL = "local"
    NOT_SET = "notset"  # setting not provided

    @classmethod
    def _missing_(cls, value):
        """
        allow case insensitive enum values
        """
        for member in cls:
            if member.value == value.lower():
                return member


class DriverSettings(BaseSettings):
    driver: DriverConfig = Field(DriverConfig.NOT_SET, description="K/V storage driver")
    driver_redis_url: Optional[str] = None
    driver_tikv_url: Optional[List[str]] = []
    driver_local_url: Optional[str] = None
    driver_pg_url: Optional[str] = None


class Settings(DriverSettings):
    grpc_port: int = 8030

    partitions: List[str] = ["1"]

    pull_time: int = 100

    replica_number: int = -1
    total_replicas: int = 1
    nuclia_partitions: int = 50

    # NODE INFORMATION

    node_replicas: int = 2  # TODO discuss default value

    chitchat_binding_host: str = "0.0.0.0"
    chitchat_binding_port: int = 31337
    chitchat_enabled: bool = True

    # chitchat_peers_addr: List[str] = ["localhost:3001"] # TODO is it seed list?
    # swim_host_key: str = "ingest.key" # TODO: ask if it's swim specific keys?

    logging_config: Optional[str] = None

    node_writer_port: int = 10000
    node_reader_port: int = 10001
    node_sidecar_port: int = 10002

    # Only for testing proposes
    writer_port_map: Dict[str, int] = {}
    reader_port_map: Dict[str, int] = {}
    sidecar_port_map: Dict[str, int] = {}

    # Node limits
    max_shard_fields: int = 200000  # max number of fields to target per shard
    max_node_replicas: int = (
        600  # max number of shard replicas a single node will manage
    )

    local_reader_threads: int = 5
    local_writer_threads: int = 5

    max_receive_message_length: int = 4

    # Search query timeouts
    relation_search_timeout: float = 10.0
    relation_types_timeout: float = 10.0

    nodes_load_ingest: bool = False


settings = Settings()
