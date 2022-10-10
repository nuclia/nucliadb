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
from typing import Dict, List, Optional

from pydantic import BaseSettings


class Settings(BaseSettings):
    grpc_port: int = 8030

    inner_metrics_host: str = "0.0.0.0"
    inner_metrics_port: int = 8081

    driver: str = "redis"  # redis | tikv
    driver_redis_url: Optional[str] = None
    driver_tikv_url: Optional[List[str]] = []
    driver_local_url: Optional[str] = None

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
    chitchat_sock_path = "/tmp/rust_python.sock"  # TODO discuss default value

    # chitchat_peers_addr: List[str] = ["localhost:3001"] # TODO is it seed list?
    # swim_host_key: str = "ingest.key" # TODO: ask if it's swim specific keys?

    monitor: bool = False
    monitor_port: int = 50101

    logging_config: Optional[str] = None

    node_writer_port: int = 10000
    node_reader_port: int = 10001
    node_sidecar_port: int = 100002

    # Only for testing proposes
    writer_port_map: Dict[int, int] = {}
    reader_port_map: Dict[int, int] = {}
    sidecar_port_map: Dict[int, int] = {}
    max_node_fields: int = 200000

    local_reader_threads = 5
    local_writer_threads = 5

    max_receive_message_length: int = 4


settings = Settings()
