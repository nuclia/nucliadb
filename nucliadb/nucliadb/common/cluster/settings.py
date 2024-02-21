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
import enum

from pydantic import BaseSettings, Field


class ClusterDiscoveryMode(str, enum.Enum):
    MANUAL = "manual"
    KUBERNETES = "kubernetes"
    SINGLE_NODE = "single_node"


class Settings(BaseSettings):
    data_path: str = "./data/node"
    standalone_mode: bool = False
    standalone_node_port: int = Field(
        default=10009,
        title="Standalone node port",
        description="Port to use for standalone nodes to communication with each other through",
    )

    node_replicas: int = 2

    node_writer_port: int = 10000
    node_reader_port: int = 10001

    # Only for testing purposes
    writer_port_map: dict[str, int] = {}
    reader_port_map: dict[str, int] = {}

    # Node limits
    max_shard_paragraphs: int = Field(
        default=250_000,
        title="Max shard paragraphs",
        description="Maximum number of paragraphs to target per shard",
    )
    max_shard_fields: int = Field(
        default=125_000,
        title="Max shard fields",
        description="Maximum number of fields to target per shard. "
        "If this is reached before max_shard_paragraphs, we will create a new shard",
    )
    max_node_replicas: int = Field(
        default=800,
        title="Max node replicas",
        description="Maximum number of shard replicas a single node will manage",
    )
    max_resource_paragraphs: int = Field(
        default=50_000,
        title="Max paragraphs per resource",
        description="Maximum number of paragraphs allowed on a single resource",
    )

    local_reader_threads: int = 5
    local_writer_threads: int = 5

    cluster_discovery_mode: ClusterDiscoveryMode = ClusterDiscoveryMode.KUBERNETES
    cluster_discovery_kubernetes_namespace: str = "nucliadb"
    cluster_discovery_kubernetes_selector: str = "appType=node"
    cluster_discovery_manual_addresses: list[str] = []


settings = Settings()


def in_standalone_mode() -> bool:
    return settings.standalone_mode
