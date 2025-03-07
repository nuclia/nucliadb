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
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings


class StandaloneNodeRole(enum.Enum):
    ALL = "all"
    INDEX = "index"
    WORKER = "worker"


class Settings(BaseSettings):
    data_path: str = "./data/node"
    standalone_mode: bool = False
    standalone_node_role: StandaloneNodeRole = StandaloneNodeRole.ALL

    # Index limits
    max_shard_paragraphs: int = Field(
        default=500_000,
        title="Max shard paragraphs",
        description="Maximum number of paragraphs to target per shard",
    )
    max_resource_paragraphs: int = Field(
        default=50_000,
        title="Max paragraphs per resource",
        description="Maximum number of paragraphs allowed on a single resource",
    )

    nidx_api_address: Optional[str] = Field(default=None, description="NIDX gRPC API address")
    nidx_searcher_address: Optional[str] = Field(
        default=None, description="NIDX gRPC searcher API address"
    )
    nidx_indexer_address: Optional[str] = Field(
        default=None, description="NIDX gRPC indexer API address"
    )


settings = Settings()


def in_standalone_mode() -> bool:
    return settings.standalone_mode
