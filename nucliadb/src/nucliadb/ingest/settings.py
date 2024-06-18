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
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings


class DriverConfig(Enum):
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
    driver: DriverConfig = Field(default=DriverConfig.NOT_SET, description="K/V storage driver")
    driver_local_url: Optional[str] = Field(
        default=None,
        description="Local path to store data on file system. Example: /nucliadb/data/main",
    )
    driver_pg_url: Optional[str] = Field(
        default=None,
        description="PostgreSQL DSN. The connection string to the PG server. Example: postgres://username:password@postgres:5432/nucliadb.",  # noqa
    )
    driver_pg_connection_pool_min_size: int = Field(
        default=10,
        description="PostgreSQL min pool size. The minimum number of connections to the PostgreSQL server.",
    )
    driver_pg_connection_pool_max_size: int = Field(
        default=20,
        description="PostgreSQL max pool size. The maximum number of connections to the PostgreSQL server.",
    )
    driver_pg_connection_pool_acquire_timeout_ms: int = Field(
        default=1000,
        description="PostgreSQL pool acquire timeout in ms. The maximum time to wait until a connection becomes available.",
    )


class Settings(DriverSettings):
    grpc_port: int = 8030

    partitions: list[str] = ["1"]

    pull_time_error_backoff: int = 30
    pull_api_timeout: int = 60
    disable_pull_worker: bool = False

    # ingest consumer sts replica settings
    replica_number: int = -1
    total_replicas: int = 1  # number of ingest processor replicas in the cluster
    nuclia_partitions: int = 50

    max_receive_message_length: int = 4

    # Search query timeouts
    relation_search_timeout: float = 10.0
    relation_types_timeout: float = 10.0

    max_concurrent_ingest_processing: int = 5


settings = Settings()
