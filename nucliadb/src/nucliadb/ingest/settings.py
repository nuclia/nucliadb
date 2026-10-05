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

from pydantic import Field
from pydantic_settings import BaseSettings


class DriverConfig(Enum):
    MARKLOGIC = "marklogic"
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
    driver: DriverConfig = Field(default=DriverConfig.MARKLOGIC, description="K/V storage driver")
    driver_marklogic_uri: str = "http://127.0.0.1"
    driver_marklogic_username: str = "admin"
    driver_marklogic_password: str = "admin"
    driver_marklogic_database: str = "nucliadb-system"
    driver_marklogic_port: int = 8000
    driver_marklogic_admin_port: int = 8002


class CatalogConfig(Enum):
    UNSET = "unset"


# For use during migration from pull v1 to pull v2
class ProcessingPullMode(Enum):
    OFF = "off"
    V1 = "v1"
    V2 = "v2"


class Settings(DriverSettings):
    # Catalog settings
    catalog: CatalogConfig = Field(default=CatalogConfig.UNSET, description="Catalog backend")

    # Pull worker settings
    pull_time_error_backoff: int = 30
    pull_api_timeout: int = 60
    disable_pull_worker: bool = Field(
        default=False, description="Set to true to disable the pull worker task"
    )

    # Ingest consumer sts replica settings
    replica_number: int = Field(
        default=-1,
        description="The replica number of this ingest statefulset instance. Leave to -1 to auto-assign based on hostname.",
    )
    total_replicas: int = Field(default=1, description="Number of ingest statefulset replicas deployed")
    nuclia_partitions: int = Field(
        default=50, description="Total number of partitions of the nats stream."
    )
    partitions: list[str] = Field(
        default=["1"],
        description="List of partitions assigned to this ingest statefulset instance. This is automatically assigned based on the replica number and total replicas.",
    )
    max_concurrent_ingest_processing: int = Field(
        default=5,
        description="Controls the number of concurrent messages from different partitions that can be processed at the same time by ingest statefulset consumers.",
    )

    # Grpc server settings
    grpc_port: int = 8030
    max_receive_message_length: int = Field(
        default=500, description="Maximum receive grpc message length in MB."
    )

    # Search query timeouts
    relation_search_timeout: float = 10.0
    relation_types_timeout: float = 10.0


settings = Settings()
