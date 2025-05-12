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
from pydantic import Field
from pydantic_settings import BaseSettings


class BackPressureSettings(BaseSettings):
    enabled: bool = Field(
        default=False,
        description="Enable or disable back pressure.",
        alias="back_pressure_enabled",
    )
    indexing_rate: float = Field(
        default=10,
        description="Estimation of the indexing rate in messages per second. This is used to calculate the try again in time",  # noqa
    )
    ingest_rate: float = Field(
        default=4,
        description="Estimation of the ingest processed consumer rate in messages per second. This is used to calculate the try again in time",  # noqa
    )
    processing_rate: float = Field(
        default=1,
        description="Estimation of the processing rate in messages per second. This is used to calculate the try again in time",  # noqa
    )
    max_indexing_pending: int = Field(
        default=1000,
        description="Max number of messages pending to index in a node queue before rate limiting writes. Set to 0 to disable indexing back pressure checks",  # noqa
        alias="back_pressure_max_indexing_pending",
    )
    max_ingest_pending: int = Field(
        # Disabled by default
        default=0,
        description="Max number of messages pending to be ingested by processed consumers before rate limiting writes. Set to 0 to disable ingest back pressure checks",  # noqa
        alias="back_pressure_max_ingest_pending",
    )
    max_processing_pending: int = Field(
        default=1000,
        description="Max number of messages pending to process per Knowledge Box before rate limiting writes. Set to 0 to disable processing back pressure checks",  # noqa
        alias="back_pressure_max_processing_pending",
    )
    indexing_check_interval: int = Field(
        default=30,
        description="Interval in seconds to check the indexing pending messages",
    )
    ingest_check_interval: int = Field(
        default=30,
        description="Interval in seconds to check the ingest pending messages",
    )
    max_wait_time: int = Field(
        default=60,
        description="Max time in seconds to wait before trying again after back pressure",
    )


settings = BackPressureSettings()
