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
    dm_enabled: bool = True
    dm_redis_host: Optional[str] = None
    dm_redis_port: Optional[int] = None


class BackPressureSettings(BaseSettings):
    estimation_indexing_rate: float = Field(
        default=2,
        description="Estimation of the indexing rate in messages per second. This is used to calculate the try again in time",  # noqa
    )
    max_node_indexing_pending: int = Field(
        default=100,
        description="Max number of messages pending to index in a node queue before rate limiting writes",
    )


settings = Settings()

back_pressure_settings = BackPressureSettings()
