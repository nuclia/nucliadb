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
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from nucliadb.common.back_pressure.settings import settings
from nucliadb_utils.nats import NatsConnectionManager


@dataclass
class BackPressureData:
    type: str
    try_after: datetime


class BackPressureException(Exception):
    def __init__(self, data: BackPressureData):
        self.data = data

    def __str__(self):
        return f"Back pressure applied for {self.data.type}. Try again after {self.data.try_after}"


def is_back_pressure_enabled() -> bool:
    return settings.enabled


def estimate_try_after(rate: float, pending: int, max_wait: int) -> datetime:
    """
    This function estimates the time to try again based on the rate and the number of pending messages.
    """
    delta_seconds = min(pending / rate, max_wait)
    return datetime.now(timezone.utc) + timedelta(seconds=delta_seconds)


async def get_nats_consumer_pending_messages(
    nats_manager: NatsConnectionManager, *, stream: str, consumer: str
) -> int:
    # get raw js client
    js = nats_manager.js
    consumer_info = await js.consumer_info(stream, consumer)
    return consumer_info.num_pending
