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
import asyncio
from typing import Dict, Optional

from nucliadb_telemetry import metrics
from redis import asyncio as aioredis
from redis.asyncio.client import PubSub

from nucliadb_utils.cache.exceptions import GroupNotSupported, NoPubsubConfigured
from nucliadb_utils.cache.pubsub import Callback, PubSubDriver

redis_observer = metrics.Observer("redis_pubsub", labels={"type": ""})


class RedisPubsub(PubSubDriver):
    pubsub: Optional[PubSub] = None
    task: Optional[asyncio.Task] = None
    initialized: bool = False
    driver: Optional[aioredis.Redis] = None
    async_callback = False

    def __init__(self, url: Optional[str] = None):
        if url is None:
            raise AttributeError("Invalid url parameter")
        self.url = url

    async def initialize(self):
        if self.initialized is False:
            self.driver = aioredis.from_url(self.url)
            self.pubsub = self.driver.pubsub(ignore_subscribe_messages=True)
            self.initialized = True

    async def finalize(self):
        await self.pubsub.unsubscribe()
        if self.task:
            self.task.cancel()
        if self.driver:
            await self.driver.close(close_connection_pool=True)
        self.initialized = False

    async def publish(self, key: str, value: bytes):
        if self.driver is None:
            raise NoPubsubConfigured()

        with redis_observer(labels={"type": "publish"}):
            await self.driver.publish(key, value)

    async def unsubscribe(self, key: str, subscription_id: Optional[str] = None):
        if self.driver is None or self.pubsub is None:
            raise NoPubsubConfigured()

        await self.pubsub.unsubscribe(key)

    async def subscribe(
        self,
        handler: Callback,
        key: str,
        group: Optional[str] = None,
        subscription_id: Optional[str] = None,
    ):
        if group is not None:
            raise GroupNotSupported()
        if self.driver is None or self.pubsub is None:
            raise NoPubsubConfigured()

        subscription = {key: handler}
        await self.pubsub.subscribe(**subscription)  # type: ignore
        if self.task is None:
            self.task = asyncio.create_task(self.pubsub.run())

    def parse(self, data: Dict[str, str]):
        return data["data"]
