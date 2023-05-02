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

import pytest

from nucliadb_utils.cache.nats import NatsPubsub
from nucliadb_utils.cache.pubsub import PubSubDriver
from nucliadb_utils.cache.redis import RedisPubsub
from nucliadb_utils.cache.settings import settings
from nucliadb_utils.cache.utility import Cache
from nucliadb_utils.utilities import clear_global_cache


@pytest.mark.asyncio
async def test_redis_pubsub(redis):
    url = f"redis://{redis[0]}:{redis[1]}"
    settings.cache_pubsub_driver = "redis"
    settings.cache_pubsub_redis_url = url
    second_pubsub = RedisPubsub(url)
    await second_pubsub.initialize()
    await cache_validation(second_pubsub)
    await second_pubsub.finalize()
    clear_global_cache()


@pytest.mark.asyncio
async def test_nats_pubsub(natsd):
    settings.cache_pubsub_driver = "nats"
    settings.cache_pubsub_nats_url = [natsd]
    second_pubsub = NatsPubsub(hosts=[natsd], name="second")
    await second_pubsub.initialize()
    await cache_validation(second_pubsub)
    await second_pubsub.finalize()
    clear_global_cache()


async def cache_validation(second_pubsub: PubSubDriver):
    util: Cache = Cache()
    await util.initialize()

    util2: Cache = Cache(pubsub=second_pubsub)
    await util2.initialize()

    await asyncio.sleep(0.5)

    await util.set("/test", "Testing info")
    await util.get("/test") == "Testing info"

    await util2.set("/test", "Testing info 2", invalidate=True)
    await util2.get("/test") == "Testing info 2"

    retries = 5
    for _ in range(retries):
        await asyncio.sleep(1)
        if await util.get("/test") is None:
            break
    assert await util.get("/test") is None
    await util.delete("/test")

    assert await util2.get("/test") is None

    await util2.finalize()
    await util.finalize()
