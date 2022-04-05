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
import pytest

from nucliadb_ingest.maindb.redis import RedisDriver
from nucliadb_ingest.maindb.tikv import TiKVDriver


@pytest.mark.asyncio
async def test_redis_driver(redis):
    url = f"redis://{redis[0]}:{redis[1]}"
    driver = RedisDriver(url=url)
    await driver_basic(driver)


@pytest.mark.asyncio
async def test_tikv_driver(tikvd):
    url = [f"{tikvd[0]}:{tikvd[2]}"]
    driver = TiKVDriver(url=url)
    await driver_basic(driver)


async def driver_basic(driver):
    await driver.initialize()

    txn = await driver.begin()
    await txn.set("/internal/kbs/kb1/title", b"My title")
    await txn.set("/internal/kbs/kb1/shards/shard1", b"node1")

    await txn.set("/kbs/kb1/r/uuid1/text", b"My title")

    result = await txn.get("/kbs/kb1/r/uuid1/text")
    assert result == b"My title"

    await txn.commit(resource=False)

    txn = await driver.begin()
    result = await txn.get("/kbs/kb1/r/uuid1/text")
    assert result == b"My title"

    result = await txn.batch_get(
        ["/kbs/kb1/r/uuid1/text", "/internal/kbs/kb1/shards/shard1"]
    )

    await txn.commit(resource=False)

    async for key in driver.keys("/internal/kbs"):
        assert key in ["/internal/kbs/kb1/title", "/internal/kbs/kb1/shards/shard1"]

    txn = await driver.begin()
    result = await txn.delete("/internal/kbs/kb1/title")
    async for key in txn.keys("/internal/kbs"):
        assert key in ["/internal/kbs/kb1/title", "/internal/kbs/kb1/shards/shard1"]
    await txn.abort()

    await driver.finalize()
