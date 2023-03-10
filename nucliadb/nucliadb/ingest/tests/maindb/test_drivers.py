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

from nucliadb.ingest.maindb.redis import RedisDriver
from nucliadb.ingest.maindb.tikv import TiKVDriver


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


@pytest.mark.skip(
    reason="Local driver doesn't implement saving info in intermediate nodes"
)
@pytest.mark.asyncio
async def test_local_driver(local_driver):
    await driver_basic(local_driver)


async def driver_basic(driver):
    await driver.initialize()

    # Test deleting a key that doesn't exist does not raise any error
    txn = await driver.begin()
    await txn.delete("/i/do/not/exist")
    await txn.commit(resource=False)

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
    await txn.abort()

    current_internal_kbs_keys = set()
    async for key in driver.keys("/internal/kbs"):
        current_internal_kbs_keys.add(key)
    assert current_internal_kbs_keys == {
        "/internal/kbs/kb1/title",
        "/internal/kbs/kb1/shards/shard1",
    }

    # Test delete one key
    txn = await driver.begin()
    result = await txn.delete("/internal/kbs/kb1/title")
    await txn.commit(resource=False)

    current_internal_kbs_keys = set()
    async for key in driver.keys("/internal/kbs"):
        current_internal_kbs_keys.add(key)

    assert current_internal_kbs_keys == {"/internal/kbs/kb1/shards/shard1"}

    # Test nested keys are NOT deleted when deleting the parent one

    txn = await driver.begin()
    result = await txn.delete("/internal/kbs")
    await txn.commit(resource=False)

    current_internal_kbs_keys = set()
    async for key in driver.keys("/internal/kbs"):
        current_internal_kbs_keys.add(key)

    assert current_internal_kbs_keys == {"/internal/kbs/kb1/shards/shard1"}

    # Test that all nested keys where a parent path exist as a key, are all returned by scan keys

    txn = await driver.begin()
    await txn.set("/internal/kbs", b"I am the father")
    await txn.commit(resource=False)

    # It works without trailing slash ...
    txn = await driver.begin()
    current_internal_kbs_keys = set()
    async for key in txn.keys("/internal/kbs"):
        current_internal_kbs_keys.add(key)
    await txn.abort()

    assert current_internal_kbs_keys == {
        "/internal/kbs/kb1/shards/shard1",
        "/internal/kbs",
    }

    # but with it it does not return the father
    txn = await driver.begin()
    current_internal_kbs_keys = set()
    async for key in txn.keys("/internal/kbs/"):
        current_internal_kbs_keys.add(key)
    await txn.abort()

    assert current_internal_kbs_keys == {"/internal/kbs/kb1/shards/shard1"}

    await _test_keys_async_generator(driver)

    await _test_transaction_context_manager(driver)

    await driver.finalize()


async def _test_keys_async_generator(driver):
    txn = await driver.begin()
    for i in range(10):
        await txn.set(f"/keys/{i}", str(i).encode())
    await txn.commit(resource=False)

    txn = await driver.begin()
    async_generator = txn.keys("/keys/", count=10)
    await async_generator.__anext__()
    await async_generator.__anext__()
    await async_generator.aclose()
    await txn.abort()


async def _test_transaction_context_manager(driver):
    # It should abort the transaction if there are uncommited changes
    async with driver.transaction() as txn:
        assert txn.open
        await txn.set("/some/key", b"some value")
    assert not txn.open

    # It should not attempt to abort if commited
    async with driver.transaction() as txn:
        assert await txn.get("/some/key") is None
        await txn.set("/some/key", b"some value")
        await txn.commit(resource=False)

    async with driver.transaction() as txn:
        assert await txn.get("/some/key") == b"some value"
