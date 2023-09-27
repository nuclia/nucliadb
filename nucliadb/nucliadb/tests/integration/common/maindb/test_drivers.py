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
import os

import asyncpg
import pytest

from nucliadb.common.maindb.driver import Driver
from nucliadb.common.maindb.pg import PGDriver
from nucliadb.common.maindb.redis import RedisDriver
from nucliadb.common.maindb.tikv import TiKVDriver

TESTING_MAINDB_DRIVERS = os.environ.get(
    "TESTING_MAINDB_DRIVERS", "tikv,redis,pg,local"
).split(",")


@pytest.mark.skipif(
    "redis" not in TESTING_MAINDB_DRIVERS, reason="redis not in TESTING_MAINDB_DRIVERS"
)
async def test_redis_driver(redis):
    url = f"redis://{redis[0]}:{redis[1]}"
    driver = RedisDriver(url=url)
    await driver_basic(driver)


@pytest.mark.flaky(reruns=5)
@pytest.mark.skipif(
    "tikv" not in TESTING_MAINDB_DRIVERS, reason="tikv not in TESTING_MAINDB_DRIVERS"
)
async def test_tikv_driver(tikvd):
    url = [f"{tikvd.host}:{tikvd.pd_port}"]
    driver = TiKVDriver(url=url)
    await driver_basic(driver)


@pytest.mark.skipif(
    "tikv" not in TESTING_MAINDB_DRIVERS, reason="tikv not in TESTING_MAINDB_DRIVERS"
)
async def test_tikv_driver_against_restarts(tikvd):
    url = [f"{tikvd.host}:{tikvd.pd_port}"]
    driver = TiKVDriver(url=url)
    await driver.initialize()

    async def _runtest(i):
        async with driver.transaction() as txn:
            await txn.set(f"/test{i}", b"test")
            await txn.get(f"/test{i}") == b"test"

    await asyncio.gather(*[_runtest(i) for i in range(10)])

    tikvd.stop()
    tikvd.start()

    # after server is restarted, old connection is bad and connection needs
    # to be re-established but having many simultaneous requests should not stomp on each other
    await asyncio.gather(*[_runtest(i) for i in range(10)])


@pytest.mark.skipif(
    "pg" not in TESTING_MAINDB_DRIVERS, reason="pg not in TESTING_MAINDB_DRIVERS"
)
async def test_pg_driver(pg):
    url = f"postgresql://postgres:postgres@{pg[0]}:{pg[1]}/postgres"
    conn = await asyncpg.connect(url)
    await conn.execute(
        """
DROP table IF EXISTS resources;
"""
    )
    await conn.close()
    driver = PGDriver(url=url)
    await driver_basic(driver)


@pytest.mark.skip(
    reason="Local driver doesn't implement saving info in intermediate nodes"
)
@pytest.mark.skipif(
    "local" not in TESTING_MAINDB_DRIVERS, reason="local not in TESTING_MAINDB_DRIVERS"
)
async def test_local_driver(local_driver):
    await driver_basic(local_driver)


async def _clear_db(driver: Driver):
    all_keys = []
    async with driver.transaction() as txn:
        async for key in txn.keys("/"):
            all_keys.append(key)

    async with driver.transaction() as txn:
        for key in all_keys:
            await txn.delete(key)


async def driver_basic(driver: Driver):
    await driver.initialize()

    await _clear_db(driver)

    # Test deleting a key that doesn't exist does not raise any error
    txn = await driver.begin()
    await txn.delete("/i/do/not/exist")
    await txn.commit()

    txn = await driver.begin()
    await txn.set("/internal/kbs/kb1/title", b"My title")
    await txn.set("/internal/kbs/kb1/shards/shard1", b"node1")

    await txn.set("/kbs/kb1/r/uuid1/text", b"My title")

    result = await txn.get("/kbs/kb1/r/uuid1/text")
    assert result == b"My title"

    await txn.commit()

    txn = await driver.begin()
    result = await txn.get("/kbs/kb1/r/uuid1/text")
    assert result == b"My title"

    result = await txn.batch_get(
        ["/kbs/kb1/r/uuid1/text", "/internal/kbs/kb1/shards/shard1"]
    )
    await txn.abort()

    current_internal_kbs_keys = set()
    async with driver.transaction() as txn:
        async for key in txn.keys("/internal/kbs/"):
            current_internal_kbs_keys.add(key)
    assert current_internal_kbs_keys == {
        "/internal/kbs/kb1/title",
        "/internal/kbs/kb1/shards/shard1",
    }

    # Test delete one key
    txn = await driver.begin()
    result = await txn.delete("/internal/kbs/kb1/title")
    await txn.commit()

    current_internal_kbs_keys = set()
    async with driver.transaction() as txn:
        async for key in txn.keys("/internal/kbs/"):
            current_internal_kbs_keys.add(key)

    assert current_internal_kbs_keys == {"/internal/kbs/kb1/shards/shard1"}

    # Test nested keys are NOT deleted when deleting the parent one

    txn = await driver.begin()
    result = await txn.delete("/internal/kbs")
    await txn.commit()

    current_internal_kbs_keys = set()
    async with driver.transaction() as txn:
        async for key in txn.keys("/internal/kbs"):
            current_internal_kbs_keys.add(key)

    assert current_internal_kbs_keys == {"/internal/kbs/kb1/shards/shard1"}

    # Test that all nested keys where a parent path exist as a key, are all returned by scan keys

    txn = await driver.begin()
    await txn.set("/internal/kbs", b"I am the father")
    await txn.commit()

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

    async with driver.transaction() as txn:
        assert len(current_internal_kbs_keys) == await txn.count("/internal/kbs")
        assert await txn.count("/internal/a/foobar") == 0

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
    await txn.commit()

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
        await txn.commit()

    async with driver.transaction() as txn:
        assert await txn.get("/some/key") == b"some value"
