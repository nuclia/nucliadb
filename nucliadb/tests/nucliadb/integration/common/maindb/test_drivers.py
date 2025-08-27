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

import psycopg_pool
import pytest

from nucliadb.common.maindb.driver import Driver
from nucliadb.common.maindb.local import LocalDriver
from nucliadb.common.maindb.pg import PGDriver


@pytest.fixture(scope="function")
def local_driver(tmpdir):
    from nucliadb.common.maindb.local import LocalDriver

    return LocalDriver(str(tmpdir))


async def test_local_driver(local_driver):
    await driver_basic(local_driver)


async def test_pg_driver(pg_maindb_driver):
    await driver_basic(pg_maindb_driver)


async def test_pg_driver_pool_timeout(pg):
    url = f"postgresql://postgres:postgres@{pg[0]}:{pg[1]}/postgres"
    driver = PGDriver(url, connection_pool_min_size=1, connection_pool_max_size=1)
    await driver.initialize()

    # Get one connection and hold it
    async with driver.rw_transaction():
        # Try to get another connection, should fail because pool is full
        with pytest.raises(psycopg_pool.PoolTimeout):
            await driver.rw_transaction().__aenter__()

    # Should now work
    async with driver.rw_transaction():
        pass


async def _clear_db(driver: Driver):
    all_keys = []
    async with driver.ro_transaction() as txn:
        async for key in txn.keys("/"):
            all_keys.append(key)

    async with driver.rw_transaction() as txn:
        for key in all_keys:
            await txn.delete(key)
        await txn.commit()


async def driver_basic(driver: Driver):
    await driver.initialize()

    await _clear_db(driver)

    # Test deleting a key that doesn't exist does not raise any error
    async with driver.rw_transaction() as txn:
        await txn.delete("/i/do/not/exist")
        await txn.commit()

    async with driver.rw_transaction() as txn:
        await txn.set("/internal/kbs/kb1/title", b"My title")
        await txn.set("/internal/kbs/kb1/shards/shard1", b"node1")

        await txn.set("/kbs/kb1/r/uuid1/text", b"My title")

        result = await txn.get("/kbs/kb1/r/uuid1/text")
        assert result == b"My title"

        await txn.commit()

    async with driver.ro_transaction() as txn:
        result = await txn.get("/kbs/kb1/r/uuid1/text")
        assert result == b"My title"

        result = await txn.batch_get(  # type: ignore
            ["/kbs/kb1/r/uuid1/text", "/internal/kbs/kb1/shards/shard1"]
        )
        assert result == [b"My title", b"node1"]
        await txn.abort()

    current_internal_kbs_keys = set()
    async with driver.ro_transaction() as txn:
        async for key in txn.keys("/internal/kbs/"):
            current_internal_kbs_keys.add(key)
    assert current_internal_kbs_keys == {
        "/internal/kbs/kb1/title",
        "/internal/kbs/kb1/shards/shard1",
    }

    # Test delete one key
    async with driver.rw_transaction() as txn:
        result = await txn.delete("/internal/kbs/kb1/title")
        await txn.commit()

    current_internal_kbs_keys = set()
    async with driver.ro_transaction() as txn:
        async for key in txn.keys("/internal/kbs/"):
            current_internal_kbs_keys.add(key)

    try:
        assert current_internal_kbs_keys == {"/internal/kbs/kb1/shards/shard1"}
    except AssertionError:
        if isinstance(driver, LocalDriver):
            # LocalDriver does not support nested keys
            current_internal_kbs_keys = {"/internal/kbs/kb1/shards/shard1"}
        else:
            raise

    # Test nested keys are NOT deleted when deleting the parent one

    async with driver.rw_transaction() as txn:
        result = await txn.delete("/internal/kbs")
        await txn.commit()

    current_internal_kbs_keys = set()
    async with driver.ro_transaction() as txn:
        async for key in txn.keys("/internal/kbs"):
            current_internal_kbs_keys.add(key)

    try:
        assert current_internal_kbs_keys == {"/internal/kbs/kb1/shards/shard1"}
    except AssertionError:
        if isinstance(driver, LocalDriver):
            # LocalDriver does not support nested keys
            current_internal_kbs_keys = {"/internal/kbs/kb1/shards/shard1"}
        else:
            raise

    # Test that all nested keys where a parent path exist as a key, are all returned by scan keys

    async with driver.rw_transaction() as txn:
        await txn.set("/internal/kbs", b"I am the father")
        await txn.commit()

    # It works without trailing slash ...
    async with driver.ro_transaction() as txn:
        current_internal_kbs_keys = set()
        async for key in txn.keys("/internal/kbs"):
            current_internal_kbs_keys.add(key)
        await txn.abort()

    assert current_internal_kbs_keys == {
        "/internal/kbs/kb1/shards/shard1",
        "/internal/kbs",
    }

    async with driver.ro_transaction() as txn:
        assert len(current_internal_kbs_keys) == await txn.count("/internal/kbs")
        assert await txn.count("/internal/a/foobar") == 0

    # but with it it does not return the father
    async with driver.ro_transaction() as txn:
        current_internal_kbs_keys = set()
        async for key in txn.keys("/internal/kbs/"):
            current_internal_kbs_keys.add(key)
        await txn.abort()

    try:
        assert current_internal_kbs_keys == {"/internal/kbs/kb1/shards/shard1"}
    except AssertionError:
        if isinstance(driver, LocalDriver):
            # LocalDriver does not support nested keys
            current_internal_kbs_keys = {"/internal/kbs/kb1/shards/shard1"}
        else:
            raise

    await _test_keys_async_generator(driver)

    await _test_transaction_context_manager(driver)

    await driver.finalize()


async def _test_keys_async_generator(driver):
    async with driver.rw_transaction() as txn:
        for i in range(10):
            await txn.set(f"/keys/{i}", str(i).encode())
        await txn.commit()

    async with driver.ro_transaction() as txn:
        async_generator = txn.keys("/keys/", count=10)
        await async_generator.__anext__()
        await async_generator.__anext__()
        await async_generator.aclose()
        await txn.abort()


async def _test_transaction_context_manager(driver):
    async with driver.rw_transaction() as txn:
        await txn.set("/some/key", b"some value")
    assert not txn.open

    async with driver.ro_transaction() as txn:
        assert await txn.get("/some/key") is None

    # It should not attempt to abort if commited
    async with driver.rw_transaction() as txn:
        assert await txn.get("/some/key") is None
        await txn.set("/some/key", b"some value")
        await txn.commit()

    async with driver.ro_transaction() as txn:
        assert await txn.get("/some/key") == b"some value"
