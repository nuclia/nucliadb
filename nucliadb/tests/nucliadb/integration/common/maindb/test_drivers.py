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

import uuid

import pytest

from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.maindb.driver import Driver
from nucliadb.common.maindb.marklogic import MarkLogicDriver, MarkLogicTransaction
from nucliadb.common.marklogic.client import Document


async def test_transaction_scope_is_backward_compatible():
    driver = Driver()

    async with driver.ro_transaction():
        pass

    with pytest.raises(ValueError, match="either kbid or system=True"):
        async with driver.rw_transaction(kbid="kb", system=True):
            pass

    with pytest.raises(ValueError, match="either kbid or system=True"):
        async with driver.rw_transaction(system=False):
            pass

    async with driver.ro_transaction(system=True):
        pass
    async with driver.rw_transaction(kbid="kb"):
        pass


async def test_marklogic_driver_accessors():
    driver = MarkLogicDriver(uri="http://127.0.0.1", username="admin", password="admin")
    with pytest.raises(RuntimeError, match="not initialized"):
        _ = driver.client
    with pytest.raises(RuntimeError, match="not initialized"):
        _ = driver.data

    await driver.initialize()
    assert driver.client is driver._client
    assert driver.data is driver._data
    await driver.finalize()

    with pytest.raises(RuntimeError, match="not initialized"):
        _ = driver.client
    with pytest.raises(RuntimeError, match="not initialized"):
        _ = driver.data


async def test_marklogic_driver(marklogic_maindb_driver):
    """Run the generic maindb contract against a fixture-managed MarkLogic server."""
    driver = marklogic_maindb_driver
    await driver_basic(driver)


async def test_delete_by_prefix_is_scoped_and_transactional(
    marklogic_maindb_driver: MarkLogicDriver, mocker
):
    driver = marklogic_maindb_driver
    update = mocker.spy(driver.client.rows, "update")
    prefix = f"/bulk-delete/{uuid.uuid4()}/a"
    matching = (f"{prefix}/one", f"{prefix}/two")
    neighbor = f"{prefix[:-1]}b/keep"
    other_uri = f"/{uuid.uuid4()}/knowledgeboxes/config.json"
    async with driver.rw_transaction() as txn:
        assert isinstance(txn, MarkLogicTransaction)
        for key in (*matching, neighbor):
            await txn.set(key, key.encode())
        await driver.client.documents.write(
            Document(
                uri=other_uri,
                content={"maindb_key": matching[0]},
                collections=[MarkLogicCollections.KNOWLEDGEBOXES],
                content_type="application/json",
            ),
            tx=await txn.sdk_transaction(),
            params={"database": driver.database},
        )
        await txn.commit()

    async with driver.rw_transaction() as txn:
        await txn.delete_by_prefix(prefix)
        assert await txn.get(matching[0]) is None
        await txn.abort()

    async with driver.ro_transaction() as txn:
        assert await txn.get(matching[0]) == matching[0].encode()

    async with driver.rw_transaction() as txn:
        await txn.delete_by_prefix(prefix)
        await txn.commit()
    assert update.call_count == 2

    async with driver.ro_transaction() as txn:
        assert await txn.batch_get([*matching, neighbor]) == [None, None, neighbor.encode()]
    documents = await driver.client.documents.read(other_uri, params={"database": driver.database})
    assert isinstance(documents, list)
    assert len(documents) == 1

    async with driver.rw_transaction() as txn:
        assert isinstance(txn, MarkLogicTransaction)
        await txn.delete(neighbor)
        await driver.client.documents.delete(other_uri, params=await txn.params())
        await txn.commit()


async def _clear_db(driver: Driver):
    async with driver.rw_transaction() as txn:
        await txn.delete_by_prefix("/")
        await txn.commit()

    async with driver.ro_transaction() as txn:
        assert await txn.count("/") == 0


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

        result = await txn.batch_get(["/kbs/kb1/r/uuid1/text", "/internal/kbs/kb1/shards/shard1"])  # type: ignore[assignment]
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

    assert current_internal_kbs_keys == {"/internal/kbs/kb1/shards/shard1"}

    # Test nested keys are NOT deleted when deleting the parent one

    async with driver.rw_transaction() as txn:
        result = await txn.delete("/internal/kbs")
        await txn.commit()

    current_internal_kbs_keys = set()
    async with driver.ro_transaction() as txn:
        async for key in txn.keys("/internal/kbs"):
            current_internal_kbs_keys.add(key)

    assert current_internal_kbs_keys == {"/internal/kbs/kb1/shards/shard1"}

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

    assert current_internal_kbs_keys == {"/internal/kbs/kb1/shards/shard1"}

    await _test_keys_async_generator(driver)

    await _test_transaction_context_manager(driver)

    await _clear_db(driver)

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
