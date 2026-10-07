import asyncio
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from nucliadb.common.maindb.marklogic import MarkLogicDriver


@pytest.fixture
async def driver():
    driver = MarkLogicDriver("http://localhost", "admin", "admin")
    await driver.initialize()
    client = MagicMock()
    client.transactions.create = AsyncMock(
        side_effect=lambda **kwargs: MagicMock(
            id=kwargs["database"], commit=AsyncMock(), rollback=AsyncMock()
        )
    )
    client.documents.read = AsyncMock(return_value=[])
    client.documents.write = AsyncMock(return_value=None)
    client.documents.delete = AsyncMock(return_value=None)
    client.rows.update = AsyncMock(return_value=None)
    client.eval = AsyncMock(return_value=[])
    original_client = driver.client
    driver._client = client
    try:
        yield driver
    finally:
        driver._client = original_client
        await driver.finalize()


@pytest.mark.asyncio
async def test_kv_operations_route_to_explicit_database(driver):
    client = driver.client
    for kbid, database in (
        (None, driver.system_database),
        ("one", "nucliadb-kb-one"),
        ("two", "nucliadb-kb-two"),
    ):
        context = driver.rw_transaction() if kbid is None else driver.rw_transaction(kbid=kbid)
        async with context as txn:
            assert txn.database == database
            assert txn.kbid == kbid
            await txn.set("same-key", b"value")
            assert client.documents.write.call_args.kwargs["params"]["database"] == database
            client.documents.read.return_value = [
                SimpleNamespace(uri="maindb:same-key", content={"value": "dmFsdWU="})
            ]
            assert await txn.get("same-key") == b"value"
            assert client.documents.read.call_args.kwargs["params"]["database"] == database
            assert await txn.batch_get(["same-key", "missing"]) == [b"value", None]
            assert client.documents.read.call_args.kwargs["params"]["database"] == database
            client.documents.read.return_value = []
            await txn.insert("another-key", b"value")
            assert client.documents.read.call_args.kwargs["params"]["database"] == database
            assert client.documents.write.call_args.kwargs["params"]["database"] == database
            await txn.delete("same-key")
            assert client.documents.delete.call_args.kwargs["params"]["database"] == database
            await txn.delete_by_prefix("same")
            assert client.rows.update.call_args.kwargs["params"]["database"] == database
            client.eval.return_value = ["maindb:same-key"]
            assert [key async for key in txn.keys("same")] == ["same-key"]
            assert client.eval.call_args.kwargs["params"]["database"] == database
            client.eval.return_value = [1]
            assert await txn.count("same") == 1
            assert client.eval.call_args.kwargs["params"]["database"] == database
            await txn.commit()
    assert client.transactions.create.await_count == 3
    assert client.transactions.create.call_args_list[0].kwargs == {"database": driver.system_database}
    assert client.transactions.create.call_args_list[1].kwargs == {"database": "nucliadb-kb-one"}
    assert client.transactions.create.call_args_list[2].kwargs == {"database": "nucliadb-kb-two"}


@pytest.mark.asyncio
@pytest.mark.parametrize("kbid", [None, "one", "two"])
async def test_readonly_does_not_create_sdk_transaction(driver, kbid):
    client = driver.client
    database = driver.system_database if kbid is None else driver.kb_database(kbid)
    context = driver.ro_transaction() if kbid is None else driver.ro_transaction(kbid=kbid)
    async with context as txn:
        assert await txn.get("same-key") is None
        assert await txn.batch_get(["same-key"]) == [None]
        assert client.documents.read.call_args.kwargs["params"] == {"database": database}
        assert "txid" not in client.documents.read.call_args.kwargs["params"]
        assert [key async for key in txn.keys("same")] == []
        assert client.eval.call_args.kwargs["params"] == {"database": database}
        assert await txn.count("same") == 0
        assert client.eval.call_args.kwargs["params"] == {"database": database}
        for method, args in (
            ("set", ("same-key", b"value")),
            ("insert", ("same-key", b"value")),
            ("delete", ("same-key",)),
            ("delete_by_prefix", ("same",)),
            ("commit", ()),
        ):
            with pytest.raises(RuntimeError, match="read only"):
                await getattr(txn, method)(*args)
    client.transactions.create.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["commit", "abort", "exit", "exception"])
async def test_sdk_transaction_lifecycle(driver, outcome):
    with pytest.raises(ValueError, match="failure") if outcome == "exception" else nullcontext():
        async with driver.rw_transaction(kbid="one") as txn:
            await txn.set("same-key", b"value")
            await txn.get("same-key")
            sdk_txn = txn._sdk_transaction
            if outcome in ("commit", "abort"):
                await getattr(txn, outcome)()
                await getattr(txn, outcome)()
            elif outcome == "exception":
                raise ValueError("failure")
    driver.client.transactions.create.assert_awaited_once_with(database="nucliadb-kb-one")
    assert not txn.open
    assert txn._sdk_transaction is None
    if outcome == "commit":
        sdk_txn.commit.assert_awaited_once_with()
        sdk_txn.rollback.assert_not_awaited()
    else:
        sdk_txn.rollback.assert_awaited_once_with()
        sdk_txn.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_sdk_transaction_is_created_once_concurrently(driver):
    async with driver.rw_transaction(kbid="one") as txn:
        transactions = await asyncio.gather(*(txn.sdk_transaction() for _ in range(10)))
        assert all(transaction is transactions[0] for transaction in transactions)
    driver.client.transactions.create.assert_awaited_once_with(database="nucliadb-kb-one")


@pytest.mark.asyncio
async def test_transaction_rejects_another_database(driver):
    async with driver.rw_transaction(kbid="one") as txn:
        with pytest.raises(TypeError):
            await txn.params("nucliadb-kb-two")
        with pytest.raises(AttributeError):
            txn.database = "nucliadb-kb-two"
    driver.client.transactions.create.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("read_only", [False, True])
async def test_closed_transaction_rejects_operations(driver, read_only):
    context = driver.ro_transaction() if read_only else driver.rw_transaction()
    async with context as txn:
        pass
    assert not txn.open
    with pytest.raises(RuntimeError, match="closed"):
        await txn.get("key")
    driver.client.transactions.create.assert_not_awaited()
