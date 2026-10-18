from unittest.mock import AsyncMock, MagicMock

import pytest

from nucliadb.common.maindb.marklogic import MarkLogicDriver, MarkLogicTransaction
from nucliadb.common.marklogic import documents
from nucliadb.common.marklogic.document_update import DocumentUpdate


@pytest.fixture
def txn():
    driver = MagicMock(spec=MarkLogicDriver)
    driver.client.rows.update = AsyncMock(return_value=[{"uri": "my_doc"}])
    driver.client.documents.read = AsyncMock(side_effect=AssertionError("Unexpected read"))
    driver.client.documents.write = AsyncMock(side_effect=AssertionError("Unexpected rewrite"))
    transaction = MagicMock(spec=MarkLogicTransaction)
    transaction.driver = driver
    transaction.open = True
    transaction.read_only = False
    transaction.params = AsyncMock(return_value={"database": "test", "txid": "tx-1"})
    return transaction


async def test_update_document_without_reading(txn):
    update = DocumentUpdate(uri="my_doc", collection="test").set(foo="baz", extra=[1, 2, 3])

    assert await documents.update_document(txn, update)

    txn.driver.client.rows.update.assert_awaited_once_with(
        dsl=(
            'op.fromLiterals([{uri: "my_doc"}])'
            ".joinDoc(op.col('doc'), op.col('uri'))"
            ".patch(op.col('doc'), op.patchBuilder('/')"
            '.remove("node(\\"foo\\")").insertNamedChild(\'.\', "foo", "baz")'
            '.remove("node(\\"extra\\")").insertNamedChild(\'.\', "extra", [1, 2, 3]))'
            ".write().select(['uri'])"
        ),
        params={"database": "test", "txid": "tx-1"},
    )
    txn.driver.client.documents.read.assert_not_awaited()
    txn.driver.client.documents.write.assert_not_awaited()


@pytest.mark.parametrize("upsert", [False, True])
async def test_update_missing_document(txn, upsert):
    txn.driver.client.rows.update.return_value = []
    txn.driver.client.documents.write.side_effect = None
    txn.sdk_transaction = AsyncMock(return_value=object())
    update = DocumentUpdate(
        uri="my_doc", collection="test", defaults={"a": "b", "removed": 1}, upsert=upsert
    ).set(foo="baz")
    update.remove("removed")

    assert await documents.update_document(txn, update) is upsert

    txn.driver.client.documents.read.assert_not_awaited()
    if upsert:
        document = txn.driver.client.documents.write.call_args.args[0]
        assert document.content == {"a": "b", "foo": "baz"}
        assert document.collections == ["test"]
    else:
        txn.driver.client.documents.write.assert_not_awaited()


async def test_empty_update(txn):
    assert await documents.update_document(txn, DocumentUpdate(uri="my_doc", collection="test"))
    txn.driver.client.rows.update.assert_not_awaited()
    txn.driver.client.documents.read.assert_not_awaited()
    txn.driver.client.documents.write.assert_not_awaited()
