from unittest.mock import patch
from uuid import uuid4

import pytest

from nucliadb.common.marklogic import documents
from nucliadb.common.marklogic.document_update import DocumentUpdate


@pytest.mark.parametrize("value", [{"nested": True}, [], None, {}, False, 0])
async def test_patch_root_properties(marklogic_maindb_driver, value):
    uri = f"/tests/document-patch/{uuid4().hex}.json"
    async with marklogic_maindb_driver.rw_transaction() as txn:
        await documents.write(txn, uri, "test-patch", {"foo": "bar", "a": "b"})

        update = DocumentUpdate(uri=uri, collection="test-patch").set(foo="baz", extra=[1, 2, 3])
        with patch.object(
            marklogic_maindb_driver.client.documents,
            "read",
            side_effect=AssertionError("Patch must not read document contents"),
        ):
            assert await documents.update_document(txn, update)

        assert await documents.read(txn, uri) == {"foo": "baz", "a": "b", "extra": [1, 2, 3]}

        update = DocumentUpdate(uri=uri, collection="test-patch").set(
            foo=value, extra=[], nullable=None, empty={}, enabled=False, count=0
        )
        update.remove("a", "absent")
        assert await documents.update_document(txn, update)
        assert await documents.read(txn, uri) == {
            "foo": value,
            "extra": [],
            "nullable": None,
            "empty": {},
            "enabled": False,
            "count": 0,
        }

        missing_uri = f"{uri}.missing"
        update = DocumentUpdate(uri=missing_uri, collection="test-patch", upsert=False).set(foo="baz")
        assert not await documents.update_document(txn, update)
        update.upsert = True
        update.defaults = {"a": "b"}
        assert await documents.update_document(txn, update)
        assert await documents.read(txn, missing_uri) == {"foo": "baz", "a": "b"}
