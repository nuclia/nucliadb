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
from unittest.mock import patch
from uuid import uuid4

import pytest

from nucliadb.common.marklogic import documents
from nucliadb.common.marklogic.document_update import DocumentUpdate


async def test_read_properties(marklogic_maindb_driver):
    uri = f"/tests/document-projection/{uuid4().hex}.json"
    selected = {
        "text": "hello",
        "nested": {"items": [1, 2]},
        "array": ["first", "second"],
        "empty_array": [],
        "empty_object": {},
        "nullable": None,
        "enabled": False,
        "count": 0,
        "quote'and\"slash/": "escaped",
    }
    async with marklogic_maindb_driver.rw_transaction() as txn:
        await documents.write(txn, uri, "test-projection", {**selected, "excluded": "large content"})
        with patch.object(
            marklogic_maindb_driver.client.documents,
            "read",
            side_effect=AssertionError("Projection must use Optic"),
        ):
            assert await documents.read_properties(txn, uri, [*selected, "absent", "text"]) == {
                **selected,
                "absent": None,
            }
            assert await documents.read_properties(txn, uri, []) == {}
            assert await documents.read_properties(txn, uri, ["absent"]) == {"absent": None}
            assert await documents.read_properties(txn, f"{uri}.missing", ["text"]) is None
        await txn.commit()

    async with marklogic_maindb_driver.ro_transaction() as txn:
        assert await documents.read_properties(txn, uri, ["text"]) == {"text": "hello"}


async def test_read_many_properties(marklogic_maindb_driver):
    first_uri = f"/tests/document-projection/{uuid4().hex}.json"
    second_uri = f"/tests/document-projection/{uuid4().hex}.json"
    missing_uri = f"{first_uri}.missing"
    async with marklogic_maindb_driver.rw_transaction() as txn:
        await documents.write(txn, first_uri, "test-projection", {"text": "first", "extra": 1})
        await documents.write(txn, second_uri, "test-projection", {"text": "second", "extra": 2})
        await txn.commit()

    async with marklogic_maindb_driver.ro_transaction() as txn:
        with patch.object(
            marklogic_maindb_driver.client.documents,
            "read",
            side_effect=AssertionError("Projection must use Optic"),
        ):
            assert await documents.read_many_properties(
                txn, [first_uri, missing_uri, second_uri, first_uri], ["text", "absent"]
            ) == {
                first_uri: {"text": "first", "absent": None},
                second_uri: {"text": "second", "absent": None},
            }
            assert await documents.read_many_properties(txn, [first_uri, second_uri], []) == {
                first_uri: {},
                second_uri: {},
            }
            assert await documents.read_many_properties(txn, [missing_uri], ["text"]) == {}
            assert await documents.read_many_properties(txn, [], ["text"]) == {}


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
