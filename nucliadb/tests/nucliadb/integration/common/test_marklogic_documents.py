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
