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
from nucliadb_protos.knowledgebox_pb2 import Labels

from nucliadb.common.datamanagers.labels import LabelsDataManager

pytestmark = pytest.mark.asyncio


async def test_labelset_ids(maindb_driver):
    ldm = LabelsDataManager
    kbid = "foo"
    # Check that initially all are empty
    async with maindb_driver.transaction() as txn:
        assert await ldm._get_labelset_ids(kbid, txn) is None

    # Check that adding to the list creates the list
    async with maindb_driver.transaction() as txn:
        await ldm._add_to_labelset_ids(kbid, txn, ["bar", "ba"])
        await txn.commit()
    async with maindb_driver.transaction() as txn:
        await ldm._add_to_labelset_ids(kbid, txn, ["bar", "baz"])
        await txn.commit()
    async with maindb_driver.transaction() as txn:
        assert await ldm._get_labelset_ids(kbid, txn) == ["bar", "ba", "baz"]

    # Check that removing from the list removes the item
    async with maindb_driver.transaction() as txn:
        await ldm._delete_from_labelset_ids(kbid, txn, ["ba"])
        await txn.commit()
    async with maindb_driver.transaction() as txn:
        assert await ldm._get_labelset_ids(kbid, txn) == ["bar", "baz"]

    # Check that list is empty after removing all
    async with maindb_driver.transaction() as txn:
        await ldm._delete_from_labelset_ids(kbid, txn, ["bar", "baz"])
        await txn.commit()
    async with maindb_driver.transaction() as txn:
        assert await ldm._get_labelset_ids(kbid, txn) == []

    # Check that set_labels overwrites the list
    async with maindb_driver.transaction() as txn:
        await ldm._set_labelset_ids(kbid, txn, ["bar", "baz"])
        await txn.commit()

    ldb_obj = LabelsDataManager(maindb_driver)
    labels = Labels()
    labels.labelset["bar"].title = "bar"
    await ldb_obj.set_labels(kbid, labels)

    async with maindb_driver.transaction() as txn:
        assert await ldm._get_labelset_ids(kbid, txn) == ["bar"]


async def test_labelset_ids_bw_compat(maindb_driver):
    ldm = LabelsDataManager
    kbid = "foo"
    # Check that initially all are empty
    async with maindb_driver.transaction() as txn:
        assert await ldm._deprecated_scan_labelset_ids(kbid, txn) == []
        assert await ldm._get_labelset_ids(kbid, txn) is None
        assert await ldm._get_labelset_ids_bw_compat(kbid, txn) == []

    # Check that adding to the list creates the list
    async with maindb_driver.transaction() as txn:
        await ldm._add_to_labelset_ids(kbid, txn, ["bar", "ba"])
        await txn.commit()
    async with maindb_driver.transaction() as txn:
        assert await ldm._get_labelset_ids(kbid, txn) == ["bar", "ba"]
        assert await ldm._deprecated_scan_labelset_ids(kbid, txn) == []

    # Check that adding appends to the list
    async with maindb_driver.transaction() as txn:
        await ldm._add_to_labelset_ids(kbid, txn, ["baz", "ba"])
        await txn.commit()
    async with maindb_driver.transaction() as txn:
        assert await ldm._get_labelset_ids(kbid, txn) == ["bar", "ba", "baz"]

    # Check that removing from the list removes the item
    async with maindb_driver.transaction() as txn:
        await ldm._delete_from_labelset_ids(kbid, txn, ["ba"])
        await txn.commit()
    async with maindb_driver.transaction() as txn:
        assert await ldm._get_labelset_ids(kbid, txn) == ["bar", "baz"]

    # Check that removing also creates the list
    async with maindb_driver.transaction() as txn:
        assert await ldm._get_labelset_ids("other", txn) is None
        await ldm._delete_from_labelset_ids("other", txn, ["bar", "baz"])
        await txn.commit()
    async with maindb_driver.transaction() as txn:
        assert await ldm._get_labelset_ids("other", txn) == []

    # Check legacy method
    async with maindb_driver.transaction() as txn:
        await txn.set("kb/labels/foo/1", b"somedata")
        await txn.commit()
    async with maindb_driver.transaction() as txn:
        await ldm._get_labelset_ids("foo", txn) is None
        await ldm._deprecated_scan_labelset_ids("foo", txn) == ["1"]
        await ldm._get_labelset_ids_bw_compat("foo", txn) == ["1"]
