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

from nucliadb.common import datamanagers

pytestmark = pytest.mark.asyncio


async def test_labelset_ids(maindb_driver):
    kbid = "foo"
    # Check that initially all are empty
    async with maindb_driver.transaction() as txn:
        assert await datamanagers.labels._get_labelset_ids(txn, kbid=kbid) is None

    # Check that adding to the list creates the list
    async with maindb_driver.transaction() as txn:
        await datamanagers.labels._add_to_labelset_ids(
            txn, kbid=kbid, labelsets=["bar", "ba"]
        )
        await txn.commit()
    async with maindb_driver.transaction() as txn:
        await datamanagers.labels._add_to_labelset_ids(
            txn, kbid=kbid, labelsets=["bar", "baz"]
        )
        await txn.commit()
    async with maindb_driver.transaction() as txn:
        assert await datamanagers.labels._get_labelset_ids(txn, kbid=kbid) == [
            "bar",
            "ba",
            "baz",
        ]

    # Check that removing from the list removes the item
    async with maindb_driver.transaction() as txn:
        await datamanagers.labels._delete_from_labelset_ids(
            txn, kbid=kbid, labelsets=["ba"]
        )
        await txn.commit()
    async with maindb_driver.transaction() as txn:
        assert await datamanagers.labels._get_labelset_ids(txn, kbid=kbid) == [
            "bar",
            "baz",
        ]

    # Check that list is empty after removing all
    async with maindb_driver.transaction() as txn:
        await datamanagers.labels._delete_from_labelset_ids(
            txn, kbid=kbid, labelsets=["bar", "baz"]
        )
        await txn.commit()
    async with maindb_driver.transaction() as txn:
        assert await datamanagers.labels._get_labelset_ids(txn, kbid=kbid) == []

    # Check that set_labels overwrites the list
    async with maindb_driver.transaction() as txn:
        await datamanagers.labels._set_labelset_ids(
            txn, kbid=kbid, labelsets=["bar", "baz"]
        )
        await txn.commit()

    async with maindb_driver.transaction() as txn:
        labels = Labels()
        labels.labelset["bar"].title = "bar"
        await datamanagers.labels.set_labels(txn, kbid=kbid, labels=labels)
        await txn.commit()

    async with maindb_driver.transaction() as txn:
        assert await datamanagers.labels._get_labelset_ids(txn, kbid=kbid) == ["bar"]


async def test_labelset_ids_bw_compat(maindb_driver):
    kbid = "foo"
    # Check that initially all are empty
    async with maindb_driver.transaction() as txn:
        assert (
            await datamanagers.labels._deprecated_scan_labelset_ids(txn, kbid=kbid)
            == []
        )
        assert await datamanagers.labels._get_labelset_ids(txn, kbid=kbid) is None
        assert (
            await datamanagers.labels._get_labelset_ids_bw_compat(txn, kbid=kbid) == []
        )

    # Check that adding to the list creates the list
    async with maindb_driver.transaction() as txn:
        await datamanagers.labels._add_to_labelset_ids(
            txn, kbid=kbid, labelsets=["bar", "ba"]
        )
        await txn.commit()
    async with maindb_driver.transaction() as txn:
        assert await datamanagers.labels._get_labelset_ids(txn, kbid=kbid) == [
            "bar",
            "ba",
        ]
        assert (
            await datamanagers.labels._deprecated_scan_labelset_ids(txn, kbid=kbid)
            == []
        )

    # Check that adding appends to the list
    async with maindb_driver.transaction() as txn:
        await datamanagers.labels._add_to_labelset_ids(
            txn, kbid=kbid, labelsets=["baz", "ba"]
        )
        await txn.commit()
    async with maindb_driver.transaction() as txn:
        assert await datamanagers.labels._get_labelset_ids(txn, kbid=kbid) == [
            "bar",
            "ba",
            "baz",
        ]

    # Check that removing from the list removes the item
    async with maindb_driver.transaction() as txn:
        await datamanagers.labels._delete_from_labelset_ids(
            txn, kbid=kbid, labelsets=["ba"]
        )
        await txn.commit()
    async with maindb_driver.transaction() as txn:
        assert await datamanagers.labels._get_labelset_ids(txn, kbid=kbid) == [
            "bar",
            "baz",
        ]

    # Check that removing also creates the list
    async with maindb_driver.transaction() as txn:
        assert await datamanagers.labels._get_labelset_ids(txn, kbid="other") is None
        await datamanagers.labels._delete_from_labelset_ids(
            txn, kbid="other", labelsets=["bar", "baz"]
        )
        await txn.commit()
    async with maindb_driver.transaction() as txn:
        assert await datamanagers.labels._get_labelset_ids(txn, kbid="other") == []

    # Check legacy method
    async with maindb_driver.transaction() as txn:
        await txn.set("kb/labels/foo/1", b"somedata")
        await txn.commit()
    async with maindb_driver.transaction() as txn:
        await datamanagers.labels._get_labelset_ids(txn, kbid="foo") is None
        await datamanagers.labels._deprecated_scan_labelset_ids(txn, kbid="foo") == [
            "1"
        ]
        await datamanagers.labels._get_labelset_ids_bw_compat(txn, kbid="foo") == ["1"]
