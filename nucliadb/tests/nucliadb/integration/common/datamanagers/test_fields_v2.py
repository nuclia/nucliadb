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
"""
Integration tests for nucliadb.common.datamanagers.fields_v2.

Covers: set, set_status, get_raw, get_status, get_statuses, get_all_field_ids,
        has_field, delete.
"""

import pytest

from nucliadb.common.datamanagers import fields_v2, kb_v2, resources_v2
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.resource import Resource
from nucliadb_protos import resources_pb2 as rpb2
from nucliadb_protos import writer_pb2 as wpb2

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TEXT = "t"
FILE = "f"


def make_status(
    code: wpb2.FieldStatus.Status.ValueType = wpb2.FieldStatus.Status.PROCESSED,
) -> wpb2.FieldStatus:
    s = wpb2.FieldStatus()
    s.status = code
    return s


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
async def kbid(maindb_driver: Driver) -> str:
    kbid = KnowledgeBox.new_unique_kbid()
    async with maindb_driver.rw_transaction() as txn:
        await kb_v2.set_kbid_for_slug(txn, kbid=kbid, slug=f"slug-{kbid}")
        await txn.commit()
    return kbid


@pytest.fixture()
async def rid(maindb_driver: Driver, kbid: str) -> str:
    rid = Resource.new_unique_rid()
    async with maindb_driver.rw_transaction() as txn:
        await resources_v2.set_slug(txn, kbid=kbid, rid=rid, slug=f"slug-{rid}")
        await txn.commit()
    return rid


# ---------------------------------------------------------------------------
# set / get_raw
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_and_get_raw(maindb_driver: Driver, kbid: str, rid: str) -> None:
    payload = b"raw-field-bytes"

    async with maindb_driver.rw_transaction() as txn:
        await fields_v2.set(txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="body", value=payload)
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        result = await fields_v2.get_raw(txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="body")

    assert result == payload


@pytest.mark.asyncio
async def test_set_overwrites_existing_value(maindb_driver: Driver, kbid: str, rid: str) -> None:
    async with maindb_driver.rw_transaction() as txn:
        await fields_v2.set(txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="body", value=b"v1")
        await txn.commit()

    async with maindb_driver.rw_transaction() as txn:
        await fields_v2.set(txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="body", value=b"v2")
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        result = await fields_v2.get_raw(txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="body")

    assert result == b"v2"


@pytest.mark.asyncio
async def test_get_raw_returns_none_for_missing_field(
    maindb_driver: Driver, kbid: str, rid: str
) -> None:
    async with maindb_driver.ro_transaction() as txn:
        result = await fields_v2.get_raw(
            txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="nonexistent"
        )
    assert result is None


@pytest.mark.asyncio
async def test_set_with_protobuf_message(maindb_driver: Driver, kbid: str, rid: str) -> None:
    text_field = rpb2.FieldText(body="hello world", format=rpb2.FieldText.Format.PLAIN)

    async with maindb_driver.rw_transaction() as txn:
        await fields_v2.set(txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="body", value=text_field)
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        raw = await fields_v2.get_raw(txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="body")

    assert raw is not None
    recovered = rpb2.FieldText()
    recovered.ParseFromString(raw)
    assert recovered.body == "hello world"


# ---------------------------------------------------------------------------
# set_status / get_status / get_statuses
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_and_get_status(maindb_driver: Driver, kbid: str, rid: str) -> None:
    async with maindb_driver.rw_transaction() as txn:
        await fields_v2.set(txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="body", value=b"data")
        await fields_v2.set_status(
            txn,
            kbid=kbid,
            rid=rid,
            field_type=TEXT,
            field_id="body",
            status=make_status(wpb2.FieldStatus.Status.PROCESSED),
        )
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        result = await fields_v2.get_status(txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="body")

    assert result is not None
    assert result.status == wpb2.FieldStatus.Status.PROCESSED


@pytest.mark.asyncio
async def test_get_status_returns_none_for_missing_row(
    maindb_driver: Driver, kbid: str, rid: str
) -> None:
    async with maindb_driver.ro_transaction() as txn:
        result = await fields_v2.get_status(
            txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="no-such-field"
        )
    assert result is None


@pytest.mark.asyncio
async def test_get_statuses_returns_in_order(maindb_driver: Driver, kbid: str, rid: str) -> None:
    async with maindb_driver.rw_transaction() as txn:
        for fid in ("f1", "f2", "f3"):
            await fields_v2.set(txn, kbid=kbid, rid=rid, field_type=TEXT, field_id=fid, value=b"x")
        await fields_v2.set_status(
            txn,
            kbid=kbid,
            rid=rid,
            field_type=TEXT,
            field_id="f1",
            status=make_status(wpb2.FieldStatus.Status.PROCESSED),
        )
        await fields_v2.set_status(
            txn,
            kbid=kbid,
            rid=rid,
            field_type=TEXT,
            field_id="f3",
            status=make_status(wpb2.FieldStatus.Status.ERROR),
        )
        await txn.commit()

    field_ids = [
        rpb2.FieldID(field_type=rpb2.FieldType.TEXT, field="f1"),
        rpb2.FieldID(field_type=rpb2.FieldType.TEXT, field="f2"),
        rpb2.FieldID(field_type=rpb2.FieldType.TEXT, field="f3"),
    ]

    async with maindb_driver.ro_transaction() as txn:
        statuses = await fields_v2.get_statuses(txn, kbid=kbid, rid=rid, fields=field_ids)

    assert len(statuses) == 3
    assert statuses[0].status == wpb2.FieldStatus.Status.PROCESSED  # f1
    assert statuses[1].status == wpb2.FieldStatus.Status.PENDING  # f2 - default empty
    assert statuses[2].status == wpb2.FieldStatus.Status.ERROR  # f3


@pytest.mark.asyncio
async def test_get_statuses_empty_input(maindb_driver: Driver, kbid: str, rid: str) -> None:
    async with maindb_driver.ro_transaction() as txn:
        result = await fields_v2.get_statuses(txn, kbid=kbid, rid=rid, fields=[])
    assert result == []


# ---------------------------------------------------------------------------
# has_field
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_has_field_true_after_set(maindb_driver: Driver, kbid: str, rid: str) -> None:
    async with maindb_driver.rw_transaction() as txn:
        await fields_v2.set(txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="body", value=b"x")
        await txn.commit()

    fid = rpb2.FieldID(field_type=rpb2.FieldType.TEXT, field="body")
    async with maindb_driver.ro_transaction() as txn:
        assert await fields_v2.has_field(txn, kbid=kbid, rid=rid, field_id=fid) is True


@pytest.mark.asyncio
async def test_has_field_false_for_missing(maindb_driver: Driver, kbid: str, rid: str) -> None:
    fid = rpb2.FieldID(field_type=rpb2.FieldType.TEXT, field="ghost")
    async with maindb_driver.ro_transaction() as txn:
        assert await fields_v2.has_field(txn, kbid=kbid, rid=rid, field_id=fid) is False


# ---------------------------------------------------------------------------
# get_all_field_ids
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_all_field_ids(maindb_driver: Driver, kbid: str, rid: str) -> None:
    async with maindb_driver.rw_transaction() as txn:
        await fields_v2.set(txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="body", value=b"x")
        await fields_v2.set(txn, kbid=kbid, rid=rid, field_type=FILE, field_id="doc", value=b"y")
        # These are the special title/summary generic fields that should be excluded
        await fields_v2.set(txn, kbid=kbid, rid=rid, field_type="a", field_id="title", value=b"t")
        await fields_v2.set(txn, kbid=kbid, rid=rid, field_type="a", field_id="summary", value=b"s")
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        result = await fields_v2.get_all_field_ids(txn, kbid=kbid, rid=rid)

    field_pairs = {(f.field_type, f.field) for f in result.fields}
    assert (rpb2.FieldType.TEXT, "body") in field_pairs
    assert (rpb2.FieldType.FILE, "doc") in field_pairs
    # title and summary generics must be excluded
    assert (rpb2.FieldType.GENERIC, "title") not in field_pairs
    assert (rpb2.FieldType.GENERIC, "summary") not in field_pairs


@pytest.mark.asyncio
async def test_get_all_field_ids_empty(maindb_driver: Driver, kbid: str, rid: str) -> None:
    async with maindb_driver.ro_transaction() as txn:
        result = await fields_v2.get_all_field_ids(txn, kbid=kbid, rid=rid)
    assert list(result.fields) == []


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_removes_field(maindb_driver: Driver, kbid: str, rid: str) -> None:
    async with maindb_driver.rw_transaction() as txn:
        await fields_v2.set(txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="body", value=b"x")
        await txn.commit()

    async with maindb_driver.rw_transaction() as txn:
        await fields_v2.delete(txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="body")
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        assert await fields_v2.get_raw(txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="body") is None
        fid = rpb2.FieldID(field_type=rpb2.FieldType.TEXT, field="body")
        assert await fields_v2.has_field(txn, kbid=kbid, rid=rid, field_id=fid) is False


@pytest.mark.asyncio
async def test_delete_nonexistent_field_is_noop(maindb_driver: Driver, kbid: str, rid: str) -> None:
    async with maindb_driver.rw_transaction() as txn:
        await fields_v2.delete(txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="ghost")
        await txn.commit()  # must not raise
