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

from nucliadb.common.datamanagers import conversations, fields, kb, resources
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


def test_document_uris_are_kb_scoped() -> None:
    """Only the KnowledgeBox registry is shared; everything else lives in the KB database."""
    assert resources._uri("resource-1") == "/resources/resource-1.json"
    assert fields._uri("resource-1", TEXT, "a/b") == "/resources/resource-1/fields/t/a%2Fb.json"
    assert conversations._page_uri("resource-1", "a/b", 2) == (
        "/resources/resource-1/conversations/a%2Fb/2.json"
    )


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
    async with maindb_driver.rw_transaction(system=True) as txn:
        await kb.set_slug(txn, kbid=kbid, slug=f"slug-{kbid}")
        await txn.commit()
    return kbid


@pytest.fixture()
async def rid(maindb_driver: Driver, kbid: str) -> str:
    rid = Resource.new_unique_rid()
    async with maindb_driver.rw_transaction(kbid=kbid) as txn:
        await resources.set_slug(txn, kbid=kbid, rid=rid, slug=f"slug-{rid}")
        await txn.commit()
    return rid


# ---------------------------------------------------------------------------
# set / get
# ---------------------------------------------------------------------------


def make_text(body: str) -> rpb2.FieldText:
    return rpb2.FieldText(body=body, format=rpb2.FieldText.Format.PLAIN)


@pytest.mark.asyncio
async def test_set_and_get(maindb_driver: Driver, kbid: str, rid: str) -> None:
    payload = make_text("raw-field-value")

    async with maindb_driver.rw_transaction(kbid=kbid) as txn:
        await fields.set(txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="body", value=payload)
        await txn.commit()

    async with maindb_driver.ro_transaction(kbid=kbid) as txn:
        result = await fields.get(
            txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="body", pb_klass=rpb2.FieldText
        )

    assert result == payload


@pytest.mark.asyncio
async def test_set_overwrites_existing_value(maindb_driver: Driver, kbid: str, rid: str) -> None:
    async with maindb_driver.rw_transaction(kbid=kbid) as txn:
        await fields.set(
            txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="body", value=make_text("v1")
        )
        await txn.commit()

    async with maindb_driver.rw_transaction(kbid=kbid) as txn:
        await fields.set(
            txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="body", value=make_text("v2")
        )
        await txn.commit()

    async with maindb_driver.ro_transaction(kbid=kbid) as txn:
        result = await fields.get(
            txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="body", pb_klass=rpb2.FieldText
        )

    assert result is not None
    assert result.body == "v2"


@pytest.mark.asyncio
async def test_get_returns_none_for_missing_field(maindb_driver: Driver, kbid: str, rid: str) -> None:
    async with maindb_driver.ro_transaction(kbid=kbid) as txn:
        result = await fields.get(
            txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="nonexistent", pb_klass=rpb2.FieldText
        )
    assert result is None


# ---------------------------------------------------------------------------
# set_status / get_status / get_statuses
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_and_get_status(maindb_driver: Driver, kbid: str, rid: str) -> None:
    async with maindb_driver.rw_transaction(kbid=kbid) as txn:
        await fields.set(
            txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="body", value=make_text("data")
        )
        await fields.set_status(
            txn,
            kbid=kbid,
            rid=rid,
            field_type=TEXT,
            field_id="body",
            status=make_status(wpb2.FieldStatus.Status.PROCESSED),
        )
        await txn.commit()

    async with maindb_driver.ro_transaction(kbid=kbid) as txn:
        result = await fields.get_status(txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="body")

    assert result is not None
    assert result.status == wpb2.FieldStatus.Status.PROCESSED


@pytest.mark.asyncio
async def test_get_status_returns_none_for_missing_row(
    maindb_driver: Driver, kbid: str, rid: str
) -> None:
    async with maindb_driver.ro_transaction(kbid=kbid) as txn:
        result = await fields.get_status(
            txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="no-such-field"
        )
    assert result is None


@pytest.mark.asyncio
async def test_get_statuses_returns_in_order(maindb_driver: Driver, kbid: str, rid: str) -> None:
    async with maindb_driver.rw_transaction(kbid=kbid) as txn:
        for fid in ("f1", "f2", "f3"):
            await fields.set(
                txn, kbid=kbid, rid=rid, field_type=TEXT, field_id=fid, value=make_text("x")
            )
        await fields.set_status(
            txn,
            kbid=kbid,
            rid=rid,
            field_type=TEXT,
            field_id="f1",
            status=make_status(wpb2.FieldStatus.Status.PROCESSED),
        )
        await fields.set_status(
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

    async with maindb_driver.ro_transaction(kbid=kbid) as txn:
        statuses = await fields.get_statuses(txn, kbid=kbid, rid=rid, fields=field_ids)

    assert len(statuses) == 3
    assert statuses[0].status == wpb2.FieldStatus.Status.PROCESSED  # f1
    assert statuses[1].status == wpb2.FieldStatus.Status.PENDING  # f2 - default empty
    assert statuses[2].status == wpb2.FieldStatus.Status.ERROR  # f3


@pytest.mark.asyncio
async def test_get_statuses_empty_input(maindb_driver: Driver, kbid: str, rid: str) -> None:
    async with maindb_driver.ro_transaction(kbid=kbid) as txn:
        result = await fields.get_statuses(txn, kbid=kbid, rid=rid, fields=[])
    assert result == []


# ---------------------------------------------------------------------------
# has_field
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_has_field_true_after_set(maindb_driver: Driver, kbid: str, rid: str) -> None:
    async with maindb_driver.rw_transaction(kbid=kbid) as txn:
        await fields.set(txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="body", value=make_text("x"))
        await txn.commit()

    fid = rpb2.FieldID(field_type=rpb2.FieldType.TEXT, field="body")
    async with maindb_driver.ro_transaction(kbid=kbid) as txn:
        assert await fields.exists(txn, kbid=kbid, rid=rid, field_id=fid) is True


@pytest.mark.asyncio
async def test_has_field_false_for_missing(maindb_driver: Driver, kbid: str, rid: str) -> None:
    fid = rpb2.FieldID(field_type=rpb2.FieldType.TEXT, field="ghost")
    async with maindb_driver.ro_transaction(kbid=kbid) as txn:
        assert await fields.exists(txn, kbid=kbid, rid=rid, field_id=fid) is False


# ---------------------------------------------------------------------------
# get_all_field_ids
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_all_field_ids(maindb_driver: Driver, kbid: str, rid: str) -> None:
    async with maindb_driver.rw_transaction(kbid=kbid) as txn:
        await fields.set(txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="body", value=make_text("x"))
        await fields.set(
            txn, kbid=kbid, rid=rid, field_type=FILE, field_id="doc", value=rpb2.FieldFile()
        )
        # These are the special title/summary generic fields that should be excluded
        await fields.set(txn, kbid=kbid, rid=rid, field_type="a", field_id="title", value=make_text("t"))
        await fields.set(
            txn, kbid=kbid, rid=rid, field_type="a", field_id="summary", value=make_text("s")
        )
        await txn.commit()

    async with maindb_driver.ro_transaction(kbid=kbid) as txn:
        result = await fields.get_all_field_ids(txn, kbid=kbid, rid=rid)

    field_pairs = {(f.field_type, f.field) for f in result.fields}
    assert (rpb2.FieldType.TEXT, "body") in field_pairs
    assert (rpb2.FieldType.FILE, "doc") in field_pairs
    # title and summary generics must be excluded
    assert (rpb2.FieldType.GENERIC, "title") not in field_pairs
    assert (rpb2.FieldType.GENERIC, "summary") not in field_pairs


@pytest.mark.asyncio
async def test_get_all_field_ids_empty(maindb_driver: Driver, kbid: str, rid: str) -> None:
    async with maindb_driver.ro_transaction(kbid=kbid) as txn:
        result = await fields.get_all_field_ids(txn, kbid=kbid, rid=rid)
    assert list(result.fields) == []


@pytest.mark.asyncio
async def test_fields_are_scoped_by_resource_and_md5(maindb_driver: Driver, kbid: str, rid: str) -> None:
    other_rid = Resource.new_unique_rid()
    other_kbid = KnowledgeBox.new_unique_kbid()
    async with maindb_driver.rw_transaction(kbid=kbid) as txn:
        await fields.set(
            txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="a/b", value=make_text("first")
        )
        await fields.set(
            txn, kbid=kbid, rid=other_rid, field_type=TEXT, field_id="a/b", value=make_text("second")
        )
        await fields.set_md5(txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="a/b", md5="hash-1")
        await txn.commit()

    async with maindb_driver.rw_transaction(kbid=other_kbid) as txn:
        await fields.set(
            txn, kbid=other_kbid, rid=rid, field_type=TEXT, field_id="a/b", value=make_text("third")
        )
        await txn.commit()

    async with maindb_driver.ro_transaction(kbid=kbid) as txn:
        first = await fields.get(
            txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="a/b", pb_klass=rpb2.FieldText
        )
        second = await fields.get(
            txn, kbid=kbid, rid=other_rid, field_type=TEXT, field_id="a/b", pb_klass=rpb2.FieldText
        )
        assert first is not None and first.body == "first"
        assert second is not None and second.body == "second"
        ids = await fields.get_all_field_ids(txn, kbid=kbid, rid=rid)
        assert [(field.field_type, field.field) for field in ids.fields] == [
            (rpb2.FieldType.TEXT, "a/b")
        ]
        assert await fields.exists_md5(txn, kbid=kbid, md5="hash-1", field_type=TEXT)
        assert not await fields.exists_md5(txn, kbid=kbid, md5="hash-1", field_type=FILE)

    async with maindb_driver.ro_transaction(kbid=other_kbid) as txn:
        third = await fields.get(
            txn, kbid=other_kbid, rid=rid, field_type=TEXT, field_id="a/b", pb_klass=rpb2.FieldText
        )
        assert third is not None and third.body == "third"
        assert not await fields.exists_md5(txn, kbid=other_kbid, md5="hash-1", field_type=TEXT)


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_removes_field(maindb_driver: Driver, kbid: str, rid: str) -> None:
    async with maindb_driver.rw_transaction(kbid=kbid) as txn:
        await fields.set(txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="body", value=make_text("x"))
        await txn.commit()

    async with maindb_driver.rw_transaction(kbid=kbid) as txn:
        await fields.delete(txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="body")
        await txn.commit()

    async with maindb_driver.ro_transaction(kbid=kbid) as txn:
        assert (
            await fields.get(
                txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="body", pb_klass=rpb2.FieldText
            )
            is None
        )
        fid = rpb2.FieldID(field_type=rpb2.FieldType.TEXT, field="body")
        assert await fields.exists(txn, kbid=kbid, rid=rid, field_id=fid) is False


@pytest.mark.asyncio
async def test_delete_nonexistent_field_is_noop(maindb_driver: Driver, kbid: str, rid: str) -> None:
    async with maindb_driver.rw_transaction(kbid=kbid) as txn:
        await fields.delete(txn, kbid=kbid, rid=rid, field_type=TEXT, field_id="ghost")
        await txn.commit()  # must not raise
