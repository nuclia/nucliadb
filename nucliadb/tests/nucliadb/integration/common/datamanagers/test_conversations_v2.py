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
Integration tests for nucliadb.common.datamanagers.conversations_v2.

Covers: set_metadata, get_metadata, set_page, get_page,
        set_splits_metadata, get_splits_metadata, delete_field.
"""

import pytest

from nucliadb.common.datamanagers import conversations_v2, fields_v2, kb_v2, resources_v2
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.resource import Resource
from nucliadb_protos.resources_pb2 import (
    Conversation,
    FieldConversation,
    Message,
    SplitMetadata,
    SplitsMetadata,
)

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


@pytest.fixture()
async def field_id(maindb_driver: Driver, kbid: str, rid: str) -> str:
    """Create the parent kb_fields row ('c' type) so FK constraints are satisfied."""
    fid = "chat"
    async with maindb_driver.rw_transaction() as txn:
        await fields_v2.set(txn, kbid=kbid, rid=rid, field_type="c", field_id=fid, value=b"")
        await txn.commit()
    return fid


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_conversation(*texts: str) -> Conversation:
    conv = Conversation()
    for text in texts:
        msg = conv.messages.add()
        msg.content.text = text
        msg.type = Message.MessageType.QUESTION
    return conv


def make_metadata(pages: int = 1) -> FieldConversation:
    meta = FieldConversation()
    meta.pages = pages
    meta.total = pages * 10
    return meta


def make_splits_metadata(*split_ids: str) -> SplitsMetadata:
    meta = SplitsMetadata()
    for sid in split_ids:
        meta.metadata[sid].CopyFrom(SplitMetadata())
    return meta


# ---------------------------------------------------------------------------
# set_metadata / get_metadata
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_and_get_metadata(maindb_driver: Driver, kbid: str, rid: str, field_id: str) -> None:
    meta = make_metadata(pages=3)

    async with maindb_driver.rw_transaction() as txn:
        await conversations_v2.set_metadata(txn, kbid=kbid, rid=rid, field_id=field_id, metadata=meta)
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        result = await conversations_v2.get_metadata(txn, kbid=kbid, rid=rid, field_id=field_id)

    assert result is not None
    assert result.pages == 3


@pytest.mark.asyncio
async def test_set_metadata_overwrites(
    maindb_driver: Driver, kbid: str, rid: str, field_id: str
) -> None:
    async with maindb_driver.rw_transaction() as txn:
        await conversations_v2.set_metadata(
            txn, kbid=kbid, rid=rid, field_id=field_id, metadata=make_metadata(pages=1)
        )
        await txn.commit()

    async with maindb_driver.rw_transaction() as txn:
        await conversations_v2.set_metadata(
            txn, kbid=kbid, rid=rid, field_id=field_id, metadata=make_metadata(pages=5)
        )
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        result = await conversations_v2.get_metadata(txn, kbid=kbid, rid=rid, field_id=field_id)

    assert result is not None
    assert result.pages == 5


@pytest.mark.asyncio
async def test_get_metadata_returns_none_for_missing_field(
    maindb_driver: Driver, kbid: str, rid: str
) -> None:
    async with maindb_driver.ro_transaction() as txn:
        result = await conversations_v2.get_metadata(txn, kbid=kbid, rid=rid, field_id="no-such-field")
    assert result is None


# ---------------------------------------------------------------------------
# set_page / get_page
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_and_get_page(maindb_driver: Driver, kbid: str, rid: str, field_id: str) -> None:
    conv = make_conversation("Hello", "World")

    async with maindb_driver.rw_transaction() as txn:
        await conversations_v2.set_page(txn, kbid=kbid, rid=rid, field_id=field_id, page=1, value=conv)
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        result = await conversations_v2.get_page(txn, kbid=kbid, rid=rid, field_id=field_id, page=1)

    assert result is not None
    assert len(result.messages) == 2
    assert result.messages[0].content.text == "Hello"
    assert result.messages[1].content.text == "World"


@pytest.mark.asyncio
async def test_set_page_overwrites(maindb_driver: Driver, kbid: str, rid: str, field_id: str) -> None:
    async with maindb_driver.rw_transaction() as txn:
        await conversations_v2.set_page(
            txn, kbid=kbid, rid=rid, field_id=field_id, page=1, value=make_conversation("v1")
        )
        await txn.commit()

    async with maindb_driver.rw_transaction() as txn:
        await conversations_v2.set_page(
            txn, kbid=kbid, rid=rid, field_id=field_id, page=1, value=make_conversation("v2")
        )
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        result = await conversations_v2.get_page(txn, kbid=kbid, rid=rid, field_id=field_id, page=1)

    assert result is not None
    assert result.messages[0].content.text == "v2"


@pytest.mark.asyncio
async def test_multiple_pages_are_independent(
    maindb_driver: Driver, kbid: str, rid: str, field_id: str
) -> None:
    async with maindb_driver.rw_transaction() as txn:
        await conversations_v2.set_page(
            txn, kbid=kbid, rid=rid, field_id=field_id, page=1, value=make_conversation("page-one")
        )
        await conversations_v2.set_page(
            txn, kbid=kbid, rid=rid, field_id=field_id, page=2, value=make_conversation("page-two")
        )
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        p1 = await conversations_v2.get_page(txn, kbid=kbid, rid=rid, field_id=field_id, page=1)
        p2 = await conversations_v2.get_page(txn, kbid=kbid, rid=rid, field_id=field_id, page=2)

    assert p1 is not None and p1.messages[0].content.text == "page-one"
    assert p2 is not None and p2.messages[0].content.text == "page-two"


@pytest.mark.asyncio
async def test_get_page_returns_none_for_missing_page(
    maindb_driver: Driver, kbid: str, rid: str, field_id: str
) -> None:
    async with maindb_driver.ro_transaction() as txn:
        result = await conversations_v2.get_page(txn, kbid=kbid, rid=rid, field_id=field_id, page=99)
    assert result is None


@pytest.mark.asyncio
async def test_set_page_rejects_page_zero(
    maindb_driver: Driver, kbid: str, rid: str, field_id: str
) -> None:
    async with maindb_driver.rw_transaction() as txn:
        with pytest.raises(ValueError, match="pages start at index 1"):
            await conversations_v2.set_page(
                txn, kbid=kbid, rid=rid, field_id=field_id, page=0, value=make_conversation("x")
            )


@pytest.mark.asyncio
async def test_get_page_rejects_page_zero(
    maindb_driver: Driver, kbid: str, rid: str, field_id: str
) -> None:
    async with maindb_driver.ro_transaction() as txn:
        with pytest.raises(ValueError, match="pages start at index 1"):
            await conversations_v2.get_page(txn, kbid=kbid, rid=rid, field_id=field_id, page=0)


# ---------------------------------------------------------------------------
# set_splits_metadata / get_splits_metadata
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_and_get_splits_metadata(
    maindb_driver: Driver, kbid: str, rid: str, field_id: str
) -> None:
    splits = make_splits_metadata("split-a", "split-b")

    async with maindb_driver.rw_transaction() as txn:
        await conversations_v2.set_splits_metadata(
            txn, kbid=kbid, rid=rid, field_id=field_id, splits_metadata=splits
        )
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        result = await conversations_v2.get_splits_metadata(txn, kbid=kbid, rid=rid, field_id=field_id)

    assert result is not None
    assert "split-a" in result.metadata
    assert "split-b" in result.metadata


@pytest.mark.asyncio
async def test_get_splits_metadata_returns_none_when_not_set(
    maindb_driver: Driver, kbid: str, rid: str, field_id: str
) -> None:
    async with maindb_driver.ro_transaction() as txn:
        result = await conversations_v2.get_splits_metadata(txn, kbid=kbid, rid=rid, field_id=field_id)
    assert result is None


@pytest.mark.asyncio
async def test_splits_metadata_does_not_clash_with_pages(
    maindb_driver: Driver, kbid: str, rid: str, field_id: str
) -> None:
    """The sentinel page=0 must not conflict with real page data."""
    async with maindb_driver.rw_transaction() as txn:
        await conversations_v2.set_splits_metadata(
            txn,
            kbid=kbid,
            rid=rid,
            field_id=field_id,
            splits_metadata=make_splits_metadata("s1"),
        )
        await conversations_v2.set_page(
            txn, kbid=kbid, rid=rid, field_id=field_id, page=1, value=make_conversation("msg")
        )
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        splits = await conversations_v2.get_splits_metadata(txn, kbid=kbid, rid=rid, field_id=field_id)
        page = await conversations_v2.get_page(txn, kbid=kbid, rid=rid, field_id=field_id, page=1)

    assert splits is not None and "s1" in splits.metadata
    assert page is not None and page.messages[0].content.text == "msg"


# ---------------------------------------------------------------------------
# delete_field
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_field_removes_all_pages_and_metadata(
    maindb_driver: Driver, kbid: str, rid: str, field_id: str
) -> None:
    async with maindb_driver.rw_transaction() as txn:
        await conversations_v2.set_page(
            txn, kbid=kbid, rid=rid, field_id=field_id, page=1, value=make_conversation("p1")
        )
        await conversations_v2.set_page(
            txn, kbid=kbid, rid=rid, field_id=field_id, page=2, value=make_conversation("p2")
        )
        await conversations_v2.set_splits_metadata(
            txn,
            kbid=kbid,
            rid=rid,
            field_id=field_id,
            splits_metadata=make_splits_metadata("s"),
        )
        await txn.commit()

    async with maindb_driver.rw_transaction() as txn:
        await conversations_v2.delete_field(txn, kbid=kbid, rid=rid, field_id=field_id)
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        assert (
            await conversations_v2.get_page(txn, kbid=kbid, rid=rid, field_id=field_id, page=1) is None
        )
        assert (
            await conversations_v2.get_page(txn, kbid=kbid, rid=rid, field_id=field_id, page=2) is None
        )
        assert (
            await conversations_v2.get_splits_metadata(txn, kbid=kbid, rid=rid, field_id=field_id)
            is None
        )


@pytest.mark.asyncio
async def test_delete_field_noop_when_no_rows_exist(maindb_driver: Driver, kbid: str, rid: str) -> None:
    async with maindb_driver.rw_transaction() as txn:
        await conversations_v2.delete_field(txn, kbid=kbid, rid=rid, field_id="ghost")
        await txn.commit()  # must not raise
