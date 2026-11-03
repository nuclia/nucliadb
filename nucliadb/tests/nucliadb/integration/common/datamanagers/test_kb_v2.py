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

from nucliadb.common.datamanagers import kb
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb_protos import knowledgebox_pb2
from tests.utils.knowledgebox import create_knowledgebox

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def new_kbid() -> str:
    return KnowledgeBox.new_unique_kbid()


def make_config(title: str = "Test KB") -> knowledgebox_pb2.KnowledgeBoxConfig:
    cfg = knowledgebox_pb2.KnowledgeBoxConfig()
    cfg.title = title
    return cfg


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
async def kbid(maindb_driver: Driver) -> str:
    """Create a KB through the ORM and return its kbid."""
    return await create_knowledgebox(maindb_driver)


# ---------------------------------------------------------------------------
# set_config / get_config
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_config_creates_row_and_is_readable(maindb_driver: Driver) -> None:

    kbid = await create_knowledgebox(maindb_driver)
    cfg = make_config("My KB")

    async with maindb_driver.rw_transaction(kbid=kbid) as txn:
        await kb.set(txn, kbid=kbid, config=cfg)
        await txn.commit()

    async with maindb_driver.ro_transaction(kbid=kbid) as txn:
        result = await kb.get_config(txn, kbid=kbid)

    assert result is not None
    assert result.title == "My KB"


@pytest.mark.asyncio
async def test_set_config_overwrites_existing(maindb_driver: Driver) -> None:
    kbid = await create_knowledgebox(maindb_driver)

    async with maindb_driver.rw_transaction(kbid=kbid) as txn:
        await kb.set(txn, kbid=kbid, config=make_config("First"))
        await txn.commit()

    async with maindb_driver.rw_transaction(kbid=kbid) as txn:
        await kb.set(txn, kbid=kbid, config=make_config("Second"))
        await txn.commit()

    async with maindb_driver.ro_transaction(kbid=kbid) as txn:
        result = await kb.get_config(txn, kbid=kbid)

    assert result is not None
    assert result.title == "Second"


@pytest.mark.asyncio
async def test_get_config_returns_none_for_missing_kb(maindb_driver: Driver) -> None:
    kbid = new_kbid()
    async with maindb_driver.ro_transaction(kbid=kbid) as txn:
        result = await kb.get_config(txn, kbid=kbid)
    assert result is None


# ---------------------------------------------------------------------------
# set_slug / get_kbid
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_slug_and_get_kbid(maindb_driver: Driver) -> None:
    kbid = await create_knowledgebox(maindb_driver)
    slug = f"my-slug-{kbid}"

    async with maindb_driver.rw_transaction(kbid=None) as txn:
        await kb.set_slug(txn, kbid=kbid, slug=slug)
        await txn.commit()

    async with maindb_driver.ro_transaction(kbid=None) as txn:
        result = await kb.get_kbid(txn, slug=slug)

    assert result == kbid


@pytest.mark.asyncio
async def test_set_slug_overwrites_slug(maindb_driver: Driver) -> None:
    kbid = await create_knowledgebox(maindb_driver)

    async with maindb_driver.rw_transaction(kbid=None) as txn:
        await kb.set_slug(txn, kbid=kbid, slug="old-slug")
        await txn.commit()

    async with maindb_driver.rw_transaction(kbid=None) as txn:
        await kb.set_slug(txn, kbid=kbid, slug="new-slug")
        await txn.commit()

    async with maindb_driver.ro_transaction(kbid=None) as txn:
        assert await kb.get_kbid(txn, slug="new-slug") == kbid
        assert await kb.get_kbid(txn, slug="old-slug") is None


@pytest.mark.asyncio
async def test_get_kbid_returns_none_for_unknown_slug(maindb_driver: Driver) -> None:
    async with maindb_driver.ro_transaction(kbid=None) as txn:
        result = await kb.get_kbid(txn, slug="no-such-slug")
    assert result is None


# ---------------------------------------------------------------------------
# exists_kb
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_exists_kb_true_for_active_kb(maindb_driver: Driver, kbid: str) -> None:
    async with maindb_driver.ro_transaction(kbid=None) as txn:
        assert await kb.exists(txn, kbid=kbid) is True


@pytest.mark.asyncio
async def test_exists_kb_false_for_missing_kb(maindb_driver: Driver) -> None:
    async with maindb_driver.ro_transaction(kbid=None) as txn:
        assert await kb.exists(txn, kbid=new_kbid()) is False


@pytest.mark.asyncio
async def test_exists_kb_false_after_soft_delete(maindb_driver: Driver, kbid: str) -> None:
    async with maindb_driver.rw_transaction(kbid=None) as txn:
        await kb.soft_delete(txn, kbid=kbid)
        await txn.commit()

    async with maindb_driver.ro_transaction(kbid=None) as txn:
        assert await kb.exists(txn, kbid=kbid) is False


@pytest.mark.asyncio
async def test_exists_kb_false_after_hard_delete(maindb_driver: Driver, kbid: str) -> None:
    async with maindb_driver.rw_transaction(kbid=None) as txn:
        await kb.delete(txn, kbid=kbid)
        await txn.commit()

    async with maindb_driver.ro_transaction(kbid=None) as txn:
        assert await kb.exists(txn, kbid=kbid) is False


# ---------------------------------------------------------------------------
# soft_delete
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_soft_delete_clears_slug(maindb_driver: Driver, kbid: str) -> None:
    async with maindb_driver.rw_transaction(kbid=None) as txn:
        await kb.soft_delete(txn, kbid=kbid)
        await txn.commit()

    # The row still exists but slug is NULL, so get_kbid returns None for the old slug
    async with maindb_driver.ro_transaction(kbid=None) as txn:
        result = await kb.get_kbid(txn, slug=f"slug-{kbid}")
    assert result is None


@pytest.mark.asyncio
async def test_soft_delete_on_nonexistent_kb_is_noop(maindb_driver: Driver) -> None:
    """soft_delete must not raise when the KB does not exist."""
    async with maindb_driver.rw_transaction(kbid=None) as txn:
        await kb.soft_delete(txn, kbid=new_kbid())  # must not raise
        await txn.commit()


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_removes_kb_row(maindb_driver: Driver, kbid: str) -> None:
    async with maindb_driver.rw_transaction(kbid=None) as txn:
        await kb.delete(txn, kbid=kbid)
        await txn.commit()

    async with maindb_driver.ro_transaction(kbid=kbid) as txn:
        assert await kb.get_config(txn, kbid=kbid) is None

    async with maindb_driver.ro_transaction(kbid=None) as txn:
        assert await kb.exists(txn, kbid=kbid) is False


@pytest.mark.asyncio
async def test_delete_on_nonexistent_kb_is_noop(maindb_driver: Driver) -> None:
    async with maindb_driver.rw_transaction(kbid=None) as txn:
        await kb.delete(txn, kbid=new_kbid())  # must not raise
        await txn.commit()


# ---------------------------------------------------------------------------
# get_kbs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_kbs_yields_active_kbs(maindb_driver: Driver) -> None:
    kbid_a = await create_knowledgebox(maindb_driver)
    kbid_b = await create_knowledgebox(maindb_driver)

    async with maindb_driver.ro_transaction(kbid=None) as txn:
        all_kbs = {kbid async for kbid, _ in kb.iter(txn)}

    assert kbid_a in all_kbs
    assert kbid_b in all_kbs


@pytest.mark.asyncio
async def test_get_kbs_with_slug_prefix(maindb_driver: Driver) -> None:
    prefix = f"pfx-{new_kbid()[:8]}-"
    kbid_a = await create_knowledgebox(maindb_driver, slug=f"{prefix}one")
    kbid_b = await create_knowledgebox(maindb_driver, slug=f"{prefix}two")
    kbid_c = await create_knowledgebox(maindb_driver)

    async with maindb_driver.ro_transaction(kbid=None) as txn:
        filtered = {kbid async for kbid, _ in kb.iter(txn, slug_prefix=prefix)}

    assert kbid_a in filtered
    assert kbid_b in filtered
    assert kbid_c not in filtered


@pytest.mark.asyncio
async def test_get_kbs_does_not_yield_soft_deleted(maindb_driver: Driver) -> None:
    kbid = await create_knowledgebox(maindb_driver)

    async with maindb_driver.rw_transaction(kbid=None) as txn:
        await kb.soft_delete(txn, kbid=kbid)
        await txn.commit()

    async with maindb_driver.ro_transaction(kbid=None) as txn:
        all_kbs = {kbid async for kbid, _ in kb.iter(txn)}

    assert kbid not in all_kbs
