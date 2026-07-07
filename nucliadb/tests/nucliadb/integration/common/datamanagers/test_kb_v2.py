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
Integration tests for nucliadb.common.datamanagers.kb_v2.

Covers every public function in the module:
  set_config, delete, soft_delete, set_kbid_for_slug, update_kb_shards,
  exists_kb, get_config, get_kb_uuid, get_kbs, get_kb_shards.
"""

import pytest

from nucliadb.common.datamanagers import kb_v2
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb_protos import knowledgebox_pb2, writer_pb2

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def new_kbid() -> str:
    return KnowledgeBox.new_unique_kbid()


def make_config(title: str = "Test KB") -> knowledgebox_pb2.KnowledgeBoxConfig:
    cfg = knowledgebox_pb2.KnowledgeBoxConfig()
    cfg.title = title
    return cfg


def make_shards(kbid: str) -> writer_pb2.Shards:
    shards = writer_pb2.Shards()
    shards.kbid = kbid
    return shards


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
async def kbid(maindb_driver: Driver) -> str:
    """Create a minimal KB row (with a slug) and return its kbid."""
    kbid = new_kbid()
    async with maindb_driver.rw_transaction() as txn:
        await kb_v2.set_kbid_for_slug(txn, kbid=kbid, slug=f"slug-{kbid}")
        await txn.commit()
    return kbid


# ---------------------------------------------------------------------------
# set_config / get_config
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_config_creates_row_and_is_readable(maindb_driver: Driver) -> None:
    kbid = new_kbid()
    cfg = make_config("My KB")

    async with maindb_driver.rw_transaction() as txn:
        await kb_v2.set_config(txn, kbid=kbid, config=cfg)
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        result = await kb_v2.get_config(txn, kbid=kbid)

    assert result is not None
    assert result.title == "My KB"


@pytest.mark.asyncio
async def test_set_config_overwrites_existing(maindb_driver: Driver) -> None:
    kbid = new_kbid()

    async with maindb_driver.rw_transaction() as txn:
        await kb_v2.set_config(txn, kbid=kbid, config=make_config("First"))
        await txn.commit()

    async with maindb_driver.rw_transaction() as txn:
        await kb_v2.set_config(txn, kbid=kbid, config=make_config("Second"))
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        result = await kb_v2.get_config(txn, kbid=kbid)

    assert result is not None
    assert result.title == "Second"


@pytest.mark.asyncio
async def test_get_config_returns_none_for_missing_kb(maindb_driver: Driver) -> None:
    async with maindb_driver.ro_transaction() as txn:
        result = await kb_v2.get_config(txn, kbid=new_kbid())
    assert result is None


# ---------------------------------------------------------------------------
# set_kbid_for_slug / get_kb_uuid
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_kbid_for_slug_and_get_kb_uuid(maindb_driver: Driver) -> None:
    kbid = new_kbid()
    slug = f"my-slug-{kbid}"

    async with maindb_driver.rw_transaction() as txn:
        await kb_v2.set_kbid_for_slug(txn, kbid=kbid, slug=slug)
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        result = await kb_v2.get_kb_uuid(txn, slug=slug)

    assert result == kbid


@pytest.mark.asyncio
async def test_set_kbid_for_slug_overwrites_slug(maindb_driver: Driver) -> None:
    kbid = new_kbid()

    async with maindb_driver.rw_transaction() as txn:
        await kb_v2.set_kbid_for_slug(txn, kbid=kbid, slug="old-slug")
        await txn.commit()

    async with maindb_driver.rw_transaction() as txn:
        await kb_v2.set_kbid_for_slug(txn, kbid=kbid, slug="new-slug")
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        assert await kb_v2.get_kb_uuid(txn, slug="new-slug") == kbid
        assert await kb_v2.get_kb_uuid(txn, slug="old-slug") is None


@pytest.mark.asyncio
async def test_get_kb_uuid_returns_none_for_unknown_slug(maindb_driver: Driver) -> None:
    async with maindb_driver.ro_transaction() as txn:
        result = await kb_v2.get_kb_uuid(txn, slug="no-such-slug")
    assert result is None


# ---------------------------------------------------------------------------
# exists_kb
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_exists_kb_true_for_active_kb(maindb_driver: Driver, kbid: str) -> None:
    async with maindb_driver.ro_transaction() as txn:
        assert await kb_v2.exists_kb(txn, kbid=kbid) is True


@pytest.mark.asyncio
async def test_exists_kb_false_for_missing_kb(maindb_driver: Driver) -> None:
    async with maindb_driver.ro_transaction() as txn:
        assert await kb_v2.exists_kb(txn, kbid=new_kbid()) is False


@pytest.mark.asyncio
async def test_exists_kb_false_after_soft_delete(maindb_driver: Driver, kbid: str) -> None:
    async with maindb_driver.rw_transaction() as txn:
        await kb_v2.soft_delete(txn, kbid=kbid)
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        assert await kb_v2.exists_kb(txn, kbid=kbid) is False


@pytest.mark.asyncio
async def test_exists_kb_false_after_hard_delete(maindb_driver: Driver, kbid: str) -> None:
    async with maindb_driver.rw_transaction() as txn:
        await kb_v2.delete(txn, kbid=kbid)
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        assert await kb_v2.exists_kb(txn, kbid=kbid) is False


# ---------------------------------------------------------------------------
# soft_delete
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_soft_delete_clears_slug(maindb_driver: Driver, kbid: str) -> None:
    async with maindb_driver.rw_transaction() as txn:
        await kb_v2.soft_delete(txn, kbid=kbid)
        await txn.commit()

    # The row still exists but slug is NULL, so get_kb_uuid returns None for the old slug
    async with maindb_driver.ro_transaction() as txn:
        result = await kb_v2.get_kb_uuid(txn, slug=f"slug-{kbid}")
    assert result is None


@pytest.mark.asyncio
async def test_soft_delete_on_nonexistent_kb_is_noop(maindb_driver: Driver) -> None:
    """soft_delete must not raise when the KB does not exist."""
    async with maindb_driver.rw_transaction() as txn:
        await kb_v2.soft_delete(txn, kbid=new_kbid())  # must not raise
        await txn.commit()


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_removes_kb_row(maindb_driver: Driver, kbid: str) -> None:
    async with maindb_driver.rw_transaction() as txn:
        await kb_v2.delete(txn, kbid=kbid)
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        assert await kb_v2.get_config(txn, kbid=kbid) is None
        assert await kb_v2.exists_kb(txn, kbid=kbid) is False


@pytest.mark.asyncio
async def test_delete_on_nonexistent_kb_is_noop(maindb_driver: Driver) -> None:
    async with maindb_driver.rw_transaction() as txn:
        await kb_v2.delete(txn, kbid=new_kbid())  # must not raise
        await txn.commit()


# ---------------------------------------------------------------------------
# update_kb_shards / get_kb_shards
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_and_get_kb_shards(maindb_driver: Driver, kbid: str) -> None:
    shards = make_shards(kbid)

    async with maindb_driver.rw_transaction() as txn:
        await kb_v2.update_kb_shards(txn, kbid=kbid, shards=shards)
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        result = await kb_v2.get_kb_shards(txn, kbid=kbid)

    assert result is not None
    assert result.kbid == kbid


@pytest.mark.asyncio
async def test_get_kb_shards_returns_none_for_missing_kb(maindb_driver: Driver) -> None:
    async with maindb_driver.ro_transaction() as txn:
        result = await kb_v2.get_kb_shards(txn, kbid=new_kbid())
    assert result is None


@pytest.mark.asyncio
async def test_get_kb_shards_for_update(maindb_driver: Driver, kbid: str) -> None:
    shards = make_shards(kbid)

    async with maindb_driver.rw_transaction() as txn:
        await kb_v2.update_kb_shards(txn, kbid=kbid, shards=shards)
        await txn.commit()

    # for_update=True is a SELECT … FOR UPDATE; verify it returns the same data
    async with maindb_driver.rw_transaction() as txn:
        result = await kb_v2.get_kb_shards(txn, kbid=kbid, for_update=True)

    assert result is not None
    assert result.kbid == kbid


# ---------------------------------------------------------------------------
# get_kbs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_kbs_yields_active_kbs(maindb_driver: Driver) -> None:
    kbid_a = new_kbid()
    kbid_b = new_kbid()

    async with maindb_driver.rw_transaction() as txn:
        await kb_v2.set_kbid_for_slug(txn, kbid=kbid_a, slug="prefix-alpha")
        await kb_v2.set_kbid_for_slug(txn, kbid=kbid_b, slug="prefix-beta")
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        all_kbs = {kbid async for kbid, _ in kb_v2.get_kbs(txn)}

    assert kbid_a in all_kbs
    assert kbid_b in all_kbs


@pytest.mark.asyncio
async def test_get_kbs_with_slug_prefix(maindb_driver: Driver) -> None:
    kbid_a = new_kbid()
    kbid_b = new_kbid()
    kbid_c = new_kbid()
    prefix = f"pfx-{kbid_a[:8]}-"

    async with maindb_driver.rw_transaction() as txn:
        await kb_v2.set_kbid_for_slug(txn, kbid=kbid_a, slug=f"{prefix}one")
        await kb_v2.set_kbid_for_slug(txn, kbid=kbid_b, slug=f"{prefix}two")
        await kb_v2.set_kbid_for_slug(txn, kbid=kbid_c, slug="other-slug")
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        filtered = {kbid async for kbid, _ in kb_v2.get_kbs(txn, slug_prefix=prefix)}

    assert kbid_a in filtered
    assert kbid_b in filtered
    assert kbid_c not in filtered


@pytest.mark.asyncio
async def test_get_kbs_does_not_yield_soft_deleted(maindb_driver: Driver) -> None:
    kbid = new_kbid()

    async with maindb_driver.rw_transaction() as txn:
        await kb_v2.set_kbid_for_slug(txn, kbid=kbid, slug=f"slug-{kbid}")
        await txn.commit()

    async with maindb_driver.rw_transaction() as txn:
        await kb_v2.soft_delete(txn, kbid=kbid)
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        all_kbs = {kbid async for kbid, _ in kb_v2.get_kbs(txn)}

    assert kbid not in all_kbs
