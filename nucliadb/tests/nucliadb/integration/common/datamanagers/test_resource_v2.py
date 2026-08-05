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

from nucliadb.common.datamanagers import kb, resources
from nucliadb.common.maindb.driver import Driver
from nucliadb.common.maindb.exceptions import ConflictError
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.resource import Resource
from nucliadb_protos import resources_pb2

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.fixture(scope="function")
async def kbid(maindb_driver: Driver) -> str:
    """Create a new knowledge box and return its kbid."""
    kbid = KnowledgeBox.new_unique_kbid()
    async with maindb_driver.rw_transaction() as txn:
        await kb.set_slug(txn, slug=kbid, kbid=kbid)
        await txn.commit()
    return kbid


@pytest.mark.asyncio
async def test_set_slug_raises_conflict_error(
    maindb_driver: Driver,
    kbid: str,
) -> None:
    rid = Resource.new_unique_rid()

    async with maindb_driver.rw_transaction() as txn:
        await resources.set_slug(
            txn,
            kbid=kbid,
            rid=rid,
            slug="test-slug",
        )
        await txn.commit()

    # Now try to set the same slug again for another resource, which should raise a conflict error
    with pytest.raises(ConflictError):
        async with maindb_driver.rw_transaction() as txn:
            rid2 = Resource.new_unique_rid()
            await resources.set_slug(
                txn,
                kbid=kbid,
                rid=rid2,
                slug="test-slug",
            )
            await txn.commit()

    # But setting it for the previous resource should succeed, as it is idempotent
    async with maindb_driver.rw_transaction() as txn:
        await resources.set_slug(
            txn,
            kbid=kbid,
            rid=rid,
            slug="another-slug",
        )
        await txn.commit()


async def test_update_slug(
    maindb_driver: Driver,
    kbid: str,
) -> None:
    """
    Test that modifying a slug for a resource works correctly.
    """
    rid = Resource.new_unique_rid()
    initial_slug = "initial-slug"
    async with maindb_driver.rw_transaction() as txn:
        basic = resources_pb2.Basic(slug=initial_slug, title="Test Title")
        await resources.upsert(
            txn,
            kbid=kbid,
            rid=rid,
            slug=initial_slug,
            basic=basic,
        )
        await txn.commit()

    # Now modify the slug
    async with maindb_driver.rw_transaction() as txn:
        old_slug = await resources.update_slug(
            txn,
            kbid=kbid,
            rid=rid,
            new_slug="modified-slug",
        )
        await txn.commit()

    assert old_slug == initial_slug, f"Expected old slug to be 'initial-slug', got {old_slug}"


async def test_resource_set_get(
    maindb_driver: Driver,
    kbid: str,
) -> None:
    """
    Test that resource metadata can be set and retrieved correctly.
    """
    rid = Resource.new_unique_rid()
    assert {rid async for rid in resources.iterate_ids(kbid=kbid)} == set()

    async with maindb_driver.rw_transaction() as txn:
        assert await resources.exists(txn, kbid=kbid, rid=rid) is False
        assert await resources.count(txn, kbid=kbid) == 0

        await resources.set_slug(
            txn,
            kbid=kbid,
            rid=rid,
            slug="test-slug",
        )
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        assert await resources.count(txn, kbid=kbid) == 1
        assert [rid async for rid in resources.iterate_ids(kbid=kbid)] == [rid]
        assert await resources.exists(txn, kbid=kbid, rid=rid) is True
        assert await resources.get_basic(txn, kbid=kbid, rid=rid) is None
        data = await resources.get(
            txn, kbid=kbid, rid=rid, columns=("slug", "basic", "origin", "security", "extra", "shard")
        )
        assert data is not None
        assert data.slug == "test-slug"
        assert data.basic is None
        assert data.origin is None
        assert data.security is None
        assert data.extra is None
        assert data.shard is None
        assert await resources.get_uuid(txn, kbid=kbid, slug="test-slug") == rid

    async with maindb_driver.rw_transaction() as txn:
        extra = resources_pb2.Extra()
        extra.metadata.update({"key": "value"})
        await resources.upsert(
            txn,
            kbid=kbid,
            rid=rid,
            shard="shard-1",
            basic=resources_pb2.Basic(slug="test-slug", title="Test Title"),
            extra=extra,
            origin=resources_pb2.Origin(source_id="test-source"),
            security=resources_pb2.Security(access_groups=["group1", "group2"]),
        )
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        basic = await resources.get_basic(txn, kbid=kbid, rid=rid)
        assert basic is not None
        assert basic.slug == "test-slug"
        assert basic.title == "Test Title"
        data = await resources.get(
            txn, kbid=kbid, rid=rid, columns=("slug", "basic", "origin", "security", "extra", "shard")
        )
        assert data is not None
        origin = data.origin
        assert origin is not None
        assert origin.source_id == "test-source"
        security = data.security
        assert security is not None
        assert security.access_groups == ["group1", "group2"]
        extra_ = data.extra
        assert extra_ is not None
        assert extra_.metadata["key"] == "value"
        shard_id = data.shard
        assert shard_id == "shard-1"

    async with maindb_driver.rw_transaction() as txn:
        await resources.delete(txn, kbid=kbid, rid=rid)
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        assert await resources.exists(txn, kbid=kbid, rid=rid) is False
        assert await resources.count(txn, kbid=kbid) == 0
        assert [rid async for rid in resources.iterate_ids(kbid=kbid)] == []
        assert await resources.get_basic(txn, kbid=kbid, rid=rid) is None


async def test_upsert_distinguishes_none_from_unset(
    maindb_driver: Driver,
    kbid: str,
) -> None:
    rid = Resource.new_unique_rid()

    async with maindb_driver.rw_transaction() as txn:
        extra = resources_pb2.Extra()
        extra.metadata.update({"key": "value"})
        await resources.upsert(
            txn,
            kbid=kbid,
            rid=rid,
            slug="test-slug",
            shard="shard-1",
            basic=resources_pb2.Basic(slug="test-slug", title="Test Title"),
            origin=resources_pb2.Origin(source_id="test-source"),
            security=resources_pb2.Security(access_groups=["group1"]),
            extra=extra,
        )
        await txn.commit()

    async with maindb_driver.rw_transaction() as txn:
        await resources.upsert(
            txn,
            kbid=kbid,
            rid=rid,
            slug=None,
            basic=None,
            origin=resources.UNSET,
            security=resources.UNSET,
            extra=resources.UNSET,
            shard=resources.UNSET,
        )
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        data = await resources.get(
            txn, kbid=kbid, rid=rid, columns=("slug", "basic", "origin", "security", "extra", "shard")
        )
        assert data is not None
        assert data.slug is None
        assert data.basic is None
        assert data.origin is not None
        assert data.origin.source_id == "test-source"
        assert data.security is not None
        assert data.security.access_groups == ["group1"]
        assert data.extra is not None
        assert data.extra.metadata["key"] == "value"
        assert data.shard is not None
        assert data.shard == "shard-1"


async def test_get_selected_columns(
    maindb_driver: Driver,
    kbid: str,
) -> None:
    rid = Resource.new_unique_rid()

    async with maindb_driver.rw_transaction() as txn:
        await resources.upsert(
            txn,
            kbid=kbid,
            rid=rid,
            slug=None,
            shard="shard-1",
            basic=resources_pb2.Basic(slug="test-slug", title="Test Title"),
            origin=resources_pb2.Origin(source_id="test-source"),
        )
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        resource = await resources.get(txn, kbid=kbid, rid=rid, columns=("slug", "basic", "shard"))

    assert resource is not None
    assert resource.slug is None
    assert resource.shard == "shard-1"
    assert resource.basic is not resources.UNSET
    assert resource.basic is not None
    assert resource.basic.title == "Test Title"
    assert resource.origin is resources.UNSET
    assert resource.security is resources.UNSET
    assert resource.extra is resources.UNSET


@pytest.mark.asyncio
async def test_exists_returns_false_for_invalid_uuid(
    maindb_driver: Driver,
) -> None:
    async with maindb_driver.ro_transaction() as txn:
        result = await resources.exists(txn, kbid="not-a-valid-uuid", rid="also-not-valid")
    assert result is False
