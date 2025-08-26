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

from nucliadb.common import datamanagers
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.resource import KB_RESOURCE_SLUG
from nucliadb_protos import resources_pb2
from nucliadb_protos.resources_pb2 import Basic


async def check_slug(driver: Driver, kbid, rid, slug):
    async with driver.transaction(read_only=True) as txn:
        basic = await datamanagers.resources.get_basic(txn, kbid=kbid, rid=rid)
        assert basic is not None
        assert basic.slug == slug
        uuid = await datamanagers.resources.get_resource_uuid_from_slug(txn, kbid=kbid, slug=slug)
        assert uuid == rid


@pytest.fixture(scope="function")
async def resource_with_slug(maindb_driver: Driver):
    kbid = "kbid"
    rid = "rid"
    slug = "slug"

    async with maindb_driver.transaction(read_only=False) as txn:
        await txn.set(KB_RESOURCE_SLUG.format(kbid=kbid, slug=slug), rid.encode())
        basic = Basic(slug=slug)
        await datamanagers.resources.set_basic(txn, kbid=kbid, rid=rid, basic=basic)
        await txn.commit()

    await check_slug(maindb_driver, kbid, rid, slug)

    return kbid, rid, slug


async def test_modify_slug(resource_with_slug, maindb_driver: Driver):
    kbid, rid, _ = resource_with_slug
    new_slug = "new_slug"

    async with maindb_driver.transaction(read_only=False) as txn:
        await datamanagers.resources.modify_slug(txn, kbid=kbid, rid=rid, new_slug=new_slug)
        await txn.commit()

    await check_slug(maindb_driver, kbid, rid, new_slug)


async def test_all_fields(maindb_driver: Driver):
    kbid = "mykb"
    rid = "myresource"
    field = resources_pb2.FieldID(field="myfield", field_type=resources_pb2.FieldType.LINK)

    async with maindb_driver.transaction(read_only=True) as txn:
        all_fields = await datamanagers.resources.get_all_field_ids(txn, kbid=kbid, rid=rid)
        assert all_fields is None

        assert (await datamanagers.resources.has_field(txn, kbid=kbid, rid=rid, field_id=field)) is False

    # set a field for a resource

    async with maindb_driver.transaction(read_only=False) as txn:
        pb = resources_pb2.AllFieldIDs()
        pb.fields.append(field)
        await datamanagers.resources.set_all_field_ids(txn, kbid=kbid, rid=rid, allfields=pb)
        await txn.commit()

    async with maindb_driver.transaction(read_only=True) as txn:
        all_fields = await datamanagers.resources.get_all_field_ids(txn, kbid=kbid, rid=rid)
        assert all_fields is not None
        assert len(all_fields.fields) == 1

        assert (await datamanagers.resources.has_field(txn, kbid=kbid, rid=rid, field_id=field)) is True

    # set no fields

    async with maindb_driver.transaction(read_only=False) as txn:
        await datamanagers.resources.set_all_field_ids(
            txn, kbid=kbid, rid=rid, allfields=resources_pb2.AllFieldIDs()
        )
        await txn.commit()

    async with maindb_driver.transaction(read_only=True) as txn:
        all_fields = await datamanagers.resources.get_all_field_ids(txn, kbid=kbid, rid=rid)
        assert all_fields is not None
        assert len(all_fields.fields) == 0

        assert (await datamanagers.resources.has_field(txn, kbid=kbid, rid=rid, field_id=field)) is False
