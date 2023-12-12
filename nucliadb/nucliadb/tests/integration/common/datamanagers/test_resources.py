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
from nucliadb_protos.resources_pb2 import Basic

from nucliadb.common.datamanagers.resources import ResourcesDataManager
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.resource import KB_RESOURCE_SLUG
from nucliadb.ingest.orm.utils import set_basic

pytestmark = pytest.mark.asyncio


async def check_slug(driver: Driver, kbid, rid, slug):
    async with driver.transaction() as txn:
        basic = await ResourcesDataManager.get_resource_basic(txn, kbid, rid)
        assert basic is not None
        assert basic.slug == slug
        uuid = await ResourcesDataManager.get_resource_uuid_from_slug(txn, kbid, slug)
        assert uuid == rid


@pytest.fixture(scope="function")
async def resource_with_slug(maindb_driver: Driver):
    kbid = "kbid"
    rid = "rid"
    slug = "slug"

    async with maindb_driver.transaction() as txn:
        await txn.set(KB_RESOURCE_SLUG.format(kbid=kbid, slug=slug), rid.encode())
        basic = Basic(slug=slug)
        await set_basic(txn, kbid, rid, basic)
        await txn.commit()

    await check_slug(maindb_driver, kbid, rid, slug)

    return kbid, rid, slug


async def test_modify_slug_by_uuid(resource_with_slug, maindb_driver: Driver):
    kbid, rid, old_slug = resource_with_slug
    new_slug = "new_slug"

    # Modify the slug
    async with maindb_driver.transaction() as txn:
        await ResourcesDataManager.modify_slug_by_uuid(txn, kbid, rid, new_slug)
        await txn.commit()

    await check_slug(maindb_driver, kbid, rid, new_slug)


async def test_modify_slug_by_slug(resource_with_slug, maindb_driver: Driver):
    kbid, rid, old_slug = resource_with_slug
    new_slug = "new_slug"

    # Modify the slug
    async with maindb_driver.transaction() as txn:
        await ResourcesDataManager.modify_slug_by_slug(txn, kbid, old_slug, new_slug)
        await txn.commit()

    await check_slug(maindb_driver, kbid, rid, new_slug)
