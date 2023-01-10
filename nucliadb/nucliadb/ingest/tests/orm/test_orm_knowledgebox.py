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

from nucliadb.ingest.orm.knowledgebox import KnowledgeBox, iter_in_chunks
from nucliadb.ingest.tests.fixtures import broker_resource


@pytest.mark.asyncio
async def test_knowledgebox_purge_handles_unexisting_shard_payload(
    gcs_storage, redis_driver
):
    await KnowledgeBox.purge(redis_driver, "idonotexist")


@pytest.mark.asyncio
async def test_iter_in_chunks():
    async def generate_n(n):
        if n is None:
            return
        for i in range(n):
            yield i

    total_items = 100
    chunk_size = 10
    iterations = 0
    async for chunk in iter_in_chunks(generate_n(total_items), chunk_size=chunk_size):
        assert len(chunk) == chunk_size
        assert chunk == list(
            range(iterations * chunk_size, (iterations * chunk_size) + chunk_size)
        )
        iterations += 1

    assert iterations == total_items / chunk_size

    # Check when generator doesn't yield anything
    iterations = None
    async for chunk in iter_in_chunks(generate_n(None)):
        iterations += 1
    assert iterations is None


@pytest.fixture(scope="function")
def tikv_driver_configured(tikv_driver):
    from nucliadb.ingest.settings import settings
    from nucliadb_utils.store import MAIN

    prev_driver = settings.driver
    settings.driver = "tikv"
    settings.driver_tikv_url = tikv_driver.url
    MAIN["driver"] = tikv_driver_configured

    yield

    settings.driver = prev_driver
    MAIN.pop("driver", None)

@pytest.fixture(scope="function")
async def tikv_txn(tikv_driver):
    txn = await tikv_driver.begin()
    yield txn
    await txn.abort()


@pytest.mark.asyncio
async def test_knowledgebox_delete_all_kb_keys(
    gcs_storage,
    cache,
    fake_node,
    tikv_driver_configured,
    tikv_driver,
    knowledgebox_ingest: str,
):
    txn = await tikv_driver.begin()
    kbid = knowledgebox_ingest
    kb_obj = KnowledgeBox(txn, gcs_storage, cache, kbid=kbid)

    # Create some resources in the KB
    n_resources = 1000
    uuids = set()
    for _ in range(n_resources):
        bm = broker_resource(kbid)
        r = await kb_obj.add_resource(uuid=bm.uuid, slug=bm.uuid, basic=bm.basic)
        assert r is not None
        await r.set_slug()
        uuids.add(bm.uuid)
    await txn.commit(resource=False)

    # Check that all of them are there
    txn = await tikv_driver.begin()
    kb_obj = KnowledgeBox(txn, gcs_storage, cache, kbid=kbid)
    for uuid in uuids:
        assert await kb_obj.get_resource_uuid_by_slug(uuid) == uuid
    await txn.abort()

    # Now delete all kb keys
    await KnowledgeBox.delete_all_kb_keys(tikv_driver, kbid)

    # Check that all of them were deleted
    txn = await tikv_driver.begin()
    kb_obj = KnowledgeBox(txn, gcs_storage, cache, kbid=kbid)
    for uuid in uuids:
        assert await kb_obj.get_resource_uuid_by_slug(uuid) is None
    await txn.abort()
