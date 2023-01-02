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
    gcs_storage,
    redis_driver,
    txn,
    cache,
):
    await KnowledgeBox.purge(redis_driver, "idonotexist")


@pytest.mark.asyncio
async def test_iter_in_chunks():
    async def thegenerator(n):
        for i in range(n):
            yield i

    total_items = 100
    chunk_size = 10
    iterations = 0
    async for chunk in iter_in_chunks(thegenerator(total_items), chunk_size=chunk_size):
        assert len(chunk) == 10
        assert chunk == list(
            range(iterations * chunk_size, (iterations * chunk_size) + chunk_size)
        )
        iterations += 1

    assert iterations == 10


@pytest.mark.asyncio
async def test_knowledgebox_delete_all_kb_keys(
    gcs_storage,
    redis_driver,
    txn,
    cache,
    fake_node,
    knowledgebox_ingest: str,
):
    kbid = knowledgebox_ingest
    kb_obj = KnowledgeBox(txn, gcs_storage, cache, kbid=kbid)

    # Create some resources in the KB
    for i in range(10):
        uuid = f"myresource{i}"
        bm = broker_resource(kbid, uuid)
        r = await kb_obj.add_resource(uuid=uuid, slug=uuid, basic=bm.basic)
        assert r is not None

    await txn.commit(resource=False)

    # Now delete all kb keys
    await KnowledgeBox.delete_all_kb_keys(redis_driver, kbid, chunk_size=3)

    # Check that all of them were deleted
    for i in range(10):
        uuid = f"myresource{i}"
        assert await kb_obj.get_resource_uuid_by_slug(uuid) is None
