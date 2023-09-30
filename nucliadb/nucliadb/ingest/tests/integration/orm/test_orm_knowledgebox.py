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

from nucliadb.ingest.orm.knowledgebox import KnowledgeBox, chunker
from nucliadb.ingest.tests.fixtures import broker_resource


@pytest.mark.asyncio
async def test_knowledgebox_purge_handles_unexisting_shard_payload(
    gcs_storage, maindb_driver
):
    await KnowledgeBox.purge(maindb_driver, "idonotexist")


def test_chunker():
    total_items = 100
    chunk_size = 10
    iterations = 0
    for chunk in chunker(list(range(total_items)), chunk_size):
        assert len(chunk) == chunk_size
        assert chunk == list(
            range(iterations * chunk_size, (iterations * chunk_size) + chunk_size)
        )
        iterations += 1

    assert iterations == total_items / chunk_size

    iterations = 0
    for chunk in chunker([], 2):
        iterations += 1
    assert iterations == 0


@pytest.mark.asyncio
async def test_knowledgebox_delete_all_kb_keys(
    gcs_storage,
    cache,
    fake_node,
    maindb_driver,
    knowledgebox_ingest: str,
):
    txn = await maindb_driver.begin()
    kbid = knowledgebox_ingest
    kb_obj = KnowledgeBox(txn, gcs_storage, kbid=kbid)

    # Create some resources in the KB
    n_resources = 100
    uuids = set()
    for _ in range(n_resources):
        bm = broker_resource(kbid)
        r = await kb_obj.add_resource(uuid=bm.uuid, slug=bm.uuid, basic=bm.basic)
        assert r is not None
        await r.set_slug()
        uuids.add(bm.uuid)
    await txn.commit()

    # Check that all of them are there
    txn = await maindb_driver.begin()
    kb_obj = KnowledgeBox(txn, gcs_storage, kbid=kbid)
    for uuid in uuids:
        assert await kb_obj.get_resource_uuid_by_slug(uuid) == uuid
    await txn.abort()

    # Now delete all kb keys
    await KnowledgeBox.delete_all_kb_keys(maindb_driver, kbid, chunk_size=10)

    # Check that all of them were deleted
    txn = await maindb_driver.begin()
    kb_obj = KnowledgeBox(txn, gcs_storage, kbid=kbid)
    for uuid in uuids:
        assert await kb_obj.get_resource_uuid_by_slug(uuid) is None
    await txn.abort()
