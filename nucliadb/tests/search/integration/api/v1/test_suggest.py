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

from httpx import AsyncClient

from nucliadb.common.datamanagers.cluster import KB_SHARDS
from nucliadb.common.maindb.utils import get_driver
from nucliadb.common.nidx import get_nidx_fake_node
from nucliadb.search.api.v1.router import KB_PREFIX
from nucliadb_protos.nodereader_pb2 import SuggestFeatures, SuggestRequest
from nucliadb_protos.writer_pb2 import Shards as PBShards


# @pytest.mark.flaky(reruns=5)
async def test_suggest_resource_all(
    cluster_nucliadb_search: AsyncClient, test_search_resource: str
) -> None:
    kbid = test_search_resource

    resp = await cluster_nucliadb_search.get(
        f"/{KB_PREFIX}/{kbid}/suggest?query=own+text",
    )
    assert resp.status_code == 200
    paragraph_results = resp.json()["paragraphs"]["results"]
    assert len(paragraph_results) == 1

    # get shards ids

    driver = get_driver()
    async with driver.transaction(read_only=True) as txn:
        key = KB_SHARDS.format(kbid=kbid)
        node_obj = get_nidx_fake_node()
        async for key in txn.keys(key):
            value = await txn.get(key)
            assert value is not None
            shards = PBShards()
            shards.ParseFromString(value)

            shard_id = shards.shards[0].nidx_shard_id
            shard = await node_obj.get_shard(shard_id)
            assert shard.shard_id == shard_id
            assert shard.fields == 3
            assert shard.paragraphs == 2
            assert shard.sentences == 3

            prequest = SuggestRequest(
                features=[SuggestFeatures.ENTITIES, SuggestFeatures.PARAGRAPHS],
            )
            prequest.shard = shard_id
            prequest.body = "Ramon"
            suggest = await node_obj.reader.Suggest(prequest)  # type: ignore
            assert suggest.total == 1, f"Request:\n{prequest}\nResponse:\n{suggest}"
