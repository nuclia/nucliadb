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
from httpx import AsyncClient
from nidx_protos.nodereader_pb2 import GetShardRequest, SuggestFeatures, SuggestRequest
from nidx_protos.noderesources_pb2 import ShardId

from nucliadb.common.datamanagers.kb import get_shards as get_kb_shards
from nucliadb.common.maindb.utils import get_driver
from nucliadb.common.nidx import get_nidx_api_client, get_nidx_searcher_client
from nucliadb.search.api.v1.router import KB_PREFIX


@pytest.mark.deploy_modes("cluster")
async def test_suggest_resource_all(nucliadb_search: AsyncClient, test_search_resource: str) -> None:
    kbid = test_search_resource

    resp = await nucliadb_search.get(
        f"/{KB_PREFIX}/{kbid}/suggest?query=own+text",
    )
    assert resp.status_code == 200
    paragraph_results = resp.json()["paragraphs"]["results"]
    assert len(paragraph_results) == 1

    # get shards ids

    driver = get_driver()
    async with driver.ro_transaction() as txn:
        kb_shards = await get_kb_shards(txn, kbid=kbid)
        assert kb_shards is not None
        shard_id = kb_shards.shards[0].nidx_shard_id
        shard = await get_nidx_api_client().GetShard(GetShardRequest(shard_id=ShardId(id=shard_id)))
        assert shard.shard_id == shard_id
        assert shard.fields == 3
        assert shard.paragraphs == 2
        assert shard.sentences == 3

        prequest = SuggestRequest(
            features=[SuggestFeatures.ENTITIES, SuggestFeatures.PARAGRAPHS],
        )
        prequest.shard_ids[:] = [shard_id]
        prequest.body = "Ramon"
        suggest = await get_nidx_searcher_client().Suggest(prequest)
        assert suggest.total == 1, f"Request:\n{prequest}\nResponse:\n{suggest}"
