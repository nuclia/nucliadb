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

from nucliadb_models.graph.responses import GraphNodesSearchResponse


@pytest.mark.deploy_modes("standalone")
async def test_graph_nodes_search(
    nucliadb_reader: AsyncClient,
    kb_with_entity_graph: str,
):
    kbid = kb_with_entity_graph

    # Search all nodes
    # (n)
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph/nodes",
        json={
            "query": {
                "prop": "node",
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    nodes = GraphNodesSearchResponse.model_validate(resp.json()).nodes
    assert len(nodes) == 18

    # Search PERSON nodes
    # (n:PERSON)
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph/nodes",
        json={
            "query": {
                "prop": "node",
                "group": "PERSON",
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    nodes = GraphNodesSearchResponse.model_validate(resp.json()).nodes
    assert len(nodes) == 6
