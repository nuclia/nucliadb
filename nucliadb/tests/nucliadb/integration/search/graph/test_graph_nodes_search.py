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

# FIXME: all asserts here are wrong, as we are not deduplicating nor Rust is
# returning the proper entities. Fix all asserts once both issues are tackled


@pytest.mark.deploy_modes("standalone")
async def test_graph_nodes_search(
    nucliadb_reader: AsyncClient,
    kb_with_entity_graph: str,
):
    kbid = kb_with_entity_graph

    # (:PERSON)
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
    assert len(nodes) == 24

    # (:PERSON)-[]->()
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph/nodes",
        json={
            "query": {
                "prop": "source_node",
                "group": "PERSON",
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    nodes = GraphNodesSearchResponse.model_validate(resp.json()).nodes
    assert len(nodes) == 24

    # ()-[]->(:PERSON)
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph/nodes",
        json={
            "query": {
                "prop": "destination_node",
                "group": "PERSON",
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    nodes = GraphNodesSearchResponse.model_validate(resp.json()).nodes
    assert len(nodes) == 6

    # (:Anastasia) -- implemented as an or instead of using prop=node
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph/nodes",
        json={
            "query": {
                "or": [
                    {
                        "prop": "source_node",
                        "value": "Anastasia",
                        "group": "PERSON",
                    },
                    {
                        "prop": "destination_node",
                        "value": "Anastasia",
                        "group": "PERSON",
                    },
                ],
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    nodes = GraphNodesSearchResponse.model_validate(resp.json()).nodes
    assert len(nodes) == 4
