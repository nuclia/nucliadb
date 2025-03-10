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

from nucliadb_models.graph.responses import GraphRelationsSearchResponse

# FIXME: in this case, the number of relations returned is correct but they are
# duplicated, as Rust don't deduplicate them. Maybe the response should be
# different?


@pytest.mark.deploy_modes("standalone")
async def test_graph_nodes_search(
    nucliadb_reader: AsyncClient,
    kb_with_entity_graph: str,
):
    kbid = kb_with_entity_graph

    # [:LIVE_IN]
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph/relations",
        json={
            "query": {
                "prop": "relation",
                "label": "LIVE_IN",
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    relations = GraphRelationsSearchResponse.model_validate(resp.json()).relations
    assert len(relations) == 2

    # [: LIVE_IN | BORN_IN]
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph/relations",
        json={
            "query": {
                "or": [
                    {
                        "prop": "relation",
                        "label": "LIVE_IN",
                    },
                    {
                        "prop": "relation",
                        "label": "BORN_IN",
                    },
                ]
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    relations = GraphRelationsSearchResponse.model_validate(resp.json()).relations
    assert len(relations) == 3

    # [:!LIVE_IN]
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/graph/relations",
        json={
            "query": {
                "not": {
                    "prop": "relation",
                    "label": "LIVE_IN",
                }
            },
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    relations = GraphRelationsSearchResponse.model_validate(resp.json()).relations
    assert len(relations) == 14
