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

from nucliadb_models.metadata import RelationEntity, RelationType


@pytest.mark.deploy_modes("standalone")
async def test_user_defined_knowledge_graph(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
    entity_graph: tuple[dict[str, RelationEntity], dict[str, RelationType], list[tuple[str, str, str]]],
):
    kbid = standalone_knowledgebox
    entities, relations, paths = entity_graph

    graph = paths[:3]
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "Knowledge graph",
            "slug": "knowledge-graph",
            "summary": "User defined knowledge graph",
            "usermetadata": {
                "relations": [
                    {
                        "relation": relations[relation].value,
                        "label": relation,
                        "from": entities[source].model_dump(),
                        "to": entities[target].model_dump(),
                    }
                    for source, relation, target in graph
                ],
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/resource/{rid}",
        params={
            "show": ["basic", "relations"],
        },
    )
    assert resp.status_code == 200
    user_graph = resp.json()["usermetadata"]["relations"]
    assert len(user_graph) == len(graph)

    # Update graph

    graph = paths
    resp = await nucliadb_writer.patch(
        f"/kb/{kbid}/resource/{rid}",
        json={
            "usermetadata": {
                "relations": [
                    {
                        "relation": "ENTITY",
                        "label": relation,
                        "from": entities[source].model_dump(),
                        "to": entities[target].model_dump(),
                    }
                    for source, relation, target in graph
                ],
            },
        },
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/resource/{rid}",
        params={
            "show": ["basic", "relations"],
        },
    )
    assert resp.status_code == 200
    user_graph = resp.json()["usermetadata"]["relations"]
    assert len(user_graph) == len(graph)

    # Search graph

    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json={
            "query_entities": [
                {
                    "name": entities["Anna"].value,
                    "type": entities["Anna"].type.value,
                    "subtype": entities["Anna"].group,
                }
            ],
            "features": ["relations"],
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    retrieved_graph = set()
    for source, subgraph in body["relations"]["entities"].items():
        for target in subgraph["related_to"]:
            if target["direction"] == "in":
                path = (source, "-", target["relation_label"], "->", target["entity"])
            else:
                path = (source, "<-", target["relation_label"], "-", target["entity"])

            assert path not in retrieved_graph, "We don't expect duplicated paths"
            retrieved_graph.add(path)

    assert len(retrieved_graph) == 5
