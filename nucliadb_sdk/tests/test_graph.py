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
import nucliadb_sdk
from nucliadb_models.graph.requests import (
    GraphNodesSearchRequest,
    GraphRelationsSearchRequest,
    GraphSearchRequest,
)


async def test_graph_search(
    docs_dataset: str, sdk: nucliadb_sdk.NucliaDB, sdk_async: nucliadb_sdk.NucliaDBAsync
):
    # paths

    paths_request = GraphSearchRequest.model_validate(
        {
            "query": {
                "prop": "path",
                "source": {
                    "type": "entity",
                },
            },
            "top_k": 10,
        }
    )

    results = sdk.graph_search(kbid=docs_dataset, content=paths_request)
    assert len(results.paths) == 10

    results = await sdk_async.graph_search(kbid=docs_dataset, content=paths_request)
    assert len(results.paths) == 10

    # nodes

    nodes_request = GraphNodesSearchRequest.model_validate(
        {
            "query": {
                "prop": "node",
                "type": "entity",
            },
            "top_k": 10,
        }
    )

    results = sdk.graph_nodes(kbid=docs_dataset, content=nodes_request)
    assert len(results.nodes) == 10

    results = await sdk_async.graph_nodes(kbid=docs_dataset, content=nodes_request)
    assert len(results.nodes) == 10

    # relations

    relations_request = GraphRelationsSearchRequest.model_validate(
        {
            "query": {
                "prop": "relation",
                "label": "operating system",
            },
            "top_k": 10,
        }
    )

    results = sdk.graph_relations(kbid=docs_dataset, content=relations_request)
    assert len(results.relations) == 1

    results = await sdk_async.graph_relations(kbid=docs_dataset, content=relations_request)
    assert len(results.relations) == 1
