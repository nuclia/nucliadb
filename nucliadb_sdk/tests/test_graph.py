# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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

    graph_search = sdk.graph_search(kbid=docs_dataset, content=paths_request)
    assert len(graph_search.paths) == 10

    graph_search = await sdk_async.graph_search(kbid=docs_dataset, content=paths_request)
    assert len(graph_search.paths) == 10

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

    nodes_search = sdk.graph_nodes(kbid=docs_dataset, content=nodes_request)
    assert len(nodes_search.nodes) == 10

    nodes_search = await sdk_async.graph_nodes(kbid=docs_dataset, content=nodes_request)
    assert len(nodes_search.nodes) == 10

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

    relations_search = sdk.graph_relations(kbid=docs_dataset, content=relations_request)
    assert len(relations_search.relations) == 1

    relations_search = await sdk_async.graph_relations(kbid=docs_dataset, content=relations_request)
    assert len(relations_search.relations) == 1
