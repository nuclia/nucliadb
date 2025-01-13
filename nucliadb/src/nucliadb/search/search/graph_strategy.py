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

import asyncio
import heapq
import json
from collections import defaultdict
from datetime import datetime
from typing import Any, Collection, Iterable, Optional, Union

from nuclia_models.predict.generative_responses import (
    JSONGenerativeResponse,
    MetaGenerativeResponse,
    StatusGenerativeResponse,
)
from sentry_sdk import capture_exception

from nucliadb.search import logger
from nucliadb.search.requesters.utils import Method, node_query
from nucliadb.search.search.chat.query import (
    find_request_from_ask_request,
    get_relations_results_from_entities,
)
from nucliadb.search.search.find import query_parser_from_find_request
from nucliadb.search.search.find_merge import (
    compose_find_resources,
    hydrate_and_rerank,
    paragraph_id_to_text_block_matches,
)
from nucliadb.search.search.hydrator import ResourceHydrationOptions, TextBlockHydrationOptions
from nucliadb.search.search.merge import merge_suggest_results
from nucliadb.search.search.metrics import RAGMetrics
from nucliadb.search.search.query import QueryParser
from nucliadb.search.search.rerankers import Reranker, RerankingOptions
from nucliadb.search.utilities import get_predict
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import ExtractedDataTypeName
from nucliadb_models.search import (
    AskRequest,
    ChatModel,
    DirectionalRelation,
    EntitySubgraph,
    GraphStrategy,
    KnowledgeboxFindResults,
    KnowledgeboxSuggestResults,
    NucliaDBClientType,
    RelationDirection,
    Relations,
    ResourceProperties,
    UserPrompt,
)
from nucliadb_protos import nodereader_pb2
from nucliadb_protos.utils_pb2 import RelationNode

SCHEMA = {
    "title": "score_triplets",
    "description": "Return a list of triplets and their relevance scores (0-10) for the supplied question.",
    "type": "object",
    "properties": {
        "triplets": {
            "type": "array",
            "description": "A list of triplets with their relevance scores.",
            "items": {
                "type": "object",
                "properties": {
                    "head_entity": {"type": "string", "description": "The first entity in the triplet."},
                    "relationship": {
                        "type": "string",
                        "description": "The relationship between the two entities.",
                    },
                    "tail_entity": {
                        "type": "string",
                        "description": "The second entity in the triplet.",
                    },
                    "score": {
                        "type": "integer",
                        "description": "A relevance score in the range 0 to 10.",
                        "minimum": 0,
                        "maximum": 10,
                    },
                },
                "required": ["head_entity", "relationship", "tail_entity", "score"],
            },
        }
    },
    "required": ["triplets"],
}

PROMPT = """\
You are an advanced language model assisting in scoring relationships (edges) between two entities in a knowledge graph, given a user’s question.

For each provided **(head_entity, relationship, tail_entity)**, you must:
1. Assign a **relevance score** between **0** and **10**.
2. **0** means “this relationship can’t be relevant at all to the question.”
3. **10** means “this relationship is extremely relevant to the question.”
4. You may use **any integer** between 0 and 10 (e.g., 3, 7, etc.) based on how relevant you deem the relationship to be.
5. **Language Agnosticism**: The question and the relationships may be in different languages. The relevance scoring should still work and be agnostic of the language.
6. Relationships that may not answer the question directly but expand knowledge in a relevant way, should also be scored positively.

Once you have decided the best score for each triplet, return these results **using a function call** in JSON format with the following rules:

- The function name should be `score_triplets`.
- The first argument should be the list of triplets.
- Each triplet should have the following keys:
  - `head_entity`: The first entity in the triplet.
  - `relationship`: The relationship between the two entities.
  - `tail_entity`: The second entity in the triplet.
  - `score`: The relevance score in the range 0 to 10.

You **must** comply with the provided JSON Schema to ensure a well-structured response and mantain the order of the triplets.


## Examples:

### Example 1:

**Input**

{
 "question": "Who is the mayor of the capital city of Australia?",
 "triplets": [
    {
        "head_entity": "Australia",
        "relationship": "has prime minister",
        "tail_entity": "Scott Morrison"
    },
    {
        "head_entity": "Canberra",
        "relationship": "is capital of",
        "tail_entity": "Australia"
    },
    {
        "head_entity": "Scott Knowles",
        "relationship": "holds position",
        "tail_entity": "Mayor"
    },
    {
        "head_entity": "Barbera Smith",
        "relationship": "tiene cargo",
        "tail_entity": "Alcalde"
    },
    {
        "head_entity": "Austria",
        "relationship": "has capital",
        "tail_entity": "Vienna"
    }
 ]
}

**Output**

{
    "triplets": [
        {
            "head_entity": "Australia",
            "relationship": "has prime minister",
            "tail_entity": "Scott Morrison",
            "score": 4
        },
        {
            "head_entity": "Canberra",
            "relationship": "is capital of",
            "tail_entity": "Australia",
            "score": 8
        },
        {
            "head_entity": "Scott Knowles",
            "relationship": "holds position",
            "tail_entity": "Mayor",
            "score": 8
        },
        {
            "head_entity": "Barbera Smith",
            "relationship": "tiene cargo",
            "tail_entity": "Alcalde",
            "score": 8
        },
        {
            "head_entity": "Austria",
            "relationship": "has capital",
            "tail_entity": "Vienna",
            "score": 0
        }
    ]
}



### Example 2:

**Input**

{
    "question": "How many products does John Adams Roofing Inc. offer?",
    "triplets": [
        {
            "head_entity": "John Adams Roofing Inc.",
            "relationship": "has product",
            "tail_entity": "Titanium Grade 3 Roofing Nails"
        },
        {
            "head_entity": "John Adams Roofing Inc.",
            "relationship": "is located in",
            "tail_entity": "New York"
        },
        {
            "head_entity": "John Adams Roofing Inc.",
            "relationship": "was founded by",
            "tail_entity": "John Adams"
        },
        {
            "head_entity": "John Adams Roofing Inc.",
            "relationship": "tiene stock",
            "tail_entity": "Baldosas solares"
        },
        {
            "head_entity": "John Adams Roofing Inc.",
            "relationship": "has product",
            "tail_entity": "Mercerized Cotton Thread"
        }
    ]
}

**Output**

{
    "triplets": [
        {
            "head_entity": "John Adams Roofing Inc.",
            "relationship": "has product",
            "tail_entity": "Titanium Grade 3 Roofing Nails",
            "score": 10
        },
        {
            "head_entity": "John Adams Roofing Inc.",
            "relationship": "is located in",
            "tail_entity": "New York",
            "score": 6
        },
        {
            "head_entity": "John Adams Roofing Inc.",
            "relationship": "was founded by",
            "tail_entity": "John Adams",
            "score": 5
        },
        {
            "head_entity": "John Adams Roofing Inc.",
            "relationship": "tiene stock",
            "tail_entity": "Baldosas solares",
            "score": 10
        },
        {
            "head_entity": "John Adams Roofing Inc.",
            "relationship": "has product",
            "tail_entity": "Mercerized Cotton Thread",
            "score": 10
        }
    ]
}

Now, let's get started! Here are the triplets you need to score:

**Input**

"""


async def get_graph_results(
    *,
    kbid: str,
    query: str,
    item: AskRequest,
    ndb_client: NucliaDBClientType,
    user: str,
    origin: str,
    graph_strategy: GraphStrategy,
    generative_model: Optional[str] = None,
    metrics: RAGMetrics = RAGMetrics(),
    shards: Optional[list[str]] = None,
) -> tuple[KnowledgeboxFindResults, QueryParser]:
    relations = Relations(entities={})
    explored_entities: set[str] = set()

    for hop in range(graph_strategy.hops):
        entities_to_explore: Iterable[RelationNode] = []
        if hop == 0:
            # Get the entities from the query
            with metrics.time("graph_strat_query_entities"):
                suggest_result = await fuzzy_search_entities(
                    kbid=kbid,
                    query=query,
                    show=[],
                    field_type_filter=item.field_type_filter,
                    range_creation_start=item.range_creation_start,
                    range_creation_end=item.range_creation_end,
                    range_modification_start=item.range_modification_start,
                    range_modification_end=item.range_modification_end,
                    target_shard_replicas=shards,
                )

            if suggest_result.entities is not None:
                entities_to_explore = (
                    RelationNode(
                        ntype=RelationNode.NodeType.ENTITY, value=result.value, subtype=result.family
                    )
                    for result in suggest_result.entities.entities
                )
            else:
                entities_to_explore = []
        else:
            # Find neighbors of the current relations and remove the ones already explored
            entities_to_explore = (
                RelationNode(
                    ntype=RelationNode.NodeType.ENTITY,
                    value=relation.entity,
                    subtype=relation.entity_subtype,
                )
                for subgraph in relations.entities.values()
                for relation in subgraph.related_to
                if relation.entity not in explored_entities
            )

        # Get the relations for the new entities
        with metrics.time("graph_strat_neighbor_relations"):
            try:
                new_relations = await get_relations_results_from_entities(
                    kbid=kbid,
                    entities=entities_to_explore,
                    target_shard_replicas=shards,
                    timeout=5.0,
                    only_with_metadata=True,
                )
            except Exception as e:
                capture_exception(e)
                logger.exception("Error in getting query relations for graph strategy")
                new_relations = Relations(entities={})

            # Removing the relations connected to the entities that were already explored
            # XXX: This could be optimized by implementing a filter in the index
            # so we don't have to remove them after
            new_subgraphs = {
                entity: filter_subgraph(subgraph, explored_entities)
                for entity, subgraph in new_relations.entities.items()
            }

            if not new_subgraphs or any(not subgraph.related_to for subgraph in new_subgraphs.values()):
                break

            explored_entities.update(new_subgraphs.keys())
            relations.entities.update(new_subgraphs)

        # Rank the relevance of the relations
        # TODO: Add upper bound to the number of entities to explore for safety
        with metrics.time("graph_strat_rank_relations"):
            relations = await rank_relations(
                relations,
                query,
                kbid,
                user,
                top_k=graph_strategy.top_k,
                generative_model=generative_model,
            )

    # Get the text blocks of the paragraphs that contain the top relations
    with metrics.time("graph_strat_build_response"):
        paragraph_ids = {
            r.metadata.paragraph_id
            for rel in relations.entities.values()
            for r in rel.related_to
            if r.metadata and r.metadata.paragraph_id
        }
        find_request = find_request_from_ask_request(item, query)
        query_parser, rank_fusion, reranker = await query_parser_from_find_request(
            kbid, find_request, generative_model=generative_model
        )
        find_results = await build_graph_response(
            paragraph_ids,
            kbid=kbid,
            query=query,
            final_relations=relations,
            top_k=graph_strategy.top_k,
            reranker=reranker,
            show=find_request.show,
            extracted=find_request.extracted,
            field_type_filter=find_request.field_type_filter,
        )

    return find_results, query_parser


async def fuzzy_search_entities(
    kbid: str,
    query: str,
    show: list[ResourceProperties],
    field_type_filter: list[FieldTypeName],
    range_creation_start: Optional[datetime] = None,
    range_creation_end: Optional[datetime] = None,
    range_modification_start: Optional[datetime] = None,
    range_modification_end: Optional[datetime] = None,
    target_shard_replicas: Optional[list[str]] = None,
) -> KnowledgeboxSuggestResults:
    """Fuzzy find entities in KB given a query using the same methodology as /suggest, but split by words."""

    base_request = nodereader_pb2.SuggestRequest(
        body="", features=[nodereader_pb2.SuggestFeatures.ENTITIES]
    )
    if range_creation_start is not None:
        base_request.timestamps.from_created.FromDatetime(range_creation_start)
    if range_creation_end is not None:
        base_request.timestamps.to_created.FromDatetime(range_creation_end)
    if range_modification_start is not None:
        base_request.timestamps.from_modified.FromDatetime(range_modification_start)
    if range_modification_end is not None:
        base_request.timestamps.to_modified.FromDatetime(range_modification_end)

    tasks = []
    # XXX: Splitting by words is not ideal, in the future, modify suggest to better handle this
    for word in query.split():
        if len(word) <= 3:
            continue
        request = nodereader_pb2.SuggestRequest()
        request.CopyFrom(base_request)
        request.body = word
        tasks.append(
            node_query(kbid, Method.SUGGEST, request, target_shard_replicas=target_shard_replicas)
        )

    # Gather
    # TODO: What do I do with `incomplete_results`?
    try:
        results_raw = await asyncio.gather(*tasks)
        return await merge_suggest_results(
            [item for r in results_raw for item in r[0]],
            kbid=kbid,
            show=show,
            field_type_filter=field_type_filter,
        )
    except Exception as e:
        capture_exception(e)
        logger.exception("Error in finding entities in query for graph strategy")
        return KnowledgeboxSuggestResults(entities=None)


async def rank_relations(
    relations: Relations,
    query: str,
    kbid: str,
    user: str,
    top_k: int,
    generative_model: Optional[str] = None,
    score_threshold: int = 0,
) -> Relations:
    # Store the index for keeping track after scoring
    flat_rels: list[tuple[str, int, DirectionalRelation]] = [
        (ent, idx, rel)
        for (ent, rels) in relations.entities.items()
        for (idx, rel) in enumerate(rels.related_to)
    ]
    triplets: list[dict[str, str]] = [
        {
            "head_entity": ent,
            "relationship": rel.relation_label,
            "tail_entity": rel.entity,
        }
        if rel.direction == RelationDirection.OUT
        else {
            "head_entity": rel.entity,
            "relationship": rel.relation_label,
            "tail_entity": ent,
        }
        for (ent, _, rel) in flat_rels
    ]
    # Dedupe triplets so that they get evaluated once, we will re-associate the scores later
    triplet_to_orig_indices: dict[tuple[str, str, str], list[int]] = {}
    unique_triplets = []

    for i, t in enumerate(triplets):
        key = (t["head_entity"], t["relationship"], t["tail_entity"])
        if key not in triplet_to_orig_indices:
            triplet_to_orig_indices[key] = []
            unique_triplets.append(t)
        triplet_to_orig_indices[key].append(i)

    data = {
        "question": query,
        "triplets": unique_triplets,
    }
    prompt = PROMPT + json.dumps(data, indent=4)

    predict = get_predict()
    chat_model = ChatModel(
        question=prompt,
        user_id=user,
        json_schema=SCHEMA,
        format_prompt=False,  # We supply our own prompt
        query_context_order={},
        query_context={},
        user_prompt=UserPrompt(prompt=prompt),
        max_tokens=4096,
        generative_model=generative_model,
    )
    # TODO: Enclose this in a try-except block
    ident, model, answer_stream = await predict.chat_query_ndjson(kbid, chat_model)
    response_json = None
    status = None
    _ = None

    async for generative_chunk in answer_stream:
        item = generative_chunk.chunk
        if isinstance(item, JSONGenerativeResponse):
            response_json = item
        elif isinstance(item, StatusGenerativeResponse):
            status = item
        elif isinstance(item, MetaGenerativeResponse):
            _ = item
        else:
            # TODO: Improve for logging
            raise ValueError(f"Unknown generative chunk type: {item}")

    # TODO: Report tokens using meta?

    if response_json is None or status is None or status.code != "0":
        raise ValueError("No JSON response found")

    scored_unique_triplets: list[dict[str, Union[str, Any]]] = response_json.object["triplets"]

    if len(scored_unique_triplets) != len(unique_triplets):
        raise ValueError("Mismatch between input and output triplets")

    # Re-expand model scores to the original triplets
    scored_triplets: list[Optional[dict[str, Any]]] = [None] * len(triplets)
    for unique_idx, scored_t in enumerate(scored_unique_triplets):
        h, r, ta = (
            scored_t["head_entity"],
            scored_t["relationship"],
            scored_t["tail_entity"],
        )
        for orig_idx in triplet_to_orig_indices[(h, r, ta)]:
            scored_triplets[orig_idx] = scored_t

    if any(st is None for st in scored_triplets):
        raise ValueError("Some triplets did not get a score assigned")

    if len(scored_triplets) != len(flat_rels):
        raise ValueError("Mismatch between input and output triplets after expansion")

    scores = ((idx, t["score"]) for (idx, t) in enumerate(scored_triplets) if t is not None)

    top_k_scores = heapq.nlargest(top_k, scores, key=lambda x: x[1])
    top_k_rels: dict[str, EntitySubgraph] = defaultdict(lambda: EntitySubgraph(related_to=[]))
    for idx_flat, score in top_k_scores:
        (ent, idx, _) = flat_rels[idx_flat]
        rel = relations.entities[ent].related_to[idx]
        if score > score_threshold:
            top_k_rels[ent].related_to.append(rel)

    return Relations(entities=top_k_rels)


async def build_graph_response(
    paragraph_ids: Iterable[str],
    *,
    kbid: str,
    query: str,
    final_relations: Relations,
    top_k: int,
    reranker: Reranker,
    show: list[ResourceProperties] = [],
    extracted: list[ExtractedDataTypeName] = [],
    field_type_filter: list[FieldTypeName] = [],
) -> KnowledgeboxFindResults:
    # manually generate paragraph results

    text_blocks = paragraph_id_to_text_block_matches(paragraph_ids)

    # hydrate and rerank
    resource_hydration_options = ResourceHydrationOptions(
        show=show, extracted=extracted, field_type_filter=field_type_filter
    )
    text_block_hydration_options = TextBlockHydrationOptions()
    reranking_options = RerankingOptions(kbid=kbid, query=query)
    text_blocks, resources, best_matches = await hydrate_and_rerank(
        text_blocks,
        kbid,
        resource_hydration_options=resource_hydration_options,
        text_block_hydration_options=text_block_hydration_options,
        reranker=reranker,
        reranking_options=reranking_options,
        top_k=top_k,
    )

    find_resources = compose_find_resources(text_blocks, resources)

    return KnowledgeboxFindResults(
        query=query,
        resources=find_resources,
        best_matches=best_matches,
        relations=final_relations,
        total=len(text_blocks),
    )


def filter_subgraph(subgraph: EntitySubgraph, entities_to_remove: Collection[str]) -> EntitySubgraph:
    """
    Removes the relationships with entities in `entities_to_remove` from the subgraph.
    """
    return EntitySubgraph(
        # TODO: Limit to 150 is temporary, remove it and add a reranker scoring?
        related_to=[rel for rel in subgraph.related_to if rel.entity not in entities_to_remove][:150]
    )
