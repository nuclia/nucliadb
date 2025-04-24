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
import heapq
import json
from collections import defaultdict
from typing import Any, Collection, Iterable, Optional, Union

from nidx_protos import nodereader_pb2
from nuclia_models.predict.generative_responses import (
    JSONGenerativeResponse,
    MetaGenerativeResponse,
    StatusGenerativeResponse,
)
from pydantic import BaseModel
from sentry_sdk import capture_exception

from nucliadb.common.external_index_providers.base import TextBlockMatch
from nucliadb.common.ids import FieldId, ParagraphId
from nucliadb.search import logger
from nucliadb.search.requesters.utils import Method, node_query
from nucliadb.search.search.chat.query import (
    find_request_from_ask_request,
    get_relations_results_from_entities,
)
from nucliadb.search.search.find_merge import (
    compose_find_resources,
    hydrate_and_rerank,
)
from nucliadb.search.search.hydrator import ResourceHydrationOptions, TextBlockHydrationOptions
from nucliadb.search.search.metrics import RAGMetrics
from nucliadb.search.search.rerankers import (
    Reranker,
    RerankingOptions,
)
from nucliadb.search.utilities import get_predict
from nucliadb_models.common import FieldTypeName
from nucliadb_models.internal.predict import (
    RerankModel,
)
from nucliadb_models.resource import ExtractedDataTypeName
from nucliadb_models.search import (
    SCORE_TYPE,
    AskRequest,
    ChatModel,
    DirectionalRelation,
    EntitySubgraph,
    FindRequest,
    GraphStrategy,
    KnowledgeboxFindResults,
    NucliaDBClientType,
    QueryEntityDetection,
    RelatedEntities,
    RelatedEntity,
    RelationDirection,
    RelationRanking,
    Relations,
    ResourceProperties,
    TextPosition,
    UserPrompt,
)
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


class RelationsParagraphMatch(BaseModel):
    paragraph_id: ParagraphId
    score: float
    relations: Relations


async def get_graph_results(
    *,
    kbid: str,
    query: str,
    item: AskRequest,
    ndb_client: NucliaDBClientType,
    user: str,
    origin: str,
    graph_strategy: GraphStrategy,
    text_block_reranker: Reranker,
    generative_model: Optional[str] = None,
    metrics: RAGMetrics = RAGMetrics(),
    shards: Optional[list[str]] = None,
) -> tuple[KnowledgeboxFindResults, FindRequest]:
    relations = Relations(entities={})
    explored_entities: set[str] = set()
    scores: dict[str, list[float]] = {}
    predict = get_predict()

    for hop in range(graph_strategy.hops):
        entities_to_explore: Iterable[RelationNode] = []

        if hop == 0:
            # Get the entities from the query
            with metrics.time("graph_strat_query_entities"):
                if graph_strategy.query_entity_detection == QueryEntityDetection.SUGGEST:
                    relation_result = await fuzzy_search_entities(
                        kbid=kbid,
                        query=query,
                    )
                    if relation_result is not None:
                        entities_to_explore = (
                            RelationNode(
                                ntype=RelationNode.NodeType.ENTITY,
                                value=result.value,
                                subtype=result.family,
                            )
                            for result in relation_result.entities
                        )
                elif (
                    not entities_to_explore
                    or graph_strategy.query_entity_detection == QueryEntityDetection.PREDICT
                ):
                    try:
                        # Purposely ignore the entity subtype. This is done so we find all entities that match
                        # the entity by name. e.g: in a query like "2000", predict might detect the number as
                        # a year entity or as a currency entity. We want graph results for both, so we ignore the
                        # subtype just in this case.
                        entities_to_explore = [
                            RelationNode(ntype=r.ntype, value=r.value, subtype="")
                            for r in await predict.detect_entities(kbid, query)
                        ]
                    except Exception as e:
                        capture_exception(e)
                        logger.exception("Error in detecting entities for graph strategy")
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
                    timeout=5.0,
                    only_with_metadata=not graph_strategy.relation_text_as_paragraphs,
                    only_agentic_relations=graph_strategy.agentic_graph_only,
                    # We only want entity to entity relations (skip resource/labels/collaborators/etc.)
                    only_entity_to_entity=True,
                    deleted_entities=explored_entities,
                )
            except Exception as e:
                capture_exception(e)
                logger.exception("Error in getting query relations for graph strategy")
                new_relations = Relations(entities={})

            new_subgraphs = new_relations.entities

            explored_entities.update(new_subgraphs.keys())

            if not new_subgraphs or all(not subgraph.related_to for subgraph in new_subgraphs.values()):
                break

            relations.entities.update(new_subgraphs)

        # Rank the relevance of the relations
        with metrics.time("graph_strat_rank_relations"):
            try:
                if graph_strategy.relation_ranking == RelationRanking.RERANKER:
                    relations, scores = await rank_relations_reranker(
                        relations,
                        query,
                        kbid,
                        user,
                        top_k=graph_strategy.top_k,
                    )
                elif graph_strategy.relation_ranking == RelationRanking.GENERATIVE:
                    relations, scores = await rank_relations_generative(
                        relations,
                        query,
                        kbid,
                        user,
                        top_k=graph_strategy.top_k,
                        generative_model=generative_model,
                    )
            except Exception as e:
                capture_exception(e)
                logger.exception("Error in ranking relations for graph strategy")
                relations = Relations(entities={})
                scores = {}
                break

    # Get the text blocks of the paragraphs that contain the top relations
    with metrics.time("graph_strat_build_response"):
        find_request = find_request_from_ask_request(item, query)
        find_results = await build_graph_response(
            kbid=kbid,
            query=query,
            final_relations=relations,
            scores=scores,
            top_k=graph_strategy.top_k,
            reranker=text_block_reranker,
            show=item.show,
            extracted=item.extracted,
            field_type_filter=item.field_type_filter,
            relation_text_as_paragraphs=graph_strategy.relation_text_as_paragraphs,
        )
    return find_results, find_request


async def fuzzy_search_entities(
    kbid: str,
    query: str,
) -> Optional[RelatedEntities]:
    """Fuzzy find entities in KB given a query using the same methodology as /suggest, but split by words."""

    # Build an OR for each word in the query matching with fuzzy any word in any
    # node in any position. I.e., for the query "Rose Hamiltn", it'll match
    # "Rosa Parks" and "Margaret Hamilton"
    request = nodereader_pb2.GraphSearchRequest()
    # XXX Are those enough results? Too many?
    request.top_k = 50
    request.kind = nodereader_pb2.GraphSearchRequest.QueryKind.NODES
    for word in query.split():
        subquery = nodereader_pb2.GraphQuery.PathQuery()
        subquery.path.source.value = word
        subquery.path.source.fuzzy.kind = nodereader_pb2.GraphQuery.Node.MatchLocation.WORDS
        subquery.path.source.fuzzy.distance = 1
        subquery.path.undirected = True
        request.query.path.bool_or.operands.append(subquery)

    try:
        results, _, _ = await node_query(kbid, Method.GRAPH, request)
    except Exception as exc:
        capture_exception(exc)
        logger.exception("Error in finding entities in query for graph strategy")
        return None

    # merge shard results while deduplicating repeated entities across shards
    unique_entities: set[RelatedEntity] = set()
    for response in results:
        unique_entities.update((RelatedEntity(family=e.subtype, value=e.value) for e in response.nodes))

    return RelatedEntities(entities=list(unique_entities), total=len(unique_entities))


async def rank_relations_reranker(
    relations: Relations,
    query: str,
    kbid: str,
    user: str,
    top_k: int,
    score_threshold: float = 0.02,
) -> tuple[Relations, dict[str, list[float]]]:
    # Store the index for keeping track after scoring
    flat_rels: list[tuple[str, int, DirectionalRelation]] = [
        (ent, idx, rel)
        for (ent, rels) in relations.entities.items()
        for (idx, rel) in enumerate(rels.related_to)
    ]
    # Build triplets (dict) from each relation for use in reranker
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

    # Dedupe triplets so that they get evaluated once; map triplet -> [orig_indices]
    triplet_to_orig_indices: dict[tuple[str, str, str], list[int]] = {}
    unique_triplets: list[dict[str, str]] = []

    for i, t in enumerate(triplets):
        key = (t["head_entity"], t["relationship"], t["tail_entity"])
        if key not in triplet_to_orig_indices:
            triplet_to_orig_indices[key] = []
            unique_triplets.append(t)
        triplet_to_orig_indices[key].append(i)

    # Build the reranker model input
    predict = get_predict()
    rerank_model = RerankModel(
        question=query,
        user_id=user,
        context={
            str(idx): f"{t['head_entity']} {t['relationship']} {t['tail_entity']}"
            for idx, t in enumerate(unique_triplets)
        },
    )
    # Get the rerank scores
    res = await predict.rerank(kbid, rerank_model)

    # Convert returned scores to a list of (int_idx, score)
    # where int_idx corresponds to indices in unique_triplets
    reranked_indices_scores = [(int(idx), score) for idx, score in res.context_scores.items()]

    return _scores_to_ranked_rels(
        unique_triplets,
        reranked_indices_scores,
        triplet_to_orig_indices,
        flat_rels,
        top_k,
        score_threshold,
    )


async def rank_relations_generative(
    relations: Relations,
    query: str,
    kbid: str,
    user: str,
    top_k: int,
    generative_model: Optional[str] = None,
    score_threshold: float = 2,
    max_rels_to_eval: int = 100,
) -> tuple[Relations, dict[str, list[float]]]:
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
    unique_triplets: list[dict[str, str]] = []

    for i, t in enumerate(triplets):
        key = (t["head_entity"], t["relationship"], t["tail_entity"])
        if key not in triplet_to_orig_indices:
            triplet_to_orig_indices[key] = []
            unique_triplets.append(t)
        triplet_to_orig_indices[key].append(i)

    if len(flat_rels) > max_rels_to_eval:
        logger.warning(f"Too many relations to evaluate ({len(flat_rels)}), using reranker to reduce")
        return await rank_relations_reranker(relations, query, kbid, user, top_k=max_rels_to_eval)

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
            raise ValueError(f"Unknown generative chunk type: {item}")

    if response_json is None or status is None or status.code != "0":
        raise ValueError("No JSON response found")

    scored_unique_triplets: list[dict[str, Union[str, Any]]] = response_json.object["triplets"]

    if len(scored_unique_triplets) != len(unique_triplets):
        raise ValueError("Mismatch between input and output triplets")

    unique_indices_scores = ((idx, float(t["score"])) for (idx, t) in enumerate(scored_unique_triplets))

    return _scores_to_ranked_rels(
        unique_triplets,
        unique_indices_scores,
        triplet_to_orig_indices,
        flat_rels,
        top_k,
        score_threshold,
    )


def _scores_to_ranked_rels(
    unique_triplets: list[dict[str, str]],
    unique_indices_scores: Iterable[tuple[int, float]],
    triplet_to_orig_indices: dict[tuple[str, str, str], list[int]],
    flat_rels: list[tuple[str, int, DirectionalRelation]],
    top_k: int,
    score_threshold: float,
) -> tuple[Relations, dict[str, list[float]]]:
    """
    Helper function to convert unique scores assigned by a model back to the original relations while taking
    care of threshold
    """
    top_k_indices_scores = heapq.nlargest(top_k, unique_indices_scores, key=lambda x: x[1])

    # Prepare a new Relations object + a dict of top scores by entity
    top_k_rels: dict[str, EntitySubgraph] = defaultdict(lambda: EntitySubgraph(related_to=[]))
    top_k_scores_by_ent: dict[str, list[float]] = defaultdict(list)
    # Re-expand model scores to the original triplets
    for idx, score in top_k_indices_scores:
        # If the model's score is below threshold, skip
        if score <= score_threshold:
            continue

        # Identify which original triplets (in flat_rels) this corresponds to
        t = unique_triplets[idx]
        key = (t["head_entity"], t["relationship"], t["tail_entity"])
        orig_indices = triplet_to_orig_indices[key]

        for orig_i in orig_indices:
            ent, rel_idx, rel = flat_rels[orig_i]
            # Insert the relation into top_k_rels
            top_k_rels[ent].related_to.append(rel)

            # Keep track of which indices were chosen per entity
            top_k_scores_by_ent[ent].append(score)

    return Relations(entities=top_k_rels), dict(top_k_scores_by_ent)


def build_text_blocks_from_relations(
    relations: Relations,
    scores: dict[str, list[float]],
) -> list[TextBlockMatch]:
    """
    The goal of this function is to generate  TextBlockMatch with custom text for each unique relation in the graph.

    This is a hacky way to generate paragraphs from relations, and it is not the intended use of TextBlockMatch.
    """
    # Build a set of unique triplets with their scores
    triplets: dict[tuple[str, str, str], tuple[float, Relations, Optional[ParagraphId]]] = defaultdict(
        lambda: (0.0, Relations(entities={}), None)
    )
    paragraph_count = 0
    for ent, subgraph in relations.entities.items():
        for rel, score in zip(subgraph.related_to, scores[ent]):
            key = (
                (
                    ent,
                    rel.relation_label,
                    rel.entity,
                )
                if rel.direction == RelationDirection.OUT
                else (rel.entity, rel.relation_label, ent)
            )
            existing_score, existing_relations, p_id = triplets[key]
            if ent not in existing_relations.entities:
                existing_relations.entities[ent] = EntitySubgraph(related_to=[])

            # XXX: Since relations with the same triplet can point to different paragraphs,
            # we keep the first one, but we lose the other ones
            if p_id is None and rel.metadata and rel.metadata.paragraph_id:
                p_id = ParagraphId.from_string(rel.metadata.paragraph_id)
            else:
                # No paragraph ID set, fake it so we can hydrate the resource
                p_id = ParagraphId(
                    field_id=FieldId(rel.resource_id, "a", "usermetadata"),
                    paragraph_start=paragraph_count,
                    paragraph_end=paragraph_count + 1,
                )
                paragraph_count += 1
            existing_relations.entities[ent].related_to.append(rel)
            # XXX: Here we use the max even though all relations with same triplet should have same score
            triplets[key] = (max(existing_score, score), existing_relations, p_id)

    # Build the text blocks
    text_blocks = [
        TextBlockMatch(
            # XXX: Even though we are setting a paragraph_id, the text is not coming from the paragraph
            paragraph_id=p_id,
            score=score,
            score_type=SCORE_TYPE.RELATION_RELEVANCE,
            order=0,
            text=f"- {ent} {rel} {tail}",  # Manually build the text
            position=TextPosition(
                page_number=0,
                index=0,
                start=0,
                end=0,
                start_seconds=[],
                end_seconds=[],
            ),
            field_labels=[],
            paragraph_labels=[],
            fuzzy_search=False,
            is_a_table=False,
            representation_file="",
            page_with_visual=False,
            relevant_relations=relations,
        )
        for (ent, rel, tail), (score, relations, p_id) in triplets.items()
        if p_id is not None
    ]
    return text_blocks


def get_paragraph_info_from_relations(
    relations: Relations,
    scores: dict[str, list[float]],
) -> list[RelationsParagraphMatch]:
    """
    Gathers paragraph info from the 'relations' object, merges relations by paragraph,
    and removes paragraphs contained entirely within others.
    """

    # Group paragraphs by field so we can detect containment
    paragraphs_by_field: dict[FieldId, list[RelationsParagraphMatch]] = defaultdict(list)

    # Loop over each entity in the relation graph
    for ent, subgraph in relations.entities.items():
        for rel_score, rel in zip(scores[ent], subgraph.related_to):
            if rel.metadata and rel.metadata.paragraph_id:
                p_id = ParagraphId.from_string(rel.metadata.paragraph_id)
                match = RelationsParagraphMatch(
                    paragraph_id=p_id,
                    score=rel_score,
                    relations=Relations(entities={ent: EntitySubgraph(related_to=[rel])}),
                )
                paragraphs_by_field[p_id.field_id].append(match)

    # For each field, sort paragraphs by start asc, end desc, and do one pass to remove contained ones
    final_paragraphs: list[RelationsParagraphMatch] = []

    for _, paragraph_list in paragraphs_by_field.items():
        # Sort by paragraph_start ascending; if tie, paragraph_end descending
        paragraph_list.sort(
            key=lambda m: (m.paragraph_id.paragraph_start, -m.paragraph_id.paragraph_end)
        )

        kept: list[RelationsParagraphMatch] = []
        current_max_end = -1

        for match in paragraph_list:
            end = match.paragraph_id.paragraph_end

            # If end <= current_max_end, this paragraph is contained last one
            if end <= current_max_end:
                # We merge the scores and relations
                container = kept[-1]
                container.score = max(container.score, match.score)
                for ent, subgraph in match.relations.entities.items():
                    if ent not in container.relations.entities:
                        container.relations.entities[ent] = EntitySubgraph(related_to=[])
                    container.relations.entities[ent].related_to.extend(subgraph.related_to)

            else:
                # Not contained; keep it
                kept.append(match)
                current_max_end = end
        final_paragraphs.extend(kept)

    return final_paragraphs


async def build_graph_response(
    *,
    kbid: str,
    query: str,
    final_relations: Relations,
    scores: dict[str, list[float]],
    top_k: int,
    reranker: Reranker,
    relation_text_as_paragraphs: bool,
    show: list[ResourceProperties] = [],
    extracted: list[ExtractedDataTypeName] = [],
    field_type_filter: list[FieldTypeName] = [],
) -> KnowledgeboxFindResults:
    if relation_text_as_paragraphs:
        text_blocks = build_text_blocks_from_relations(final_relations, scores)
    else:
        paragraphs_info = get_paragraph_info_from_relations(final_relations, scores)
        text_blocks = relations_matches_to_text_block_matches(paragraphs_info)

    # hydrate and rerank
    resource_hydration_options = ResourceHydrationOptions(
        show=show, extracted=extracted, field_type_filter=field_type_filter
    )
    text_block_hydration_options = TextBlockHydrationOptions(only_hydrate_empty=True)
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


def relations_match_to_text_block_match(
    paragraph_match: RelationsParagraphMatch,
) -> TextBlockMatch:
    """
    Given a paragraph_id, return a TextBlockMatch with the bare minimum fields
    This is required by the Graph Strategy to get text blocks from the relevant paragraphs
    """
    # XXX: this is a workaround for the fact we always assume retrieval means keyword/semantic search and
    # the hydration and find response building code works with TextBlockMatch, we extended it to have relevant relations information
    parsed_paragraph_id = paragraph_match.paragraph_id
    return TextBlockMatch(
        paragraph_id=parsed_paragraph_id,
        score=paragraph_match.score,
        score_type=SCORE_TYPE.RELATION_RELEVANCE,
        order=0,  # NOTE: this will be filled later
        text="",  # NOTE: this will be filled later too
        position=TextPosition(
            page_number=0,
            index=0,
            start=parsed_paragraph_id.paragraph_start,
            end=parsed_paragraph_id.paragraph_end,
            start_seconds=[],
            end_seconds=[],
        ),
        field_labels=[],
        paragraph_labels=[],
        fuzzy_search=False,
        is_a_table=False,
        representation_file="",
        page_with_visual=False,
        relevant_relations=paragraph_match.relations,
    )


def relations_matches_to_text_block_matches(
    paragraph_matches: Collection[RelationsParagraphMatch],
) -> list[TextBlockMatch]:
    return [relations_match_to_text_block_match(match) for match in paragraph_matches]
