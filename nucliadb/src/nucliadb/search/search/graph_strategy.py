import heapq
import json
from collections import defaultdict
from typing import Iterable, Optional

from nuclia_models.predict.generative_responses import (
    JSONGenerativeResponse,
    MetaGenerativeResponse,
    StatusGenerativeResponse,
)

from nucliadb.search.search.find_merge import (
    compose_find_resources,
    hydrate_and_rerank,
    paragraph_id_to_text_block_matches,
)
from nucliadb.search.search.hydrator import ResourceHydrationOptions, TextBlockHydrationOptions
from nucliadb.search.search.rerankers import Reranker, RerankingOptions
from nucliadb.search.utilities import get_predict
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import ExtractedDataTypeName
from nucliadb_models.search import (
    ChatModel,
    DirectionalRelation,
    EntitySubgraph,
    KnowledgeboxFindResults,
    RelationDirection,
    Relations,
    ResourceProperties,
    UserPrompt,
)

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


async def rank_relations(
    relations: Relations,
    query: str,
    kbid: str,
    user: str,
    top_k: int,
    generative_model: Optional[str] = None,
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
    data = {
        "question": query,
        "triplets": triplets,
    }
    prompt = PROMPT + json.dumps(data, indent=4)

    predict = get_predict()
    item = ChatModel(
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
    ident, model, answer_stream = await predict.chat_query_ndjson(kbid, item)
    response_json = None
    status = None
    meta = None

    async for generative_chunk in answer_stream:
        item = generative_chunk.chunk
        if isinstance(item, JSONGenerativeResponse):
            response_json = item
        elif isinstance(item, StatusGenerativeResponse):
            status = item
        elif isinstance(item, MetaGenerativeResponse):
            meta = item
        else:
            # TODO: Improve for logging
            raise ValueError(f"Unknown generative chunk type: {item}")

    # TODO: Report tokens using meta?

    if response_json is None or status is None or status.code != "0":
        raise ValueError("No JSON response found")

    scored_triplets = response_json.object["triplets"]

    if len(scored_triplets) != len(flat_rels):
        raise ValueError("Mismatch between input and output triplets")
    scores = ((idx, scored_triplet["score"]) for (idx, scored_triplet) in enumerate(scored_triplets))
    top_k_scores = heapq.nlargest(top_k, scores, key=lambda x: x[1])
    top_k_rels = defaultdict(lambda: EntitySubgraph(related_to=[]))
    for idx_flat, _ in top_k_scores:
        (ent, idx, _) = flat_rels[idx_flat]
        rel = relations.entities[ent].related_to[idx]
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
