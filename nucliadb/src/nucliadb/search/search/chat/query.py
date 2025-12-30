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
import asyncio
from collections.abc import AsyncGenerator, Iterable
from time import time

from nidx_protos.nodereader_pb2 import GraphSearchResponse, SearchResponse
from nuclia_models.predict.generative_responses import GenerativeChunk

from nucliadb.common.external_index_providers.base import TextBlockMatch
from nucliadb.common.ids import ParagraphId
from nucliadb.common.models_utils import to_proto
from nucliadb.search import logger
from nucliadb.search.predict import AnswerStatusCode, RephraseResponse
from nucliadb.search.requesters.utils import Method, nidx_query
from nucliadb.search.search.chat import rpc
from nucliadb.search.search.chat.exceptions import NoRetrievalResultsError
from nucliadb.search.search.chat.parser import rao_parse_find
from nucliadb.search.search.exceptions import IncompleteFindResultsError
from nucliadb.search.search.find import find
from nucliadb.search.search.find_merge import text_block_to_find_paragraph
from nucliadb.search.search.hydrator import ResourceHydrationOptions, TextBlockHydrationOptions
from nucliadb.search.search.merge import merge_relations_results
from nucliadb.search.search.metrics import Metrics
from nucliadb.search.search.paragraphs import highlight_paragraph
from nucliadb.search.search.query_parser.fetcher import Fetcher
from nucliadb.search.search.query_parser.models import Query, RelationQuery, UnitRetrieval
from nucliadb.search.search.query_parser.parsers.unit_retrieval import convert_retrieval_to_proto
from nucliadb.search.search.rerankers import RerankableItem, Reranker, RerankingOptions, get_reranker
from nucliadb.search.settings import settings
from nucliadb.search.utilities import get_predict
from nucliadb_models import filters
from nucliadb_models.augment import (
    AugmentedResource,
    AugmentParagraph,
    AugmentParagraphs,
    AugmentRequest,
    AugmentResources,
    ParagraphMetadata,
    ResourceProp,
)
from nucliadb_models.retrieval import RerankerScore, RetrievalMatch, ScoreType
from nucliadb_models.search import (
    SCORE_TYPE,
    AskRequest,
    ChatContextMessage,
    ChatModel,
    ChatOptions,
    FindField,
    FindOptions,
    FindRequest,
    FindResource,
    KnowledgeboxFindResults,
    NucliaDBClientType,
    PreQueriesStrategy,
    PreQuery,
    PreQueryResult,
    PromptContext,
    PromptContextOrder,
    Relations,
    RephraseModel,
    TextPosition,
    parse_rephrase_prompt,
)
from nucliadb_protos import audit_pb2
from nucliadb_protos.utils_pb2 import RelationNode
from nucliadb_telemetry.errors import capture_exception
from nucliadb_utils import const
from nucliadb_utils.utilities import get_audit, has_feature

NOT_ENOUGH_CONTEXT_ANSWER = "Not enough data to answer this."


async def rephrase_query(
    kbid: str,
    chat_history: list[ChatContextMessage],
    query: str,
    user_id: str,
    user_context: list[str],
    generative_model: str | None = None,
    chat_history_relevance_threshold: float | None = None,
) -> RephraseResponse:
    # NOTE: When moving /ask to RAO, this will need to change to whatever client/utility is used
    # to call NUA predict (internally or externally in the case of onprem).
    predict = get_predict()
    req = RephraseModel(
        question=query,
        chat_history=chat_history,
        user_id=user_id,
        user_context=user_context,
        generative_model=generative_model,
        chat_history_relevance_threshold=chat_history_relevance_threshold,
    )
    return await predict.rephrase_query(kbid, req)


async def get_find_results(
    *,
    kbid: str,
    query: str,
    item: AskRequest,
    ndb_client: NucliaDBClientType,
    user: str,
    origin: str,
    metrics: Metrics,
    prequeries_strategy: PreQueriesStrategy | None = None,
) -> tuple[KnowledgeboxFindResults, list[PreQueryResult] | None, Fetcher, Reranker]:
    prequeries_results = None
    prefilter_queries_results = None
    queries_results = None
    if prequeries_strategy is not None:
        prefilters = [prequery for prequery in prequeries_strategy.queries if prequery.prefilter]
        prequeries = [prequery for prequery in prequeries_strategy.queries if not prequery.prefilter]
        if len(prefilters) > 0:
            with metrics.time("prefilters"):
                prefilter_queries_results = await run_prequeries(
                    kbid,
                    prefilters,
                    x_ndb_client=ndb_client,
                    x_nucliadb_user=user,
                    x_forwarded_for=origin,
                    metrics=metrics.child_span("prefilters"),
                )
                prefilter_matching_resources = {
                    resource
                    for _, find_results in prefilter_queries_results
                    for resource in find_results.resources.keys()
                }
                if len(prefilter_matching_resources) == 0:
                    raise NoRetrievalResultsError()
                # Make sure the main query and prequeries use the same resource filters.
                # This is important to avoid returning results that don't match the prefilter.
                matching_resources = list(prefilter_matching_resources)
                add_resource_filter(item, matching_resources)
                for prequery in prequeries:
                    add_resource_filter(prequery.request, matching_resources)
                    prequery.request.show_hidden = item.show_hidden

        if prequeries:
            with metrics.time("prequeries"):
                queries_results = await run_prequeries(
                    kbid,
                    prequeries,
                    x_ndb_client=ndb_client,
                    x_nucliadb_user=user,
                    x_forwarded_for=origin,
                    metrics=metrics.child_span("prequeries"),
                )

        prequeries_results = (prefilter_queries_results or []) + (queries_results or [])

    with metrics.time("main_query"):
        main_results, fetcher, reranker = await run_main_query(
            kbid,
            query,
            item,
            ndb_client,
            user,
            origin,
            metrics=metrics.child_span("main_query"),
        )
    return main_results, prequeries_results, fetcher, reranker


def add_resource_filter(request: FindRequest | AskRequest, resources: list[str]):
    if len(resources) == 0:
        return

    if request.filter_expression is not None:
        if len(resources) > 1:
            resource_filter: filters.FieldFilterExpression = filters.Or.model_validate(
                {"or": [filters.Resource(prop="resource", id=rid) for rid in resources]}
            )
        else:
            resource_filter = filters.Resource(prop="resource", id=resources[0])

        # Add to filter expression if set
        if request.filter_expression.field is None:
            request.filter_expression.field = resource_filter
        else:
            request.filter_expression.field = filters.And.model_validate(
                {"and": [request.filter_expression.field, resource_filter]}
            )
    else:
        # Add to old key filters instead
        request.resource_filters = resources


def find_request_from_ask_request(item: AskRequest, query: str) -> FindRequest:
    find_request = FindRequest()
    find_request.filter_expression = item.filter_expression
    find_request.resource_filters = item.resource_filters
    find_request.features = []
    if ChatOptions.SEMANTIC in item.features:
        find_request.features.append(FindOptions.SEMANTIC)
    if ChatOptions.KEYWORD in item.features:
        find_request.features.append(FindOptions.KEYWORD)
    if ChatOptions.RELATIONS in item.features:
        find_request.features.append(FindOptions.RELATIONS)
    find_request.query = query
    find_request.fields = item.fields
    find_request.filters = item.filters
    find_request.field_type_filter = item.field_type_filter
    find_request.min_score = item.min_score
    find_request.vectorset = item.vectorset
    find_request.range_creation_start = item.range_creation_start
    find_request.range_creation_end = item.range_creation_end
    find_request.range_modification_start = item.range_modification_start
    find_request.range_modification_end = item.range_modification_end
    find_request.show = item.show
    find_request.extracted = item.extracted
    find_request.highlight = item.highlight
    find_request.security = item.security
    find_request.debug = item.debug
    find_request.rephrase = item.rephrase
    find_request.rephrase_prompt = parse_rephrase_prompt(item)
    find_request.query_image = item.query_image
    find_request.rank_fusion = item.rank_fusion
    find_request.reranker = item.reranker
    # We don't support pagination, we always get the top_k results.
    find_request.top_k = item.top_k
    find_request.show_hidden = item.show_hidden
    find_request.generative_model = item.generative_model

    # this executes the model validators, that can tweak some fields
    return FindRequest.model_validate(find_request)


async def run_main_query(
    kbid: str,
    query: str,
    item: AskRequest,
    ndb_client: NucliaDBClientType,
    user: str,
    origin: str,
    metrics: Metrics,
) -> tuple[KnowledgeboxFindResults, Fetcher, Reranker]:
    find_request = find_request_from_ask_request(item, query)

    find_results, incomplete, fetcher, reranker = await find_retrieval(
        kbid,
        find_request,
        ndb_client,
        user,
        origin,
        metrics=metrics,
    )
    if incomplete:
        raise IncompleteFindResultsError()
    return find_results, fetcher, reranker


async def get_relations_results(
    *,
    kbid: str,
    text_answer: str,
    timeout: float | None = None,
) -> Relations:
    try:
        predict = get_predict()
        detected_entities = await predict.detect_entities(kbid, text_answer)

        return await get_relations_results_from_entities(
            kbid=kbid,
            entities=detected_entities,
            timeout=timeout,
        )
    except Exception as exc:
        capture_exception(exc)
        logger.exception("Error getting relations results")
        return Relations(entities={})


async def get_relations_results_from_entities(
    *,
    kbid: str,
    entities: Iterable[RelationNode],
    timeout: float | None = None,
    deleted_entities: set[str] = set(),
) -> Relations:
    entry_points = list(entities)
    retrieval = UnitRetrieval(
        query=Query(
            relation=RelationQuery(
                entry_points=entry_points,
                deleted_entities={"": list(deleted_entities)},
                deleted_entity_groups=[],
            )
        ),
        top_k=50,
    )
    request = convert_retrieval_to_proto(retrieval)

    results: list[SearchResponse]
    (
        results,
        _,
    ) = await nidx_query(
        kbid,
        Method.SEARCH,
        request,
        timeout=timeout,
    )
    relations_results: list[GraphSearchResponse] = [result.graph for result in results]
    return await merge_relations_results(
        relations_results,
        entry_points,
    )


def maybe_audit_chat(
    *,
    kbid: str,
    user_id: str,
    client_type: NucliaDBClientType,
    origin: str,
    generative_answer_time: float,
    generative_answer_first_chunk_time: float,
    generative_reasoning_first_chunk_time: float | None,
    rephrase_time: float | None,
    user_query: str,
    rephrased_query: str | None,
    retrieval_rephrase_query: str | None,
    text_answer: bytes,
    text_reasoning: str | None,
    status_code: AnswerStatusCode,
    chat_history: list[ChatContextMessage],
    query_context: PromptContext,
    query_context_order: PromptContextOrder,
    learning_id: str | None,
    model: str | None,
):
    audit = get_audit()
    if audit is None:
        return

    audit_answer = parse_audit_answer(text_answer, status_code)
    # Append chat history
    chat_history_context = [
        audit_pb2.ChatContext(author=message.author, text=message.text) for message in chat_history
    ]

    # Append paragraphs retrieved on this chat
    chat_retrieved_context = [
        audit_pb2.RetrievedContext(text_block_id=paragraph_id, text=text)
        for paragraph_id, text in query_context.items()
    ]

    audit.chat(
        kbid,
        user_id,
        to_proto.client_type(client_type),
        origin,
        question=user_query,
        generative_answer_time=generative_answer_time,
        generative_answer_first_chunk_time=generative_answer_first_chunk_time,
        generative_reasoning_first_chunk_time=generative_reasoning_first_chunk_time,
        rephrase_time=rephrase_time,
        rephrased_question=rephrased_query,
        retrieval_rephrased_question=retrieval_rephrase_query,
        chat_context=chat_history_context,
        retrieved_context=chat_retrieved_context,
        answer=audit_answer,
        reasoning=text_reasoning,
        learning_id=learning_id,
        status_code=int(status_code.value),
        model=model,
    )


def parse_audit_answer(raw_text_answer: bytes, status_code: AnswerStatusCode) -> str | None:
    if status_code == AnswerStatusCode.NO_CONTEXT or status_code == AnswerStatusCode.NO_RETRIEVAL_DATA:
        # We don't want to audit "Not enough context to answer this." and instead set a None.
        return None
    return raw_text_answer.decode()


def tokens_to_chars(n_tokens: int) -> int:
    # Multiply by 3 to have a good margin and guess between characters and tokens.
    # This will be properly cut at the NUA predict API.
    return n_tokens * 3


class ChatAuditor:
    def __init__(
        self,
        kbid: str,
        user_id: str,
        client_type: NucliaDBClientType,
        origin: str,
        user_query: str,
        rephrased_query: str | None,
        retrieval_rephrased_query: str | None,
        chat_history: list[ChatContextMessage],
        learning_id: str | None,
        query_context: PromptContext,
        query_context_order: PromptContextOrder,
        model: str | None,
    ):
        self.kbid = kbid
        self.user_id = user_id
        self.client_type = client_type
        self.origin = origin
        self.user_query = user_query
        self.rephrased_query = rephrased_query
        self.retrieval_rephrased_query = retrieval_rephrased_query
        self.chat_history = chat_history
        self.learning_id = learning_id
        self.query_context = query_context
        self.query_context_order = query_context_order
        self.model = model

    def audit(
        self,
        text_answer: bytes,
        text_reasoning: str | None,
        generative_answer_time: float,
        generative_answer_first_chunk_time: float,
        generative_reasoning_first_chunk_time: float | None,
        rephrase_time: float | None,
        status_code: AnswerStatusCode,
    ):
        maybe_audit_chat(
            kbid=self.kbid,
            user_id=self.user_id,
            client_type=self.client_type,
            origin=self.origin,
            user_query=self.user_query,
            rephrased_query=self.rephrased_query,
            retrieval_rephrase_query=self.retrieval_rephrased_query,
            generative_answer_time=generative_answer_time,
            generative_answer_first_chunk_time=generative_answer_first_chunk_time,
            generative_reasoning_first_chunk_time=generative_reasoning_first_chunk_time,
            rephrase_time=rephrase_time,
            text_answer=text_answer,
            text_reasoning=text_reasoning,
            status_code=status_code,
            chat_history=self.chat_history,
            query_context=self.query_context,
            query_context_order=self.query_context_order,
            learning_id=self.learning_id or "unknown",
            model=self.model,
        )


def sorted_prompt_context_list(context: PromptContext, order: PromptContextOrder) -> list[str]:
    """
    context = {"x": "foo", "y": "bar"}
    order = {"y": 1, "x": 0}
    sorted_prompt_context_list(context, order) == ["foo", "bar"]
    """
    sorted_items = sorted(
        context.items(),
        key=lambda item: order.get(item[0], float("inf")),
    )
    return list(map(lambda item: item[1], sorted_items))


async def run_prequeries(
    kbid: str,
    prequeries: list[PreQuery],
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
    metrics: Metrics,
) -> list[PreQueryResult]:
    """
    Runs simultaneous find requests for each prequery and returns the merged results according to the normalized weights.
    """
    results: list[PreQueryResult] = []
    max_parallel_prequeries = asyncio.Semaphore(settings.prequeries_max_parallel)

    async def _prequery_find(prequery: PreQuery, index: int):
        async with max_parallel_prequeries:
            prequery_id = prequery.id or f"prequery-{index}"
            find_results, _, _, _ = await find_retrieval(
                kbid,
                prequery.request,
                x_ndb_client,
                x_nucliadb_user,
                x_forwarded_for,
                metrics=metrics.child_span(prequery_id),
            )
            return prequery, find_results

    ops = []
    for idx, prequery in enumerate(prequeries):
        ops.append(asyncio.create_task(_prequery_find(prequery, idx)))
    ops_results = await asyncio.gather(*ops)
    for prequery, find_results in ops_results:
        results.append((prequery, find_results))
    return results


async def get_answer_stream(
    kbid: str,
    item: ChatModel,
    extra_headers: dict[str, str] | None = None,
) -> tuple[str, str, AsyncGenerator[GenerativeChunk, None]]:
    # NOTE: When moving /ask to RAO, this will need to change to whatever client/utility is used
    # to call NUA predict (internally or externally in the case of onprem).
    predict = get_predict()
    return await predict.chat_query_ndjson(
        kbid=kbid,
        item=item,
        extra_headers=extra_headers,
    )


async def find_retrieval(
    kbid: str,
    find_request: FindRequest,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
    metrics: Metrics,
) -> tuple[KnowledgeboxFindResults, bool, Fetcher, Reranker]:
    # TODO(decoupled-ask): Remove once the feature has been fully rolled out
    if not has_feature(const.Features.ASK_DECOUPLED, context={"kbid": kbid}):
        results, incomplete, parsed = await find(
            kbid,
            find_request,
            x_ndb_client,
            x_nucliadb_user,
            x_forwarded_for,
            metrics=metrics,
        )
        # this has already been asserted inside the find() call
        assert parsed.retrieval.reranker is not None, "find parser must provide a reranking algorithm"
        reranker = get_reranker(parsed.retrieval.reranker)
        return results, incomplete, parsed.fetcher, reranker

    return await rao_find(
        kbid,
        find_request,
        x_ndb_client,
        x_nucliadb_user,
        x_forwarded_for,
        metrics=metrics,
    )


async def rao_find(
    kbid: str,
    find_request: FindRequest,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
    metrics: Metrics,
) -> tuple[KnowledgeboxFindResults, bool, Fetcher, Reranker]:
    """This is an equivalent implementation of /find but uses the new /retrieve
    and /augment endpoints under the hood while providing bw/c for the /find
    response model.

    This implementation is provided to comply with the existing /find interface
    to which /ask is tighly coupled with.

    Note there's an edge case, when users ask for features=relations, in which
    we fallback to /find, as it's the simplest way to provide bw/c.

    """
    audit = get_audit()
    start_time = time()

    fetcher, retrieval_request, reranker = await rao_parse_find(kbid, find_request)

    query = find_request.query
    rephrased_query = None
    if retrieval_request.query.keyword:
        if find_request.query != retrieval_request.query.keyword.query:
            rephrased_query = retrieval_request.query.keyword.query

    retrieval_response = await rpc.retrieve(
        kbid,
        retrieval_request,
        x_ndb_client=x_ndb_client,
        x_nucliadb_user=x_nucliadb_user,
        x_forwarded_for=x_forwarded_for,
    )
    matches = retrieval_response.matches

    relations = None
    if FindOptions.RELATIONS in find_request.features:
        # the user asked for a legacy relations search, as we don't support it
        # in the /retrieve endpoint but we must maintain bw/c with /find
        # responses, we call it with to get just this part of the response
        find_response, _ = await rpc.find(
            kbid,
            FindRequest(
                features=[FindOptions.RELATIONS],
                # needed for automatic entity detection
                query=query,
                # used for "hardcoded" graph queries
                query_entities=find_request.query_entities,
            ),
            x_ndb_client,
            x_nucliadb_user,
            x_forwarded_for,
            metrics,
        )
        relations = find_response.relations

    text_blocks, resources, best_matches = await augment_and_rerank(
        kbid,
        matches,
        # here we use the original top_k, so we end up with the number of
        # results requested by the user
        top_k=find_request.top_k,
        resource_hydration_options=ResourceHydrationOptions(
            show=find_request.show,
            extracted=find_request.extracted,
            field_type_filter=find_request.field_type_filter,
        ),
        text_block_hydration_options=TextBlockHydrationOptions(),
        reranker=reranker,
        reranking_options=RerankingOptions(kbid=kbid, query=rephrased_query or query),
    )
    find_resources = compose_find_resources(text_blocks, resources)
    find_results = KnowledgeboxFindResults(
        query=query,
        rephrased_query=query,
        resources=find_resources,
        best_matches=best_matches,
        relations=relations,
        # legacy fields
        total=len(text_blocks),
        page_number=0,
        page_size=find_request.top_k,
        next_page=False,
    )

    # audit request
    if audit is not None:
        from nidx_protos.nodereader_pb2 import SearchRequest

        search_time = time() - start_time
        # TODO(decoupled-ask): implement audit.retrieve or something like that?
        audit.search(
            kbid,
            x_nucliadb_user,
            to_proto.client_type(x_ndb_client),
            x_forwarded_for,
            # TODO(decoupled-ask): we don't have this proto anymore
            SearchRequest(),
            search_time,
            len(find_resources),
            retrieval_rephrased_question=rephrased_query,
        )

    return find_results, False, fetcher, reranker


async def augment_and_rerank(
    kbid: str,
    matches: list[RetrievalMatch],
    top_k: int,
    resource_hydration_options: ResourceHydrationOptions,
    text_block_hydration_options: TextBlockHydrationOptions,
    reranker: Reranker,
    reranking_options: RerankingOptions,
):
    score_type_map = {
        ScoreType.SEMANTIC: SCORE_TYPE.VECTOR,
        ScoreType.KEYWORD: SCORE_TYPE.BM25,
        ScoreType.RRF: SCORE_TYPE.BOTH,
        ScoreType.DEFAULT_RERANKER: SCORE_TYPE.RERANKER,
        ScoreType.GRAPH: SCORE_TYPE.RELATION_RELEVANCE,
    }
    text_blocks = []
    for match in matches:
        paragraph_id = ParagraphId.from_string(match.id)
        score_type = score_type_map[match.score.type]
        text_block = TextBlockMatch(
            paragraph_id=paragraph_id,
            scores=match.score.history,
            score_type=score_type,
            position=TextPosition(
                page_number=match.metadata.page,
                index=0,
                start=paragraph_id.paragraph_start,
                end=paragraph_id.paragraph_end,
                start_seconds=[],
                end_seconds=[],
            ),
            order=-1,  # will be populated later
            fuzzy_search=False,  # we don't have this info anymore
            is_a_table=match.metadata.is_a_table,
            representation_file=match.metadata.source_file,
            field_labels=match.metadata.field_labels,
            paragraph_labels=match.metadata.paragraph_labels,
        )
        text_blocks.append(text_block)

    return await hydrate_and_rerank(
        text_blocks,
        kbid,
        resource_hydration_options=resource_hydration_options,
        text_block_hydration_options=text_block_hydration_options,
        reranker=reranker,
        reranking_options=reranking_options,
        top_k=top_k,
    )


async def hydrate_and_rerank(
    text_blocks: Iterable[TextBlockMatch],
    kbid: str,
    *,
    resource_hydration_options: ResourceHydrationOptions,
    text_block_hydration_options: TextBlockHydrationOptions,
    reranker: Reranker,
    reranking_options: RerankingOptions,
    top_k: int,
) -> tuple[list[TextBlockMatch], list[AugmentedResource], list[str]]:
    """Given a list of text blocks from a retrieval operation, hydrate and
    rerank the results.

    This function returns either the entire list or a subset of updated
    (hydrated and reranked) text blocks and their corresponding resource
    metadata. It also returns an ordered list of best matches.

    """
    # Iterate text blocks to create an "index" for faster access by id and get a
    # list of text block ids and resource ids to hydrate
    text_blocks_by_id: dict[str, TextBlockMatch] = {}  # useful for faster access to text blocks later
    resources_to_hydrate = set()
    text_block_id_to_hydrate = set()

    for text_block in text_blocks:
        rid = text_block.paragraph_id.rid
        paragraph_id = text_block.paragraph_id.full()

        # If we find multiple results (from different indexes) with different
        # metadata, this statement will only get the metadata from the first on
        # the list. We assume metadata is the same on all indexes, otherwise
        # this would be a BUG
        text_blocks_by_id.setdefault(paragraph_id, text_block)

        # rerankers that need extra results may end with less resources than the
        # ones we see now, so we'll skip this step and recompute the resources
        # later
        if not reranker.needs_extra_results:
            resources_to_hydrate.add(rid)

        if text_block_hydration_options.only_hydrate_empty and text_block.text:
            pass
        else:
            text_block_id_to_hydrate.add(paragraph_id)

    resource_select = ResourceProp.from_show_and_extracted(
        resource_hydration_options.show, resource_hydration_options.extracted
    )

    # hydrate only the strictly needed before rerank
    augment_request = AugmentRequest(
        resources=AugmentResources(
            given=list(resources_to_hydrate),
            select=resource_select,
            field_type_filter=resource_hydration_options.field_type_filter,
        ),
        paragraphs=AugmentParagraphs(
            given=[
                AugmentParagraph(
                    id=paragraph_id,
                    metadata=ParagraphMetadata(
                        field_labels=text_blocks_by_id[paragraph_id].field_labels,
                        paragraph_labels=text_blocks_by_id[paragraph_id].paragraph_labels,
                        is_an_image=text_blocks_by_id[paragraph_id].is_an_image,
                        is_a_table=text_blocks_by_id[paragraph_id].is_a_table,
                        source_file=text_blocks_by_id[paragraph_id].representation_file,
                        page=text_blocks_by_id[paragraph_id].position.page_number,
                        in_page_with_visual=text_blocks_by_id[paragraph_id].page_with_visual,
                    ),
                )
                for paragraph_id in text_block_id_to_hydrate
            ],
            text=True,
        ),
    )
    augment_response = await rpc.augment(kbid, augment_request)
    augmented_paragraphs = augment_response.paragraphs
    augmented_resources = augment_response.resources

    # add hydrated text to our text blocks
    for text_block in text_blocks:
        augmented_paragraph = augmented_paragraphs.get(text_block.paragraph_id.full(), None)
        if augmented_paragraph is not None and augmented_paragraph.text is not None:
            if text_block_hydration_options.highlight:
                text = highlight_paragraph(
                    augmented_paragraph.text, words=[], ematches=text_block_hydration_options.ematches
                )
            else:
                text = augmented_paragraph.text
            text_block.text = text

    # with the hydrated text, rerank and apply new scores to the text blocks
    to_rerank = [
        RerankableItem(
            id=text_block.paragraph_id.full(),
            score=text_block.score,
            score_type=text_block.score_type,
            content=text_block.text or "",  # TODO: add a warning, this shouldn't usually happen
        )
        for text_block in text_blocks
    ]
    reranked = await reranker.rerank(to_rerank, reranking_options)

    # after reranking, we can cut to the number of results the user wants, so we
    # don't hydrate unnecessary stuff
    reranked = reranked[:top_k]

    matches = []
    for item in reranked:
        paragraph_id = item.id
        score = item.score
        score_type = item.score_type

        text_block = text_blocks_by_id[paragraph_id]
        text_block.scores.append(RerankerScore(score=score))
        text_block.score_type = score_type

        matches.append((paragraph_id, score))

    matches.sort(key=lambda x: x[1], reverse=True)

    best_matches = []
    best_text_blocks = []
    resources_to_hydrate.clear()
    for order, (paragraph_id, _) in enumerate(matches):
        text_block = text_blocks_by_id[paragraph_id]
        text_block.order = order
        best_matches.append(paragraph_id)
        best_text_blocks.append(text_block)

        # now we have removed the text block surplus, fetch resource metadata
        if reranker.needs_extra_results:
            rid = ParagraphId.from_string(paragraph_id).rid
            resources_to_hydrate.add(rid)

    # Finally, fetch resource metadata if we haven't already done it
    if reranker.needs_extra_results:
        augmented = await rpc.augment(
            kbid,
            AugmentRequest(
                resources=AugmentResources(
                    given=list(resources_to_hydrate),
                    select=resource_select,
                    field_type_filter=resource_hydration_options.field_type_filter,
                ),
            ),
        )
        augmented_resources = augmented.resources

    resources = [resource for resource in augmented_resources.values()]

    return best_text_blocks, resources, best_matches


def compose_find_resources(
    text_blocks: list[TextBlockMatch],
    resources: list[AugmentedResource],
) -> dict[str, FindResource]:
    find_resources: dict[str, FindResource] = {}

    for resource in resources:
        rid = resource.id
        if rid not in find_resources:
            find_resources[rid] = FindResource(id=rid, fields={})
            find_resources[rid].updated_from(resource)

    for text_block in text_blocks:
        rid = text_block.paragraph_id.rid
        if rid not in find_resources:
            # resource not found in db, skipping
            continue

        find_resource = find_resources[rid]
        field_id = text_block.paragraph_id.field_id.short_without_subfield()
        find_field = find_resource.fields.setdefault(field_id, FindField(paragraphs={}))

        paragraph_id = text_block.paragraph_id.full()
        find_paragraph = text_block_to_find_paragraph(text_block)

        find_field.paragraphs[paragraph_id] = find_paragraph

    return find_resources
