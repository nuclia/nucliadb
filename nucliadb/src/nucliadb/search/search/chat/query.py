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
from typing import Iterable, Optional, Union

from nidx_protos.nodereader_pb2 import (
    GraphSearchResponse,
    SearchRequest,
    SearchResponse,
)

from nucliadb.common.external_index_providers.base import TextBlockMatch
from nucliadb.common.ids import ParagraphId
from nucliadb.common.models_utils import to_proto
from nucliadb.models.internal.augment import ParagraphText
from nucliadb.models.internal.retrieval import RerankerScore
from nucliadb.search import logger
from nucliadb.search.augmentor.models import AugmentedParagraph, AugmentedResource, Metadata, Paragraph
from nucliadb.search.augmentor.paragraphs import augment_paragraphs
from nucliadb.search.augmentor.resources import augment_resources
from nucliadb.search.augmentor.utils import limited_concurrency
from nucliadb.search.predict import AnswerStatusCode, RephraseResponse
from nucliadb.search.requesters.utils import Method, nidx_query
from nucliadb.search.search.chat.exceptions import NoRetrievalResultsError
from nucliadb.search.search.cut import cut_page
from nucliadb.search.search.exceptions import IncompleteFindResultsError
from nucliadb.search.search.find import find
from nucliadb.search.search.find_merge import _round, compose_find_resources
from nucliadb.search.search.hydrator import ResourceHydrationOptions, TextBlockHydrationOptions
from nucliadb.search.search.merge import merge_relations_results
from nucliadb.search.search.metrics import Metrics
from nucliadb.search.search.paragraphs import highlight_paragraph
from nucliadb.search.search.query_parser.fetcher import Fetcher
from nucliadb.search.search.query_parser.models import ParsedQuery, Query, RelationQuery, UnitRetrieval
from nucliadb.search.search.query_parser.parsers.find import parse_find
from nucliadb.search.search.query_parser.parsers.unit_retrieval import (
    convert_retrieval_to_proto,
    get_rephrased_query,
)
from nucliadb.search.search.rerankers import RerankableItem, Reranker, RerankingOptions
from nucliadb.search.search.retrieval import text_block_search
from nucliadb.search.settings import settings
from nucliadb.search.utilities import get_predict
from nucliadb_models import filters
from nucliadb_models.resource import Resource
from nucliadb_models.search import (
    AskRequest,
    ChatContextMessage,
    ChatOptions,
    FindOptions,
    FindRequest,
    KnowledgeboxFindResults,
    MinScore,
    NucliaDBClientType,
    PreQueriesStrategy,
    PreQuery,
    PreQueryResult,
    PromptContext,
    PromptContextOrder,
    Relations,
    RephraseModel,
    parse_rephrase_prompt,
)
from nucliadb_protos import audit_pb2
from nucliadb_protos.utils_pb2 import RelationNode
from nucliadb_telemetry.errors import capture_exception
from nucliadb_utils.utilities import get_audit

NOT_ENOUGH_CONTEXT_ANSWER = "Not enough data to answer this."


async def rephrase_query(
    kbid: str,
    chat_history: list[ChatContextMessage],
    query: str,
    user_id: str,
    user_context: list[str],
    generative_model: Optional[str] = None,
    chat_history_relevance_threshold: Optional[float] = None,
) -> RephraseResponse:
    # When moved to RAO, this must be an internal request from RAO to predict api
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


async def find_retrieval(
    *,
    kbid: str,
    query: str,
    item: AskRequest,
    ndb_client: NucliaDBClientType,
    user: str,
    origin: str,
    metrics: Metrics,
    prequeries_strategy: Optional[PreQueriesStrategy] = None,
) -> tuple[KnowledgeboxFindResults, Optional[list[PreQueryResult]], ParsedQuery]:
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
        main_results, query_parser = await run_main_query(
            kbid,
            query,
            item,
            ndb_client,
            user,
            origin,
            metrics=metrics.child_span("main_query"),
        )
    return main_results, prequeries_results, query_parser


def add_resource_filter(request: Union[FindRequest, AskRequest], resources: list[str]):
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
) -> tuple[KnowledgeboxFindResults, ParsedQuery]:
    find_request = find_request_from_ask_request(item, query)

    find_results, incomplete, parsed_query = await find(
        kbid,
        find_request,
        ndb_client,
        user,
        origin,
        metrics=metrics,
    )
    if incomplete:
        raise IncompleteFindResultsError()
    return find_results, parsed_query


async def get_relations_results(
    *,
    kbid: str,
    text_answer: str,
    timeout: Optional[float] = None,
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
    timeout: Optional[float] = None,
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
    generative_reasoning_first_chunk_time: Optional[float],
    rephrase_time: Optional[float],
    user_query: str,
    rephrased_query: Optional[str],
    retrieval_rephrase_query: Optional[str],
    text_answer: bytes,
    text_reasoning: Optional[str],
    status_code: AnswerStatusCode,
    chat_history: list[ChatContextMessage],
    query_context: PromptContext,
    query_context_order: PromptContextOrder,
    learning_id: Optional[str],
    model: Optional[str],
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


def parse_audit_answer(raw_text_answer: bytes, status_code: AnswerStatusCode) -> Optional[str]:
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
        rephrased_query: Optional[str],
        retrieval_rephrased_query: Optional[str],
        chat_history: list[ChatContextMessage],
        learning_id: Optional[str],
        query_context: PromptContext,
        query_context_order: PromptContextOrder,
        model: Optional[str],
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
        text_reasoning: Optional[str],
        generative_answer_time: float,
        generative_answer_first_chunk_time: float,
        generative_reasoning_first_chunk_time: Optional[float],
        rephrase_time: Optional[float],
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
    # XXX: TODO (use as before!)
) -> list[PreQueryResult]:
    """
    Runs simultaneous text block retrievals for each prequery and returns the list of results.
    """
    limited = limited_concurrency(max_ops=asyncio.Semaphore(settings.prequeries_max_parallel))

    # First, parse all prequeries to get their retrieval units: this will call the /query endpoint at the predict api.
    async def _pre_retrieval(request: FindRequest) -> ParsedQuery:
        async with limited:
            return await parse_find(kbid, request)

    parsed_queries: list[ParsedQuery] = await asyncio.gather(
        *[_pre_retrieval(kbid, prequery.request) for prequery in prequeries]
    )

    # Then, for each retrieval unit, perform the text block search. This calls nidx
    async def _retrieve(retrieval: UnitRetrieval) -> tuple[list[TextBlockMatch], SearchRequest, SearchResponse, list[str]]:
        # When moved to RAO, this will be a call to nucliadb_sdk.AsyncNucliaDB.retrieve()
        async with limited:
            return await text_block_search(kbid, retrieval)

    prequeries_results = await asyncio.gather(
        *[_retrieve(parsed_query.retrieval) for parsed_query in parsed_queries]
    )

    # Do a single hydration/augmentation of all text blocks to avoid unnecessary repeated calls.
    # When moved to RAO, this will be a call to nucliadb_sdk.AsyncNucliaDB.augment()
    augmented_resources, augmented_text_blocks = await augment(
        kbid,
        [tb for text_blocks, _, _, _ in prequeries_results for tb in text_blocks],
    )

    # Then convert to KnowledgeboxFindResults to keep current interface.
    return [
        (prequery, await convert_to_find_results(
            prequery,
            parsed_query,
            augmented_resources,
            augmented_text_blocks,
            result,
        )) for prequery, result, parsed_query in zip(prequeries, prequeries_results, parsed_queries)
    ]


async def convert_to_find_results(
    prequery: PreQuery,
    parsed_query: ParsedQuery,
    augmented_resources: list[AugmentedResource],
    augmented_paragraphs: list[AugmentedParagraph],
    prequery_result: tuple[list[TextBlockMatch], SearchRequest, SearchResponse, list[str]],
) -> KnowledgeboxFindResults:
    text_block_matches = prequery_result[0]
    matching_paragraph_ids = {tb.paragraph_id for tb in text_block_matches}
    matching_resource_ids = {pid.rid for pid in matching_paragraph_ids}

    search_request = prequery_result[1]
    search_response = prequery_result[2]
    queried_shards = prequery_result[3]

    # Compose response
    matching_augmented_paragraphs = [
        ap for ap in augmented_paragraphs if ap.id in matching_paragraph_ids
    ]
    matching_augmented_resources = {
        resource for resource in augmented_resources if resource.id in matching_resource_ids
    }
    find_resources = compose_find_resources(matching_augmented_paragraphs, matching_augmented_resources)

    # build relations graph
    entry_points = []
    if parsed_query.retrieval.query.relation is not None:
        entry_points = parsed_query.retrieval.query.relation.entry_points

    graph_response = search_response.graph
    relations = await merge_relations_results([graph_response], entry_points)

    min_score = MinScore(semantic=None, bm25=0)
    if parsed_query.retrieval.query.semantic is not None:
        min_score.semantic = parsed_query.retrieval.query.semantic.min_score
    if parsed_query.retrieval.query.fulltext is not None:
        min_score.bm25 = parsed_query.retrieval.query.fulltext.min_score
    find_results = KnowledgeboxFindResults(
        query=search_request.body,
        rephrased_query=get_rephrased_query(parsed_query),
        resources=find_resources,
        best_matches="best_matches",
        relations=relations,
        total=search_response.paragraph.total,
        page_size=parsed_query.retrieval.top_k,
        min_score=min_score,
        # For bw/compatibility, even if we don't use pagination here
        page_number=0,
        next_page=False,
    )
    return find_results


async def augment(
    kbid: str,
    text_blocks: list[TextBlockMatch],
    semaphore: asyncio.Semaphore,
) -> tuple[list[AugmentedResource], list[AugmentedParagraph]]:
    augmented_paragraphs = await augment_paragraphs(
        kbid,
        given=[
            Paragraph(
                id=tb.paragraph_id,
                metadata=Metadata.from_text_block_match(tb),
            )
            for tb in text_blocks
        ],
        select=[ParagraphText()],
        concurrency_control=semaphore,
    )
    augmented_resources = await augment_resources(
        kbid,
        given=list({tb.paragraph_id.rid for tb in text_blocks}),
        opts=ResourceHydrationOptions(
            show=[],
            extracted=[],
            field_type_filter=[],
        ),
        concurrency_control=semaphore,
    )
    return [r for r in augmented_resources.values() if r is not None], [
        p for p in augmented_paragraphs.values() if p is not None
    ]


#@merge_observer.wrap({"type": "find_merge"})
async def build_find_response(
    search_response: SearchResponse,
    merged_text_blocks: list[TextBlockMatch],
    graph_response: GraphSearchResponse,
    *,
    retrieval: UnitRetrieval,
    kbid: str,
    query: str,
    rephrased_query: Optional[str],
    reranker: Reranker,
    resource_hydration_options: ResourceHydrationOptions,
    text_block_hydration_options: TextBlockHydrationOptions,
) -> KnowledgeboxFindResults:
    # XXX: we shouldn't need a min score that we haven't used. Previous
    # implementations got this value from the proto request (i.e., default to 0)
    min_score_bm25 = 0.0
    if retrieval.query.keyword is not None:
        min_score_bm25 = retrieval.query.keyword.min_score
    min_score_semantic = 0.0
    if retrieval.query.semantic is not None:
        min_score_semantic = retrieval.query.semantic.min_score

    # cut
    # we assume pagination + predict reranker is forbidden and has been already
    # enforced/validated by the query parsing.
    if reranker.needs_extra_results:
        assert reranker.window is not None, "Reranker definition must enforce this condition"
        text_blocks_page, next_page = cut_page(merged_text_blocks, reranker.window)
    else:
        text_blocks_page, next_page = cut_page(merged_text_blocks, retrieval.top_k)

    # hydrate and rerank
    reranking_options = RerankingOptions(kbid=kbid, query=query)
    text_blocks, resources, best_matches = await hydrate_and_rerank(
        text_blocks_page,
        kbid,
        resource_hydration_options=resource_hydration_options,
        text_block_hydration_options=text_block_hydration_options,
        reranker=reranker,
        reranking_options=reranking_options,
        top_k=retrieval.top_k,
    )

    # build relations graph
    entry_points = []
    if retrieval.query.relation is not None:
        entry_points = retrieval.query.relation.entry_points
    relations = await merge_relations_results([graph_response], entry_points)

    # compose response
    find_resources = compose_find_resources(text_blocks, resources)

    next_page = search_response.paragraph.next_page or next_page
    total_paragraphs = search_response.paragraph.total

    find_results = KnowledgeboxFindResults(
        query=query,
        rephrased_query=rephrased_query,
        resources=find_resources,
        best_matches=best_matches,
        relations=relations,
        total=total_paragraphs,
        page_number=0,  # Bw/c with pagination
        page_size=retrieval.top_k,
        next_page=next_page,
        min_score=MinScore(bm25=_round(min_score_bm25), semantic=_round(min_score_semantic)),
    )
    return find_results



#@merge_observer.wrap({"type": "hydrate_and_rerank"})
async def hydrate_and_rerank(
    text_blocks: Iterable[TextBlockMatch],
    kbid: str,
    *,
    resource_hydration_options: ResourceHydrationOptions,
    text_block_hydration_options: TextBlockHydrationOptions,
    reranker: Reranker,
    reranking_options: RerankingOptions,
    top_k: int,
) -> tuple[list[TextBlockMatch], list[Resource], list[str]]:
    """Given a list of text blocks from a retrieval operation, hydrate and
    rerank the results.

    This function returns either the entire list or a subset of updated
    (hydrated and reranked) text blocks and their corresponding resource
    metadata. It also returns an ordered list of best matches.

    """
    max_operations = asyncio.Semaphore(50)

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

    # hydrate only the strictly needed before rerank
    ops = [
        augment_paragraphs(
            kbid,
            given=[
                Paragraph.from_text_block_match(text_blocks_by_id[paragraph_id])
                for paragraph_id in text_block_id_to_hydrate
            ],
            select=[ParagraphText()],
            concurrency_control=max_operations,
        ),
        augment_resources(
            kbid,
            given=list(resources_to_hydrate),
            opts=resource_hydration_options,
            concurrency_control=max_operations,
        ),
    ]
#    FIND_FETCH_OPS_DISTRIBUTION.observe(len(text_block_id_to_hydrate) + len(resources_to_hydrate))
    results = await asyncio.gather(*ops)

    augmented_paragraphs: dict[ParagraphId, AugmentedParagraph | None] = results[0]  # type: ignore
    augmented_resources: dict[str, AugmentedResource | None] = results[1]  # type: ignore

    # add hydrated text to our text blocks
    for text_block in text_blocks:
        augmented = augmented_paragraphs.get(text_block.paragraph_id, None)
        if augmented is not None and augmented.text is not None:
            if text_block_hydration_options.highlight:
                text = highlight_paragraph(
                    augmented.text, words=[], ematches=text_block_hydration_options.ematches
                )
            else:
                text = augmented.text
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
#        FIND_FETCH_OPS_DISTRIBUTION.observe(len(resources_to_hydrate))
        augmented_resources = await augment_resources(
            kbid,
            given=list(resources_to_hydrate),
            opts=resource_hydration_options,
            concurrency_control=max_operations,
        )

    resources = [resource for resource in augmented_resources.values() if resource is not None]

    return best_text_blocks, resources, best_matches


#@query_parser_observer.wrap({"type": "parse_find"})
async def parse_query(
    kbid: str,
    item: FindRequest,
    *,
    fetcher: Optional[Fetcher] = None,
) -> ParsedQuery:
    fetcher = fetcher or fetcher_for_find(kbid, item)
    parser = _FindParser(kbid, item, fetcher)
    retrieval = await parser.parse()
    return ParsedQuery(fetcher=fetcher, retrieval=retrieval, generation=None)


def fetcher_for_find(kbid: str, item: FindRequest) -> Fetcher:
    return Fetcher(
        kbid=kbid,
        query=item.query,
        user_vector=item.vector,
        vectorset=item.vectorset,
        rephrase=item.rephrase,
        rephrase_prompt=item.rephrase_prompt,
        generative_model=item.generative_model,
        query_image=item.query_image,
    )


class _FindParser:
    def __init__(self, kbid: str, item: FindRequest, fetcher: Fetcher):
        self.kbid = kbid
        self.item = item
        self.fetcher = fetcher

        # cached data while parsing
        self._query: Optional[Query] = None
        self._top_k: Optional[int] = None

    async def parse(self) -> UnitRetrieval:
        self._validate_request()

        self._top_k = parse_top_k(self.item)

        # parse search types (features)

        self._query = Query()

        if search_models.FindOptions.KEYWORD in self.item.features:
            self._query.keyword = await parse_keyword_query(self.item, fetcher=self.fetcher)

        if search_models.FindOptions.SEMANTIC in self.item.features:
            self._query.semantic = await parse_semantic_query(self.item, fetcher=self.fetcher)

        if search_models.FindOptions.RELATIONS in self.item.features:
            self._query.relation = await self._parse_relation_query()

        if search_models.FindOptions.GRAPH in self.item.features:
            self._query.graph = await self._parse_graph_query()

        filters = await self._parse_filters()

        try:
            rank_fusion = self._parse_rank_fusion()
        except ValidationError as exc:
            raise InternalParserError(f"Parsing error in rank fusion: {str(exc)}") from exc
        try:
            reranker = self._parse_reranker()
        except ValidationError as exc:
            raise InternalParserError(f"Parsing error in reranker: {str(exc)}") from exc

        # Adjust retrieval windows. Our current implementation assume:
        # `top_k <= reranker.window <= rank_fusion.window`
        # and as rank fusion is done before reranking, we must ensure rank
        # fusion window is at least, the reranker window
        if isinstance(reranker, PredictReranker):
            rank_fusion.window = max(rank_fusion.window, reranker.window)

        retrieval = UnitRetrieval(
            query=self._query,
            top_k=self._top_k,
            filters=filters,
            rank_fusion=rank_fusion,
            reranker=reranker,
        )
        return retrieval

    def _validate_request(self):
        # synonyms are not compatible with vector/graph search
        if (
            self.item.with_synonyms
            and self.item.query
            and (
                search_models.FindOptions.SEMANTIC in self.item.features
                or search_models.FindOptions.RELATIONS in self.item.features
                or search_models.FindOptions.GRAPH in self.item.features
            )
        ):
            raise InvalidQueryError(
                "synonyms",
                "Search with custom synonyms is only supported on paragraph and document search",
            )

        if search_models.FindOptions.SEMANTIC in self.item.features:
            if should_disable_vector_search(self.item):
                self.item.features.remove(search_models.FindOptions.SEMANTIC)

        if self.item.graph_query and search_models.FindOptions.GRAPH not in self.item.features:
            raise InvalidQueryError("graph_query", "Using a graph query requires enabling graph feature")

    async def _parse_relation_query(self) -> RelationQuery:
        detected_entities = await self._get_detected_entities()

        deleted_entity_groups = await self.fetcher.get_deleted_entity_groups()

        meta_cache = await self.fetcher.get_entities_meta_cache()
        deleted_entities = meta_cache.deleted_entities

        return RelationQuery(
            entry_points=detected_entities,
            deleted_entity_groups=deleted_entity_groups,
            deleted_entities=deleted_entities,
        )

    async def _parse_graph_query(self) -> GraphQuery:
        if self.item.graph_query is None:
            raise InvalidQueryError(
                "graph_query", "Graph query must be provided when using graph search"
            )
        return GraphQuery(query=self.item.graph_query)

    async def _get_detected_entities(self) -> list[utils_pb2.RelationNode]:
        """Get entities from request, either automatically detected or
        explicitly set by the user."""

        if self.item.query_entities:
            detected_entities = []
            for entity in self.item.query_entities:
                relation_node = utils_pb2.RelationNode()
                relation_node.value = entity.name
                if entity.type is not None:
                    relation_node.ntype = RelationNodeTypeMap[entity.type]
                if entity.subtype is not None:
                    relation_node.subtype = entity.subtype
                detected_entities.append(relation_node)
        else:
            detected_entities = await self.fetcher.get_detected_entities()

        meta_cache = await self.fetcher.get_entities_meta_cache()
        detected_entities = expand_entities(meta_cache, detected_entities)

        return detected_entities

    async def _parse_filters(self) -> Filters:
        assert self._query is not None, "query must be parsed before filters"

        has_old_filters = (
            len(self.item.filters) > 0
            or len(self.item.resource_filters) > 0
            or len(self.item.fields) > 0
            or len(self.item.keyword_filters) > 0
            or self.item.range_creation_start is not None
            or self.item.range_creation_end is not None
            or self.item.range_modification_start is not None
            or self.item.range_modification_end is not None
        )
        if self.item.filter_expression is not None and has_old_filters:
            raise InvalidQueryError("filter_expression", "Cannot mix old filters with filter_expression")

        field_expr = None
        paragraph_expr = None
        filter_operator = nodereader_pb2.FilterOperator.AND

        if has_old_filters:
            old_filters = OldFilterParams(
                label_filters=self.item.filters,
                keyword_filters=self.item.keyword_filters,
                range_creation_start=self.item.range_creation_start,
                range_creation_end=self.item.range_creation_end,
                range_modification_start=self.item.range_modification_start,
                range_modification_end=self.item.range_modification_end,
                fields=self.item.fields,
                key_filters=self.item.resource_filters,
            )
            field_expr, paragraph_expr = await parse_old_filters(old_filters, self.fetcher)

        if self.item.filter_expression is not None:
            if self.item.filter_expression.field:
                field_expr = await parse_expression(self.item.filter_expression.field, self.kbid)
            if self.item.filter_expression.paragraph:
                paragraph_expr = await parse_expression(self.item.filter_expression.paragraph, self.kbid)
            if self.item.filter_expression.operator == FilterExpression.Operator.OR:
                filter_operator = nodereader_pb2.FilterOperator.OR
            else:
                filter_operator = nodereader_pb2.FilterOperator.AND

        hidden = await filter_hidden_resources(self.kbid, self.item.show_hidden)

        return Filters(
            facets=[],
            field_expression=field_expr,
            paragraph_expression=paragraph_expr,
            filter_expression_operator=filter_operator,
            security=self.item.security,
            hidden=hidden,
            with_duplicates=self.item.with_duplicates,
        )

    def _parse_rank_fusion(self) -> RankFusion:
        rank_fusion: RankFusion

        top_k = parse_top_k(self.item)
        window = min(top_k, 500)

        if isinstance(self.item.rank_fusion, search_models.RankFusionName):
            if self.item.rank_fusion == search_models.RankFusionName.RECIPROCAL_RANK_FUSION:
                rank_fusion = ReciprocalRankFusion(window=window)
            else:
                raise InternalParserError(f"Unknown rank fusion algorithm: {self.item.rank_fusion}")

        elif isinstance(self.item.rank_fusion, search_models.ReciprocalRankFusion):
            user_window = self.item.rank_fusion.window
            rank_fusion = ReciprocalRankFusion(
                k=self.item.rank_fusion.k,
                boosting=self.item.rank_fusion.boosting,
                window=min(max(user_window or 0, top_k), 500),
            )

        else:
            raise InternalParserError(f"Unknown rank fusion {self.item.rank_fusion}")

        return rank_fusion

    def _parse_reranker(self) -> Reranker:
        reranking: Reranker

        top_k = parse_top_k(self.item)

        if isinstance(self.item.reranker, search_models.RerankerName):
            if self.item.reranker == search_models.RerankerName.NOOP:
                reranking = NoopReranker()

            elif self.item.reranker == search_models.RerankerName.PREDICT_RERANKER:
                # for predict rearnker, by default, we want a x2 factor with a
                # top of 200 results
                reranking = PredictReranker(window=min(top_k * 2, 200))

            else:
                raise InternalParserError(f"Unknown reranker algorithm: {self.item.reranker}")

        elif isinstance(self.item.reranker, search_models.PredictReranker):
            user_window = self.item.reranker.window
            reranking = PredictReranker(window=min(max(user_window or 0, top_k), 200))

        else:
            raise InternalParserError(f"Unknown reranker {self.item.reranker}")

        return reranking
