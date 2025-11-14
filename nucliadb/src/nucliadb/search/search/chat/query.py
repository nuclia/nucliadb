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
from nucliadb.common.models_utils import to_proto
from nucliadb.models.internal.augment import ParagraphText
from nucliadb.search import logger
from nucliadb.search.augmentor.models import AugmentedParagraph, AugmentedResource, Metadata, Paragraph
from nucliadb.search.augmentor.paragraphs import augment_paragraphs
from nucliadb.search.augmentor.resources import augment_resources
from nucliadb.search.augmentor.utils import limited_concurrency
from nucliadb.search.predict import AnswerStatusCode, RephraseResponse
from nucliadb.search.requesters.utils import Method, nidx_query
from nucliadb.search.search.chat.exceptions import NoRetrievalResultsError
from nucliadb.search.search.exceptions import IncompleteFindResultsError
from nucliadb.search.search.find import find
from nucliadb.search.search.find_merge import compose_find_resources
from nucliadb.search.search.hydrator import ResourceHydrationOptions
from nucliadb.search.search.merge import merge_relations_results
from nucliadb.search.search.metrics import Metrics
from nucliadb.search.search.query_parser.models import ParsedQuery, Query, RelationQuery, UnitRetrieval
from nucliadb.search.search.query_parser.parsers.find import parse_find
from nucliadb.search.search.query_parser.parsers.unit_retrieval import (
    convert_retrieval_to_proto,
    get_rephrased_query,
)
from nucliadb.search.search.retrieval import text_block_search
from nucliadb.search.settings import settings
from nucliadb.search.utilities import get_predict
from nucliadb_models import filters
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
        (prequery, convert_to_find_results(
            prequery,
            parsed_query,
            augmented_resources,
            augmented_text_blocks,
            result,
        )) for prequery, result, parsed_query in zip(prequeries, prequeries_results, parsed_queries)
    ]


def convert_to_find_results(
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
        relations="relations",
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
