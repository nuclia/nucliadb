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
from typing import AsyncGenerator, Iterable, Optional, Union

from async_lru import alru_cache
from nidx_protos import nodereader_pb2
from nidx_protos.nodereader_pb2 import (
    GraphSearchResponse,
    SearchResponse,
)
from nuclia_models.predict.generative_responses import GenerativeChunk
from pydantic import ValidationError

from nucliadb.common import datamanagers
from nucliadb.common.exceptions import InvalidQueryError
from nucliadb.common.filter_expression import parse_expression
from nucliadb.common.maindb.utils import get_driver
from nucliadb.common.models_utils import to_proto
from nucliadb.common.models_utils.from_proto import RelationNodeTypeMap
from nucliadb.search import logger
from nucliadb.search.predict import (
    AnswerStatusCode,
    RephraseResponse,
    SendToPredictError,
    convert_relations,
)
from nucliadb.search.predict_models import QueryModel
from nucliadb.search.requesters.utils import Method, nidx_query
from nucliadb.search.search.chat.exceptions import NoRetrievalResultsError
from nucliadb.search.search.exceptions import IncompleteFindResultsError
from nucliadb.search.search.find import find
from nucliadb.search.search.merge import merge_relations_results
from nucliadb.search.search.metrics import Metrics
from nucliadb.search.search.query import expand_entities
from nucliadb.search.search.query_parser.exceptions import InternalParserError
from nucliadb.search.search.query_parser.fetcher import Fetcher, FetcherCache, is_cached
from nucliadb.search.search.query_parser.models import (
    Filters,
    GraphQuery,
    NoopReranker,
    ParsedQuery,
    PredictReranker,
    Query,
    RankFusion,
    ReciprocalRankFusion,
    RelationQuery,
    Reranker,
    UnitRetrieval,
)
from nucliadb.search.search.query_parser.old_filters import OldFilterParams, parse_old_filters
from nucliadb.search.search.query_parser.parsers.common import (
    parse_keyword_query,
    parse_semantic_query,
    should_disable_vector_search,
)
from nucliadb.search.search.query_parser.parsers.unit_retrieval import convert_retrieval_to_proto
from nucliadb.search.search.utils import filter_hidden_resources
from nucliadb.search.settings import settings
from nucliadb.search.utilities import get_predict
from nucliadb_models import filters
from nucliadb_models import search as search_models
from nucliadb_models.filters import FilterExpression
from nucliadb_models.internal.predict import QueryInfo
from nucliadb_models.search import (
    AskRequest,
    ChatContextMessage,
    ChatModel,
    ChatOptions,
    FindOptions,
    FindRequest,
    Image,
    KnowledgeboxFindResults,
    MaxTokens,
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
from nucliadb_protos import audit_pb2, knowledgebox_pb2, utils_pb2
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
    generative_model: Optional[str] = None,
    chat_history_relevance_threshold: Optional[float] = None,
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


async def get_answer_stream(
    kbid: str,
    item: ChatModel,
    extra_predict_headers: Optional[dict[str, str]] = None,
) -> tuple[str, str, AsyncGenerator[GenerativeChunk, None]]:
    # NOTE: When moving /ask to RAO, this will need to change to whatever client/utility is used
    # to call NUA predict (internally or externally in the case of onprem).
    predict = get_predict()
    return await predict.chat_query_ndjson(kbid=kbid, item=item, extra_headers=extra_predict_headers)


async def get_find_results(
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
    find_results, incomplete, parsed_query = await find_retrieval(
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
    Runs simultaneous find requests for each prequery and returns the merged results according to the normalized weights.
    """
    results: list[PreQueryResult] = []
    max_parallel_prequeries = asyncio.Semaphore(settings.prequeries_max_parallel)

    async def _prequery_find(prequery: PreQuery, index: int):
        async with max_parallel_prequeries:
            prequery_id = prequery.id or f"prequery-{index}"
            find_results, _, _ = await find_retrieval(
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


async def find_retrieval(
    kbid: str,
    find_request: FindRequest,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
    metrics: Metrics,
) -> tuple[KnowledgeboxFindResults, bool, ParsedQuery]:
    if has_feature(const.Features.ASK_TO_RAO, context={"kbid": kbid}):
        find_results, incomplete, parsed_query = await rao_find(
            kbid,
            find_request,
            x_ndb_client,
            x_nucliadb_user,
            x_forwarded_for,
            metrics=metrics,
        )
    else:
        # TODO: Remove once /ask has been fully migrated to RAO.
        find_results, incomplete, parsed_query = await find(
            kbid,
            find_request,
            x_ndb_client,
            x_nucliadb_user,
            x_forwarded_for,
            metrics=metrics,
        )
    return find_results, incomplete, parsed_query


async def rao_find(
    kbid: str,
    find_request: FindRequest,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
    metrics: Metrics,
) -> tuple[KnowledgeboxFindResults, bool, ParsedQuery]:
    """
    Calls to NucliaDB retrieve and augment primitives to perform the text block search.
    It returns the results as KnowledgeboxFindResults to comply with the existing find
    interface (/ask logic is tightly coupled with /find).
    """
    parsed = await rao_parse_query(kbid, find_request)
    # TODO: Implement retrieval
    # TODO: Implement augmentation
    # TODO: Implement reranking
    # TODO: Adapt results to KnowledgeboxFindResults
    return KnowledgeboxFindResults(resources={}), False, parsed


async def rao_parse_query(kbid: str, item: FindRequest) -> ParsedQuery:
    fetcher = RAOFetcher(
        kbid=kbid,
        query=item.query,
        user_vector=item.vector,
        vectorset=item.vectorset,
        rephrase=item.rephrase,
        rephrase_prompt=item.rephrase_prompt,
        generative_model=item.generative_model,
        query_image=item.query_image,
    )
    parser = RAOQueryParser(kbid, item, fetcher)
    retrieval = await parser.parse()
    return ParsedQuery(fetcher=fetcher, retrieval=retrieval, generation=None)


class RAOFetcher(Fetcher):
    """Queries are getting more and more complex and different phases of the
    query depend on different data, not only from the user but from other parts
    of the system.

    This class is an encapsulation of data gathering across different parts of
    the system. Given the user query input, it aims to be as efficient as
    possible removing redundant expensive calls to other parts of the system. An
    instance of a fetcher caches it's results and it's thought to be used in the
    context of a single request. DO NOT use this as a global object!
    """

    def __init__(
        self,
        kbid: str,
        *,
        query: str,
        user_vector: Optional[list[float]],
        vectorset: Optional[str],
        rephrase: bool,
        rephrase_prompt: Optional[str],
        generative_model: Optional[str],
        query_image: Optional[Image],
    ):
        self.kbid = kbid
        self.query = query
        self.user_vector = user_vector
        self.user_vectorset = vectorset
        self.user_vectorset_validated = False
        self.rephrase = rephrase
        self.rephrase_prompt = rephrase_prompt
        self.generative_model = generative_model
        self.query_image = query_image

        self.cache = FetcherCache()
        self.locks: dict[str, asyncio.Lock] = {}

    # Semantic search

    async def get_matryoshka_dimension(self) -> Optional[int]:
        vectorset = await self.get_vectorset()
        return await get_matryoshka_dimension_cached(self.kbid, vectorset)

    async def _get_user_vectorset(self) -> Optional[str]:
        """Returns the user's requested vectorset and validates if it does exist
        in the KB.

        """
        async with self.locks.setdefault("user_vectorset", asyncio.Lock()):
            if not self.user_vectorset_validated:
                if self.user_vectorset is not None:
                    await self.validate_vectorset(self.kbid, self.user_vectorset)
            self.user_vectorset_validated = True
            return self.user_vectorset

    async def get_vectorset(self) -> str:
        """Get the vectorset to be used in the search. If not specified, by the
        user, Predict API or the own uses KB will provide a default.

        """
        async with self.locks.setdefault("vectorset", asyncio.Lock()):
            if is_cached(self.cache.vectorset):
                return self.cache.vectorset

            user_vectorset = await self._get_user_vectorset()
            if user_vectorset:
                # user explicitly asked for a vectorset
                self.cache.vectorset = user_vectorset
                return user_vectorset

            # when it's not provided, we get the default from Predict API
            query_info = await self._predict_query_endpoint()
            if query_info is None:
                vectorset = None
            else:
                if query_info.sentence is None:
                    logger.error(
                        "Asking for a vectorset but /query didn't return one", extra={"kbid": self.kbid}
                    )
                    vectorset = None
                else:
                    # vectors field is enforced by the data model to have at least one key
                    for vectorset in query_info.sentence.vectors.keys():
                        vectorset = vectorset
                        break

            if vectorset is None:
                # in case predict don't answer which vectorset to use, fallback to
                # the first vectorset of the KB
                async with datamanagers.with_ro_transaction() as txn:
                    async for vectorset, _ in datamanagers.vectorsets.iter(txn, kbid=self.kbid):
                        break
                assert vectorset is not None, "All KBs must have at least one vectorset in maindb"

            self.cache.vectorset = vectorset
            return vectorset

    async def get_query_vector(self) -> Optional[list[float]]:
        if self.user_vector is not None:
            query_vector = self.user_vector
        else:
            query_info = await self._predict_query_endpoint()
            if query_info is None or query_info.sentence is None:
                return None

            vectorset = await self.get_vectorset()
            if vectorset not in query_info.sentence.vectors:
                logger.warning(
                    "Predict is not responding with a valid query nucliadb vectorset",
                    extra={
                        "kbid": self.kbid,
                        "vectorset": vectorset,
                        "predict_vectorsets": ",".join(query_info.sentence.vectors.keys()),
                    },
                )
                return None

            query_vector = query_info.sentence.vectors[vectorset]

        matryoshka_dimension = await self.get_matryoshka_dimension()
        if matryoshka_dimension is not None:
            if self.user_vector is not None and len(query_vector) < matryoshka_dimension:
                raise InvalidQueryError(
                    "vector",
                    f"Invalid vector length, please check valid embedding size for {vectorset} model",
                )

            # KB using a matryoshka embeddings model, cut the query vector
            # accordingly
            query_vector = query_vector[:matryoshka_dimension]

        return query_vector

    async def get_rephrased_query(self) -> Optional[str]:
        query_info = await self._predict_query_endpoint()
        if query_info is None:
            return None
        return query_info.rephrased_query

    def get_cached_rephrased_query(self) -> Optional[str]:
        if not is_cached(self.cache.predict_query_info):
            return None
        if self.cache.predict_query_info is None:
            return None
        return self.cache.predict_query_info.rephrased_query

    async def get_semantic_min_score(self) -> Optional[float]:
        query_info = await self._predict_query_endpoint()
        if query_info is None:
            return None

        vectorset = await self.get_vectorset()
        min_score = query_info.semantic_thresholds.get(vectorset, None)
        return min_score

    # Labels

    async def get_classification_labels(self) -> knowledgebox_pb2.Labels:
        async with self.locks.setdefault("classification_labels", asyncio.Lock()):
            if is_cached(self.cache.labels):
                return self.cache.labels

            labels = await get_classification_labels(self.kbid)
            self.cache.labels = labels
            return labels

    # Entities

    async def get_detected_entities(self) -> list[utils_pb2.RelationNode]:
        async with self.locks.setdefault("detected_entities", asyncio.Lock()):
            if is_cached(self.cache.detected_entities):
                return self.cache.detected_entities

            # Optimization to avoid calling predict twice
            if is_cached(self.cache.predict_query_info):
                # /query supersets detect entities, so we already have them
                query_info = self.cache.predict_query_info
                if query_info is not None and query_info.entities is not None:
                    detected_entities = convert_relations(query_info.entities.model_dump())
                else:
                    detected_entities = []
            else:
                # No call to /query has been done, we'll use detect entities
                # endpoint instead (as it's faster)
                detected_entities = await self._predict_detect_entities()

            self.cache.detected_entities = detected_entities
            return detected_entities

    # Synonyms

    async def get_synonyms(self) -> Optional[knowledgebox_pb2.Synonyms]:
        async with self.locks.setdefault("synonyms", asyncio.Lock()):
            if is_cached(self.cache.synonyms):
                return self.cache.synonyms

            synonyms = await get_kb_synonyms(self.kbid)
            self.cache.synonyms = synonyms
            return synonyms

    # Generative

    async def get_visual_llm_enabled(self) -> bool:
        query_info = await self._predict_query_endpoint()
        if query_info is None:
            raise SendToPredictError("Error while using predict's query endpoint")

        return query_info.visual_llm

    async def get_max_context_tokens(self, max_tokens: Optional[MaxTokens]) -> int:
        query_info = await self._predict_query_endpoint()
        if query_info is None:
            raise SendToPredictError("Error while using predict's query endpoint")

        model_max = query_info.max_context
        if max_tokens is not None and max_tokens.context is not None:
            if max_tokens.context > model_max:
                raise InvalidQueryError(
                    "max_tokens.context",
                    f"Max context tokens is higher than the model's limit of {model_max}",
                )
            return max_tokens.context
        return model_max

    def get_max_answer_tokens(self, max_tokens: Optional[MaxTokens]) -> Optional[int]:
        if max_tokens is not None and max_tokens.answer is not None:
            return max_tokens.answer
        return None

    # Predict API

    async def _predict_query_endpoint(self) -> Optional[QueryInfo]:
        async with self.locks.setdefault("predict_query_endpoint", asyncio.Lock()):
            if is_cached(self.cache.predict_query_info):
                return self.cache.predict_query_info

            # we can't call get_vectorset, as it would do a recirsive loop between
            # functions, so we'll manually parse it
            vectorset = await self._get_user_vectorset()
            try:
                query_info = await query_information(
                    self.kbid,
                    self.query,
                    vectorset,
                    self.generative_model,
                    self.rephrase,
                    self.rephrase_prompt,
                    self.query_image,
                )
            except (SendToPredictError, TimeoutError):
                query_info = None

            self.cache.predict_query_info = query_info
            return query_info

    async def _predict_detect_entities(self) -> list[utils_pb2.RelationNode]:
        try:
            detected_entities = await detect_entities(self.kbid, self.query)
        except (SendToPredictError, TimeoutError) as ex:
            logger.warning(f"Errors on Predict API detecting entities: {ex}", extra={"kbid": self.kbid})
            detected_entities = []

        return detected_entities

    async def validate_vectorset(self, kbid: str, vectorset: str):
        async with datamanagers.with_ro_transaction() as txn:
            if not await datamanagers.vectorsets.exists(txn, kbid=kbid, vectorset_id=vectorset):
                raise InvalidQueryError(
                    "vectorset", f"Vectorset {vectorset} doesn't exist in you Knowledge Box"
                )


class RAOQueryParser:
    def __init__(self, kbid: str, item: FindRequest, fetcher: RAOFetcher):
        self.kbid = kbid
        self.item = item
        self.fetcher = fetcher

        # cached data while parsing
        self._query: Optional[Query] = None
        self._top_k: Optional[int] = None

    async def parse(self) -> UnitRetrieval:
        self._validate_request()

        self._top_k = self.item.top_k

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

        top_k = self.item.top_k
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

        top_k = self.item.top_k

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


@alru_cache(maxsize=None)
async def get_matryoshka_dimension_cached(kbid: str, vectorset: str) -> Optional[int]:
    # This can be safely cached as the matryoshka dimension is not expected to change
    return await get_matryoshka_dimension(kbid, vectorset)


async def get_matryoshka_dimension(kbid: str, vectorset: Optional[str]) -> Optional[int]:
    # NOTE: When moving /ask to RAO, this will need to change to some http endpoint where we can
    # get the vectorset configuration.
    async with get_driver().ro_transaction() as txn:
        matryoshka_dimension = None
        if not vectorset:
            # XXX this should be migrated once we remove the "default" vectorset
            # concept
            matryoshka_dimension = await datamanagers.kb.get_matryoshka_vector_dimension(txn, kbid=kbid)
        else:
            vectorset_config = await datamanagers.vectorsets.get(txn, kbid=kbid, vectorset_id=vectorset)
            if vectorset_config is not None and vectorset_config.vectorset_index_config.vector_dimension:
                matryoshka_dimension = vectorset_config.vectorset_index_config.vector_dimension

        return matryoshka_dimension


async def get_classification_labels(kbid: str) -> knowledgebox_pb2.Labels:
    # NOTE: When moving /ask to RAO, this will need to change to some http endpoint where we can
    # get the classification labels of the kb.
    async with get_driver().ro_transaction() as txn:
        return await datamanagers.labels.get_labels(txn, kbid=kbid)


async def get_kb_synonyms(kbid: str) -> Optional[knowledgebox_pb2.Synonyms]:
    # NOTE: When moving /ask to RAO, this will need to change to some http endpoint where we can
    # get the kb synonyms.
    async with get_driver().ro_transaction() as txn:
        return await datamanagers.synonyms.get(txn, kbid=kbid)


async def query_information(
    kbid: str,
    query: str,
    semantic_model: Optional[str],
    generative_model: Optional[str] = None,
    rephrase: bool = False,
    rephrase_prompt: Optional[str] = None,
    query_image: Optional[Image] = None,
) -> QueryInfo:
    # NOTE: When moving /ask to RAO, this will need to change to whatever client/utility is used
    # to call NUA predict (internally or externally in the case of onprem).
    predict = get_predict()
    item = QueryModel(
        text=query,
        semantic_models=[semantic_model] if semantic_model else None,
        generative_model=generative_model,
        rephrase=rephrase,
        rephrase_prompt=rephrase_prompt,
        query_image=query_image,
    )
    return await predict.query(kbid, item)


async def detect_entities(kbid: str, query: str) -> list[utils_pb2.RelationNode]:
    # NOTE: When moving /ask to RAO, this will need to change to whatever client/utility is used
    # to call NUA predict (internally or externally in the case of onprem).
    predict = get_predict()
    return await predict.detect_entities(kbid, query)
