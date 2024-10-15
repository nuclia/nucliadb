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
import dataclasses
import functools
import json
from typing import AsyncGenerator, Optional, cast

from pydantic_core import ValidationError

from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.models.responses import HTTPClientError
from nucliadb.search import logger, predict
from nucliadb.search.predict import (
    AnswerStatusCode,
    CitationsGenerativeResponse,
    GenerativeChunk,
    JSONGenerativeResponse,
    MetaGenerativeResponse,
    RephraseMissingContextError,
    RerankGenerativeResponse,
    StatusGenerativeResponse,
    TextGenerativeResponse,
)
from nucliadb.search.search.chat.exceptions import AnswerJsonSchemaTooLong, NoRetrievalResultsError
from nucliadb.search.search.chat.prompt import PromptContextBuilder
from nucliadb.search.search.chat.query import (
    NOT_ENOUGH_CONTEXT_ANSWER,
    ChatAuditor,
    get_find_results,
    get_relations_results,
    rephrase_query,
    sorted_prompt_context_list,
    tokens_to_chars,
)
from nucliadb.search.search.exceptions import (
    IncompleteFindResultsError,
    InvalidQueryError,
)
from nucliadb.search.search.metrics import RAGMetrics
from nucliadb.search.search.query import QueryParser
from nucliadb.search.search.rerankers import RankedItem, apply_reranking, sort_reranked
from nucliadb.search.utilities import get_predict
from nucliadb_models.search import (
    SCORE_TYPE,
    AnswerAskResponseItem,
    AskRequest,
    AskResponseItem,
    AskResponseItemType,
    AskRetrievalMatch,
    AskTimings,
    AskTokens,
    ChatModel,
    ChatOptions,
    CitationsAskResponseItem,
    DebugAskResponseItem,
    ErrorAskResponseItem,
    FindParagraph,
    FindRequest,
    JSONAskResponseItem,
    KnowledgeboxFindResults,
    MetadataAskResponseItem,
    MinScore,
    NucliaDBClientType,
    PrequeriesAskResponseItem,
    PreQueriesStrategy,
    PreQuery,
    PreQueryResult,
    PromptContext,
    PromptContextOrder,
    RagStrategyName,
    Relations,
    RelationsAskResponseItem,
    Reranker,
    RetrievalAskResponseItem,
    SearchOptions,
    StatusAskResponseItem,
    SyncAskMetadata,
    SyncAskResponse,
    UserPrompt,
    parse_custom_prompt,
    parse_rephrase_prompt,
)
from nucliadb_telemetry import errors
from nucliadb_utils.exceptions import LimitsExceededError


@dataclasses.dataclass
class RetrievalMatch:
    paragraph: FindParagraph
    weighted_score: float


@dataclasses.dataclass
class RetrievalResults:
    main_query: KnowledgeboxFindResults
    query_parser: QueryParser
    main_query_weight: float
    prequeries: Optional[list[PreQueryResult]] = None
    best_matches: list[RetrievalMatch] = dataclasses.field(default_factory=list)


class AskResult:
    def __init__(
        self,
        *,
        kbid: str,
        ask_request: AskRequest,
        main_results: KnowledgeboxFindResults,
        prequeries_results: Optional[list[PreQueryResult]],
        nuclia_learning_id: Optional[str],
        predict_answer_stream: AsyncGenerator[GenerativeChunk, None],
        prompt_context: PromptContext,
        prompt_context_order: PromptContextOrder,
        auditor: ChatAuditor,
        metrics: RAGMetrics,
        best_matches: list[RetrievalMatch],
    ):
        # Initial attributes
        self.kbid = kbid
        self.ask_request = ask_request
        self.main_results = main_results
        self.prequeries_results = prequeries_results or []
        self.nuclia_learning_id = nuclia_learning_id
        self.predict_answer_stream = predict_answer_stream
        self.prompt_context = prompt_context
        self.prompt_context_order = prompt_context_order
        self.auditor: ChatAuditor = auditor
        self.metrics: RAGMetrics = metrics
        self.best_matches: list[RetrievalMatch] = best_matches

        # Computed from the predict chat answer stream
        self._answer_text = ""
        self._object: Optional[JSONGenerativeResponse] = None
        self._status: Optional[StatusGenerativeResponse] = None
        self._reranking: Optional[RerankGenerativeResponse] = None
        self._citations: Optional[CitationsGenerativeResponse] = None
        self._metadata: Optional[MetaGenerativeResponse] = None
        self._relations: Optional[Relations] = None

    @property
    def status_code(self) -> AnswerStatusCode:
        if self._status is None:
            return AnswerStatusCode.SUCCESS
        return AnswerStatusCode(self._status.code)

    @property
    def status_error_details(self) -> Optional[str]:
        if self._status is None:  # pragma: no cover
            return None
        return self._status.details

    @property
    def ask_request_with_relations(self) -> bool:
        return ChatOptions.RELATIONS in self.ask_request.features

    @property
    def ask_request_with_debug_flag(self) -> bool:
        return self.ask_request.debug

    async def ndjson_stream(self) -> AsyncGenerator[str, None]:
        try:
            async for item in self._stream():
                yield self._ndjson_encode(item)
        except Exception as exc:
            # Handle any unexpected error that might happen
            # during the streaming and halt the stream
            errors.capture_exception(exc)
            logger.error(
                f"Unexpected error while generating the answer: {exc}",
                extra={"kbid": self.kbid},
            )
            error_message = "Unexpected error while generating the answer. Please try again later."
            if self.ask_request_with_debug_flag:
                error_message += f" Error: {exc}"
            item = ErrorAskResponseItem(error=error_message)
            yield self._ndjson_encode(item)
            return

    def _ndjson_encode(self, item: AskResponseItemType) -> str:
        result_item = AskResponseItem(item=item)
        return result_item.model_dump_json(exclude_none=True) + "\n"

    async def _stream(self) -> AsyncGenerator[AskResponseItemType, None]:
        # First, stream out the predict answer
        first_chunk_yielded = False
        with self.metrics.time("stream_predict_answer"):
            async for answer_chunk in self._stream_predict_answer_text():
                yield AnswerAskResponseItem(text=answer_chunk)
                if not first_chunk_yielded:
                    self.metrics.record_first_chunk_yielded()
                    first_chunk_yielded = True

        if self._object is not None:
            yield JSONAskResponseItem(object=self._object.object)
            if not first_chunk_yielded:
                # When there is a JSON generative response, we consider the first chunk yielded
                # to be the moment when the JSON object is yielded, not the text
                self.metrics.record_first_chunk_yielded()
                first_chunk_yielded = True

        # Then, rerank and cut (if needed) and stream out the retrieval results
        if self._reranking is not None:
            reranked = [
                RankedItem(id=paragraph_id, score=score, score_type=SCORE_TYPE.RERANKER)
                for paragraph_id, score in self._reranking.context_scores.items()
            ]
            sort_reranked(reranked)
            apply_reranking(self.main_results, reranked)

        yield RetrievalAskResponseItem(
            results=self.main_results,
            best_matches=[
                AskRetrievalMatch(
                    id=match.paragraph.id,
                )
                for match in self.best_matches
            ],
        )

        if len(self.prequeries_results) > 0:
            item = PrequeriesAskResponseItem()
            for index, (prequery, result) in enumerate(self.prequeries_results):
                prequery_id = prequery.id or f"prequery_{index}"
                item.results[prequery_id] = result
            yield item

        # Then the status
        if self.status_code == AnswerStatusCode.ERROR:
            # If predict yielded an error status, we yield it too and halt the stream immediately
            yield StatusAskResponseItem(
                code=self.status_code.value,
                status=self.status_code.prettify(),
                details=self.status_error_details or "Unknown error",
            )
            return

        yield StatusAskResponseItem(
            code=self.status_code.value,
            status=self.status_code.prettify(),
        )

        # Audit the answer
        if self._object is None:
            audit_answer = self._answer_text.encode("utf-8")
        else:
            audit_answer = json.dumps(self._object.object).encode("utf-8")

        try:
            rephrase_time = self.metrics.elapsed("rephrase")
        except KeyError:
            # Not all ask requests have a rephrase step
            rephrase_time = None

        self.auditor.audit(
            text_answer=audit_answer,
            generative_answer_time=self.metrics.elapsed("stream_predict_answer"),
            generative_answer_first_chunk_time=self.metrics.get_first_chunk_time() or 0,
            rephrase_time=rephrase_time,
            status_code=self.status_code,
        )

        # Stream out the citations
        if self._citations is not None:
            yield CitationsAskResponseItem(citations=self._citations.citations)

        # Stream out generic metadata about the answer
        if self._metadata is not None:
            yield MetadataAskResponseItem(
                tokens=AskTokens(
                    input=self._metadata.input_tokens,
                    output=self._metadata.output_tokens,
                ),
                timings=AskTimings(
                    generative_first_chunk=self._metadata.timings.get("generative_first_chunk"),
                    generative_total=self._metadata.timings.get("generative"),
                ),
            )

        # Stream out the relations results
        should_query_relations = (
            self.ask_request_with_relations and self.status_code == AnswerStatusCode.SUCCESS
        )
        if should_query_relations:
            relations = await self.get_relations_results()
            yield RelationsAskResponseItem(relations=relations)

        # Stream out debug information
        if self.ask_request_with_debug_flag:
            yield DebugAskResponseItem(
                metadata={
                    "prompt_context": sorted_prompt_context_list(
                        self.prompt_context, self.prompt_context_order
                    )
                }
            )

    async def json(self) -> str:
        # First, run the stream in memory to get all the data in memory
        async for _ in self._stream():
            ...

        metadata = None
        if self._metadata is not None:
            metadata = SyncAskMetadata(
                tokens=AskTokens(
                    input=self._metadata.input_tokens,
                    output=self._metadata.output_tokens,
                ),
                timings=AskTimings(
                    generative_first_chunk=self._metadata.timings.get("generative_first_chunk"),
                    generative_total=self._metadata.timings.get("generative"),
                ),
            )
        citations = {}
        if self._citations is not None:
            citations = self._citations.citations

        answer_json = None
        if self._object is not None:
            answer_json = self._object.object

        prequeries_results: Optional[dict[str, KnowledgeboxFindResults]] = None
        if self.prequeries_results:
            prequeries_results = {}
            for index, (prequery, result) in enumerate(self.prequeries_results):
                prequery_id = prequery.id or f"prequery_{index}"
                prequeries_results[prequery_id] = result

        best_matches = [
            AskRetrievalMatch(
                id=match.paragraph.id,
            )
            for match in self.best_matches
        ]

        response = SyncAskResponse(
            answer=self._answer_text,
            answer_json=answer_json,
            status=self.status_code.prettify(),
            relations=self._relations,
            retrieval_results=self.main_results,
            retrieval_best_matches=best_matches,
            prequeries=prequeries_results,
            citations=citations,
            metadata=metadata,
            learning_id=self.nuclia_learning_id or "",
        )
        if self.status_code == AnswerStatusCode.ERROR and self.status_error_details:
            response.error_details = self.status_error_details
        if self.ask_request_with_debug_flag:
            sorted_prompt_context = sorted_prompt_context_list(
                self.prompt_context, self.prompt_context_order
            )
            response.prompt_context = sorted_prompt_context
        return response.model_dump_json()

    async def get_relations_results(self) -> Relations:
        if self._relations is None:
            with self.metrics.time("relations"):
                self._relations = await get_relations_results(
                    kbid=self.kbid,
                    text_answer=self._answer_text,
                    target_shard_replicas=self.ask_request.shards,
                    timeout=5.0,
                )
        return self._relations

    async def _stream_predict_answer_text(self) -> AsyncGenerator[str, None]:
        """
        Reads the stream of the generative model, yielding the answer text but also parsing
        other items like status codes, citations and miscellaneous metadata.

        This method does not assume any order in the stream of items, but it assumes that at least
        the answer text is streamed in order.
        """
        async for generative_chunk in self.predict_answer_stream:
            item = generative_chunk.chunk
            if isinstance(item, TextGenerativeResponse):
                self._answer_text += item.text
                yield item.text
            elif isinstance(item, JSONGenerativeResponse):
                self._object = item
            elif isinstance(item, StatusGenerativeResponse):
                self._status = item
            elif isinstance(item, RerankGenerativeResponse):
                self._reranking = item
            elif isinstance(item, CitationsGenerativeResponse):
                self._citations = item
            elif isinstance(item, MetaGenerativeResponse):
                self._metadata = item
            else:
                logger.warning(
                    f"Unexpected item in predict answer stream: {item}",
                    extra={"kbid": self.kbid},
                )


class NotEnoughContextAskResult(AskResult):
    def __init__(
        self,
        main_results: Optional[KnowledgeboxFindResults] = None,
        prequeries_results: Optional[list[PreQueryResult]] = None,
    ):
        self.main_results = main_results or KnowledgeboxFindResults(resources={}, min_score=None)
        self.prequeries_results = prequeries_results or []
        self.nuclia_learning_id = None

    async def ndjson_stream(self) -> AsyncGenerator[str, None]:
        """
        In the case where there are no results in the retrieval phase, we simply
        return the find results and the messages indicating that there is not enough
        context in the corpus to answer.
        """
        yield self._ndjson_encode(RetrievalAskResponseItem(results=self.main_results))
        yield self._ndjson_encode(AnswerAskResponseItem(text=NOT_ENOUGH_CONTEXT_ANSWER))
        status = AnswerStatusCode.NO_CONTEXT
        yield self._ndjson_encode(StatusAskResponseItem(code=status.value, status=status.prettify()))

    async def json(self) -> str:
        return SyncAskResponse(
            answer=NOT_ENOUGH_CONTEXT_ANSWER,
            retrieval_results=self.main_results,
            status=AnswerStatusCode.NO_CONTEXT,
        ).model_dump_json()


async def ask(
    *,
    kbid: str,
    ask_request: AskRequest,
    user_id: str,
    client_type: NucliaDBClientType,
    origin: str,
    resource: Optional[str] = None,
) -> AskResult:
    metrics = RAGMetrics()
    chat_history = ask_request.context or []
    user_context = ask_request.extra_context or []
    user_query = ask_request.query

    # Maybe rephrase the query
    rephrased_query = None
    if len(chat_history) > 0 or len(user_context) > 0:
        try:
            with metrics.time("rephrase"):
                rephrased_query = await rephrase_query(
                    kbid,
                    chat_history=chat_history,
                    query=user_query,
                    user_id=user_id,
                    user_context=user_context,
                    generative_model=ask_request.generative_model,
                )
        except RephraseMissingContextError:
            logger.info("Failed to rephrase ask query, using original")

    try:
        retrieval_results = await retrieval_step(
            kbid=kbid,
            # Prefer the rephrased query for retrieval if available
            main_query=rephrased_query or user_query,
            ask_request=ask_request,
            client_type=client_type,
            user_id=user_id,
            origin=origin,
            metrics=metrics,
            resource=resource,
        )
    except NoRetrievalResultsError as err:
        # If a retrieval was attempted but no results were found,
        # early return the ask endpoint without querying the generative model
        return NotEnoughContextAskResult(
            main_results=err.main_query,
            prequeries_results=err.prequeries,
        )

    query_parser = retrieval_results.query_parser

    # Now we build the prompt context
    with metrics.time("context_building"):
        query_parser.max_tokens = ask_request.max_tokens  # type: ignore
        max_tokens_context = await query_parser.get_max_tokens_context()
        prompt_context_builder = PromptContextBuilder(
            kbid=kbid,
            ordered_paragraphs=[match.paragraph for match in retrieval_results.best_matches],
            resource=resource,
            user_context=user_context,
            strategies=ask_request.rag_strategies,
            image_strategies=ask_request.rag_images_strategies,
            max_context_characters=tokens_to_chars(max_tokens_context),
            visual_llm=await query_parser.get_visual_llm_enabled(),
        )
        (
            prompt_context,
            prompt_context_order,
            prompt_context_images,
        ) = await prompt_context_builder.build()

    # Make the chat request to the predict API
    custom_prompt = parse_custom_prompt(ask_request)
    chat_model = ChatModel(
        user_id=user_id,
        system=custom_prompt.system,
        user_prompt=UserPrompt(prompt=custom_prompt.user) if custom_prompt.user else None,
        query_context=prompt_context,
        query_context_order=prompt_context_order,
        chat_history=chat_history,
        question=user_query,
        truncate=True,
        citations=ask_request.citations,
        citation_threshold=ask_request.citation_threshold,
        generative_model=ask_request.generative_model,
        max_tokens=query_parser.get_max_tokens_answer(),
        query_context_images=prompt_context_images,
        json_schema=ask_request.answer_json_schema,
        rerank_context=ask_request.reranker == Reranker.PREDICT_RERANKER,
        top_k=ask_request.top_k,
    )
    with metrics.time("stream_start"):
        predict = get_predict()
        (
            nuclia_learning_id,
            nuclia_learning_model,
            predict_answer_stream,
        ) = await predict.chat_query_ndjson(kbid, chat_model)

    auditor = ChatAuditor(
        kbid=kbid,
        user_id=user_id,
        client_type=client_type,
        origin=origin,
        user_query=user_query,
        rephrased_query=rephrased_query,
        chat_history=chat_history,
        learning_id=nuclia_learning_id,
        query_context=prompt_context,
        query_context_order=prompt_context_order,
        model=nuclia_learning_model,
    )
    return AskResult(
        kbid=kbid,
        ask_request=ask_request,
        main_results=retrieval_results.main_query,
        prequeries_results=retrieval_results.prequeries,
        nuclia_learning_id=nuclia_learning_id,
        predict_answer_stream=predict_answer_stream,  # type: ignore
        prompt_context=prompt_context,
        prompt_context_order=prompt_context_order,
        auditor=auditor,
        metrics=metrics,
        best_matches=retrieval_results.best_matches,
    )


def handled_ask_exceptions(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except KnowledgeBoxNotFound:
            return HTTPClientError(
                status_code=404,
                detail=f"Knowledge Box not found.",
            )
        except LimitsExceededError as exc:
            return HTTPClientError(status_code=exc.status_code, detail=exc.detail)
        except predict.ProxiedPredictAPIError as err:
            return HTTPClientError(
                status_code=err.status,
                detail=err.detail,
            )
        except IncompleteFindResultsError:
            return HTTPClientError(
                status_code=529,
                detail="Temporary error on information retrieval. Please try again.",
            )
        except predict.RephraseMissingContextError:
            return HTTPClientError(
                status_code=412,
                detail="Unable to rephrase the query with the provided context.",
            )
        except predict.RephraseError as err:
            return HTTPClientError(
                status_code=529,
                detail=f"Temporary error while rephrasing the query. Please try again later. Error: {err}",
            )
        except InvalidQueryError as exc:
            return HTTPClientError(status_code=412, detail=str(exc))

    return wrapper


def parse_prequeries(ask_request: AskRequest) -> Optional[PreQueriesStrategy]:
    query_ids = []
    for rag_strategy in ask_request.rag_strategies:
        if rag_strategy.name == RagStrategyName.PREQUERIES:
            prequeries = cast(PreQueriesStrategy, rag_strategy)
            # Give each query a unique id if they don't have one
            for index, query in enumerate(prequeries.queries):
                if query.id is None:
                    query.id = f"prequery_{index}"
                if query.id in query_ids:
                    raise InvalidQueryError(
                        "rag_strategies",
                        "Prequeries must have unique ids",
                    )
                query_ids.append(query.id)
            return prequeries
    return None


async def retrieval_step(
    kbid: str,
    main_query: str,
    ask_request: AskRequest,
    client_type: NucliaDBClientType,
    user_id: str,
    origin: str,
    metrics: RAGMetrics,
    resource: Optional[str] = None,
) -> RetrievalResults:
    """
    This function encapsulates all the logic related to retrieval in the ask endpoint.
    """
    if resource is None:
        return await retrieval_in_kb(
            kbid,
            main_query,
            ask_request,
            client_type,
            user_id,
            origin,
            metrics,
        )
    else:
        return await retrieval_in_resource(
            kbid,
            resource,
            main_query,
            ask_request,
            client_type,
            user_id,
            origin,
            metrics,
        )


async def retrieval_in_kb(
    kbid: str,
    main_query: str,
    ask_request: AskRequest,
    client_type: NucliaDBClientType,
    user_id: str,
    origin: str,
    metrics: RAGMetrics,
) -> RetrievalResults:
    prequeries = parse_prequeries(ask_request)
    with metrics.time("retrieval"):
        main_results, prequeries_results, query_parser = await get_find_results(
            kbid=kbid,
            query=main_query,
            item=ask_request,
            ndb_client=client_type,
            user=user_id,
            origin=origin,
            metrics=metrics,
            prequeries_strategy=prequeries,
        )
        if len(main_results.resources) == 0 and all(
            len(prequery_result.resources) == 0 for (_, prequery_result) in prequeries_results or []
        ):
            raise NoRetrievalResultsError(main_results, prequeries_results)

    main_query_weight = prequeries.main_query_weight if prequeries is not None else 1.0
    best_matches = compute_best_matches(
        main_results=main_results,
        prequeries_results=prequeries_results,
        main_query_weight=main_query_weight,
    )
    return RetrievalResults(
        main_query=main_results,
        prequeries=prequeries_results,
        query_parser=query_parser,
        main_query_weight=main_query_weight,
        best_matches=best_matches,
    )


async def retrieval_in_resource(
    kbid: str,
    resource: str,
    main_query: str,
    ask_request: AskRequest,
    client_type: NucliaDBClientType,
    user_id: str,
    origin: str,
    metrics: RAGMetrics,
) -> RetrievalResults:
    if any(strategy.name == "full_resource" for strategy in ask_request.rag_strategies):
        # Retrieval is not needed if we are chatting on a specific resource and the full_resource strategy is enabled
        return RetrievalResults(
            main_query=KnowledgeboxFindResults(resources={}, min_score=None),
            prequeries=None,
            query_parser=QueryParser(
                kbid=kbid,
                features=[],
                query="",
                label_filters=ask_request.filters,
                keyword_filters=ask_request.keyword_filters,
                page_number=0,
                page_size=0,
                min_score=MinScore(),
            ),
            main_query_weight=1.0,
        )

    prequeries = parse_prequeries(ask_request)
    if prequeries is None and ask_request.answer_json_schema is not None and main_query == "":
        prequeries = calculate_prequeries_for_json_schema(ask_request)

    # Make sure the retrieval is scoped to the resource if provided
    ask_request.resource_filters = [resource]
    if prequeries is not None:
        for prequery in prequeries.queries:
            if prequery.prefilter is True:
                raise InvalidQueryError(
                    "rag_strategies",
                    "Prequeries with prefilter are not supported when asking on a resource",
                )
            prequery.request.resource_filters = [resource]

    with metrics.time("retrieval"):
        main_results, prequeries_results, query_parser = await get_find_results(
            kbid=kbid,
            query=main_query,
            item=ask_request,
            ndb_client=client_type,
            user=user_id,
            origin=origin,
            metrics=metrics,
            prequeries_strategy=prequeries,
        )
        if len(main_results.resources) == 0 and all(
            len(prequery_result.resources) == 0 for (_, prequery_result) in prequeries_results or []
        ):
            raise NoRetrievalResultsError(main_results, prequeries_results)
    main_query_weight = prequeries.main_query_weight if prequeries is not None else 1.0
    best_matches = compute_best_matches(
        main_results=main_results,
        prequeries_results=prequeries_results,
        main_query_weight=main_query_weight,
    )
    return RetrievalResults(
        main_query=main_results,
        prequeries=prequeries_results,
        query_parser=query_parser,
        main_query_weight=main_query_weight,
        best_matches=best_matches,
    )


def compute_best_matches(
    main_results: KnowledgeboxFindResults,
    prequeries_results: Optional[list[PreQueryResult]] = None,
    main_query_weight: float = 1.0,
) -> list[RetrievalMatch]:
    """
    Returns the list of matches of the retrieval results, ordered by relevance (descending weighted score).

    If prequeries_results is provided, the paragraphs of the prequeries are weighted according to the
    normalized weight of the prequery. The paragraph score is not modified, but it is used to determine the order in which they
    are presented in the LLM prompt context.

    If a paragraph is matched in various prequeries, the final weighted score is the sum of the weighted scores for each prequery.

    `main_query_weight` is the weight given to the paragraphs matching the main query when calculating the final score.
    """

    def iter_paragraphs(results: KnowledgeboxFindResults):
        for resource in results.resources.values():
            for field in resource.fields.values():
                for paragraph in field.paragraphs.values():
                    yield paragraph

    total_weights = main_query_weight + sum(prequery.weight for prequery, _ in prequeries_results or [])
    paragraph_id_to_match: dict[str, RetrievalMatch] = {}
    for paragraph in iter_paragraphs(main_results):
        normalized_weight = main_query_weight / total_weights
        rmatch = RetrievalMatch(
            paragraph=paragraph,
            weighted_score=paragraph.score * normalized_weight,
        )
        paragraph_id_to_match[paragraph.id] = rmatch

    for prequery, prequery_results in prequeries_results or []:
        for paragraph in iter_paragraphs(prequery_results):
            normalized_weight = prequery.weight / total_weights
            weighted_score = paragraph.score * normalized_weight
            if paragraph.id in paragraph_id_to_match:
                rmatch = paragraph_id_to_match[paragraph.id]
                # If a paragraph is matched in various prequeries, the final score is the
                # sum of the weighted scores
                rmatch.weighted_score += weighted_score
            else:
                paragraph_id_to_match[paragraph.id] = RetrievalMatch(
                    paragraph=paragraph,
                    weighted_score=weighted_score,
                )

    return sorted(paragraph_id_to_match.values(), key=lambda match: match.weighted_score, reverse=True)


def calculate_prequeries_for_json_schema(ask_request: AskRequest) -> Optional[PreQueriesStrategy]:
    """
    This function generates a PreQueriesStrategy with a query for each property in the JSON schema
    found in ask_request.answer_json_schema.

    This is useful for the use-case where the user is asking for a structured answer on a corpus
    that is too big to send to the generative model.

    For instance, a JSON schema like this:
    {
        "name": "book_ordering",
        "description": "Structured answer for a book to order",
        "parameters": {
            "type": "object",
            "properties": {
                "title": {
                    "type": "string",
                    "description": "The title of the book"
                },
                "author": {
                    "type": "string",
                    "description": "The author of the book"
                },
            },
            "required": ["title", "author"]
        }
    }
    Will generate a PreQueriesStrategy with 2 queries, one for each property in the JSON schema, with equal weights
    [
        PreQuery(request=FindRequest(query="The title of the book", ...), weight=1.0),
        PreQuery(request=FindRequest(query="The author of the book", ...), weight=1.0),
    ]
    """
    prequeries: list[PreQuery] = []
    json_schema = ask_request.answer_json_schema or {}
    features = []
    if ChatOptions.SEMANTIC in ask_request.features:
        features.append(SearchOptions.SEMANTIC)
    if ChatOptions.KEYWORD in ask_request.features:
        features.append(SearchOptions.KEYWORD)

    properties = json_schema.get("parameters", {}).get("properties", {})
    if len(properties) == 0:  # pragma: no cover
        return None
    for prop_name, prop_def in properties.items():
        query = prop_name
        if prop_def.get("description"):
            query += f": {prop_def['description']}"
        req = FindRequest(
            query=query,
            features=features,
            filters=[],
            keyword_filters=[],
            page_number=0,
            page_size=10,
            min_score=ask_request.min_score,
            vectorset=ask_request.vectorset,
            highlight=False,
            debug=False,
            show=[],
            with_duplicates=False,
            with_synonyms=False,
            resource_filters=[],  # to be filled with the resource filter
            rephrase=ask_request.rephrase,
            rephrase_prompt=parse_rephrase_prompt(ask_request),
            security=ask_request.security,
            autofilter=False,
        )
        prequery = PreQuery(
            request=req,
            weight=1.0,
        )
        prequeries.append(prequery)
    try:
        strategy = PreQueriesStrategy(queries=prequeries)
    except ValidationError:
        raise AnswerJsonSchemaTooLong(
            "Answer JSON schema with too many properties generated too many prequeries"
        )

    ask_request.rag_strategies = [strategy]
    return strategy
