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
import functools
import json
from typing import AsyncGenerator, Optional

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
    StatusGenerativeResponse,
    TextGenerativeResponse,
)
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
from nucliadb.search.utilities import get_predict
from nucliadb_models.search import (
    AnswerAskResponseItem,
    AskRequest,
    AskResponseItem,
    AskResponseItemType,
    AskTimings,
    AskTokens,
    ChatModel,
    ChatOptions,
    CitationsAskResponseItem,
    DebugAskResponseItem,
    ErrorAskResponseItem,
    JSONAskResponseItem,
    KnowledgeboxFindResults,
    MetadataAskResponseItem,
    MinScore,
    NucliaDBClientType,
    PromptContext,
    PromptContextOrder,
    Relations,
    RelationsAskResponseItem,
    RetrievalAskResponseItem,
    StatusAskResponseItem,
    SyncAskMetadata,
    SyncAskResponse,
    UserPrompt,
    parse_custom_prompt,
)
from nucliadb_telemetry import errors
from nucliadb_utils.exceptions import LimitsExceededError


class AskResult:
    def __init__(
        self,
        *,
        kbid: str,
        ask_request: AskRequest,
        find_results: KnowledgeboxFindResults,
        nuclia_learning_id: Optional[str],
        predict_answer_stream: AsyncGenerator[GenerativeChunk, None],
        prompt_context: PromptContext,
        prompt_context_order: PromptContextOrder,
        auditor: ChatAuditor,
        metrics: RAGMetrics,
    ):
        # Initial attributes
        self.kbid = kbid
        self.ask_request = ask_request
        self.find_results = find_results
        self.nuclia_learning_id = nuclia_learning_id
        self.predict_answer_stream = predict_answer_stream
        self.prompt_context = prompt_context
        self.prompt_context_order = prompt_context_order
        self.auditor: ChatAuditor = auditor
        self.metrics: RAGMetrics = metrics

        # Computed from the predict chat answer stream
        self._answer_text = ""
        self._object: Optional[JSONGenerativeResponse] = None
        self._status: Optional[StatusGenerativeResponse] = None
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
        if self._status is None:
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
        return result_item.model_dump_json(exclude_unset=False, exclude_none=True) + "\n"

    async def _stream(self) -> AsyncGenerator[AskResponseItemType, None]:
        # First stream out the find results
        yield RetrievalAskResponseItem(results=self.find_results)

        # Then stream out the predict answer
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

        # Stream out other metadata about the answer if available
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

        response = SyncAskResponse(
            answer=self._answer_text,
            answer_json=answer_json,
            status=self.status_code.prettify(),
            relations=self._relations,
            retrieval_results=self.find_results,
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
        return response.model_dump_json(exclude_unset=True)

    async def get_relations_results(self) -> Relations:
        if self._relations is None:
            with self.metrics.time("relations"):
                self._relations = await get_relations_results(
                    kbid=self.kbid,
                    text_answer=self._answer_text,
                    target_shard_replicas=self.ask_request.shards,
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
        find_results: KnowledgeboxFindResults,
    ):
        self.find_results = find_results
        self.nuclia_learning_id = None

    async def ndjson_stream(self) -> AsyncGenerator[str, None]:
        """
        In the case where there are no results in the retrieval phase, we simply
        return the find results and the messages indicating that there is not enough
        context in the corpus to answer.
        """
        yield self._ndjson_encode(RetrievalAskResponseItem(results=self.find_results))
        yield self._ndjson_encode(AnswerAskResponseItem(text=NOT_ENOUGH_CONTEXT_ANSWER))
        status = AnswerStatusCode.NO_CONTEXT
        yield self._ndjson_encode(StatusAskResponseItem(code=status.value, status=status.prettify()))

    async def json(self) -> str:
        return SyncAskResponse(
            answer=NOT_ENOUGH_CONTEXT_ANSWER,
            retrieval_results=self.find_results,
            status=AnswerStatusCode.NO_CONTEXT,
        ).model_dump_json(exclude_unset=True)


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

    # Retrieval is not needed if we are chatting on a specific
    # resource and the full_resource strategy is enabled
    needs_retrieval = True
    if resource is not None:
        ask_request.resource_filters = [resource]
        if any(strategy.name == "full_resource" for strategy in ask_request.rag_strategies):
            needs_retrieval = False

    # Maybe do a retrieval query
    if needs_retrieval:
        with metrics.time("retrieval"):
            find_results, query_parser = await get_find_results(
                kbid=kbid,
                # Prefer the rephrased query if available
                query=rephrased_query or user_query,
                chat_request=ask_request,
                ndb_client=client_type,
                user=user_id,
                origin=origin,
                metrics=metrics,
            )
        if len(find_results.resources) == 0:
            return NotEnoughContextAskResult(find_results=find_results)

    else:
        find_results = KnowledgeboxFindResults(resources={}, min_score=None)
        query_parser = QueryParser(
            kbid=kbid,
            features=[],
            query="",
            filters=ask_request.filters,
            page_number=0,
            page_size=0,
            min_score=MinScore(),
        )

    # Now we build the prompt context
    with metrics.time("context_building"):
        query_parser.max_tokens = ask_request.max_tokens  # type: ignore
        max_tokens_context = await query_parser.get_max_tokens_context()
        prompt_context_builder = PromptContextBuilder(
            kbid=kbid,
            find_results=find_results,
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

    custom_prompt = parse_custom_prompt(ask_request)

    # Make the chat request to the predict API
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
        generative_model=ask_request.generative_model,
        max_tokens=query_parser.get_max_tokens_answer(),
        query_context_images=prompt_context_images,
        json_schema=ask_request.answer_json_schema,
    )
    with metrics.time("stream_start"):
        predict = get_predict()
        nuclia_learning_id, predict_answer_stream = await predict.chat_query_ndjson(kbid, chat_model)

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
    )
    return AskResult(
        kbid=kbid,
        ask_request=ask_request,
        find_results=find_results,
        nuclia_learning_id=nuclia_learning_id,
        predict_answer_stream=predict_answer_stream,  # type: ignore
        prompt_context=prompt_context,
        prompt_context_order=prompt_context_order,
        auditor=auditor,
        metrics=metrics,
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
