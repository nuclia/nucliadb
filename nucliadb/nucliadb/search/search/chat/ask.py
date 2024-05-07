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
from time import monotonic as time
from typing import Any, AsyncGenerator, Optional

from nucliadb.search import logger
from nucliadb.search.predict import AnswerStatusCode
from nucliadb.search.search.chat.prompt import PromptContextBuilder
from nucliadb.search.search.chat.query import (
    get_find_results,
    get_relations_results,
    maybe_audit_chat,
    rephrase_query,
    tokens_to_chars,
)
from nucliadb.search.search.query import QueryParser
from nucliadb.search.utilities import get_predict
from nucliadb_models.search import (
    AskRequest,
    AskResultItem,
    ChatModel,
    ChatOptions,
    CitationsAskResponseItem,
    FindResultsAskResponseItem,
    KnowledgeboxFindResults,
    MinScore,
    NucliaDBClientType,
    PromptContext,
    PromptContextOrder,
    StatusAskResponseItem,
    TextAskResponseItem,
    UserPrompt,
)


class AskResult:
    def __init__(
        self,
        *,
        ask_request: AskRequest,
        find_results: KnowledgeboxFindResults,
        nuclia_learning_id: Optional[str],
        user_id: str,
        client_type: NucliaDBClientType,
        origin: str,
        start_time: float,
        user_query: str,
        rephrased_query: Optional[str],
        predict_answer_stream: AsyncGenerator,
        prompt_context: PromptContext,
        prompt_context_order: PromptContextOrder,
    ):
        # Initial attributes
        self.ask_request = ask_request
        self.find_results = find_results
        self.nuclia_learning_id = nuclia_learning_id
        self.predict_answer_stream = predict_answer_stream
        self.prompt_context = prompt_context
        self.prompt_context_order = prompt_context_order

        # Computed from the predict answer stream
        self._answer_text = ""
        self._status_code = AnswerStatusCode.NO_CONTEXT
        self._citations = None
        self._relations_results = None
        self._relations_task = None

    @property
    def status_code(self) -> AnswerStatusCode:
        return self._status_code

    @property
    def citations(self) -> dict[str, Any]:
        return self._citations or {}

    @property
    def ask_request_with_relations(self) -> bool:
        return ChatOptions.RELATIONS in self.ask_request.features

    @property
    def ask_request_with_citations(self) -> bool:
        return self.ask_request.citations

    async def stream(self) -> AsyncGenerator[AskResultItem, None]:
        # First stream out the find results
        yield AskResultItem(item=FindResultsAskResponseItem(results=self.find_results))

        # Then stream out the predict answer
        async for answer_chunk in self._stream_predict_answer_text():
            yield AskResultItem(item=TextAskResponseItem(text=answer_chunk))

        # Schedule the relations results if needed
        relations_needed = (
            self.ask_request_with_relations
            and self.status_code != AnswerStatusCode.NO_CONTEXT
        )
        if relations_needed:
            # Schedule the relations query as soon as possible
            self._schedule_relations_results()

        # Then the status code
        yield AskResultItem(item=StatusAskResponseItem(code=self.status_code))

        await maybe_audit_chat(
            kbid=self.ask_request.kbid,
            user_id=self.user_id,
            client_type=self.client_type,
            origin=self.origin,
            duration=time() - self.start_time,
            user_query=self.user_query,
            rephrased_query=self.rephrased_query,
            text_answer=self.answer_text,
            status_code=self.status_code.value,
            chat_history=self.ask_request.chat_history,
            query_context=self.prompt_context,
            learning_id=self.nuclia_learning_id,
        )

        # Stream out the citations
        if self.ask_request_with_citations:
            yield AskResultItem(item=CitationsAskResponseItem(citations=self.citations))

        # Stream out the relations results
        if relations_needed:
            relations_item = await self.get_relations_results_item()
            yield AskResultItem(item=relations_item)

    def _schedule_relations_results(self):
        if self._relations_task is None:
            self._relations_task = asyncio.create_task(
                get_relations_results(
                    kbid=self.ask_request.kbid,
                    chat_request=self.ask_request,
                    text_answer=self._answer_text,
                )
            )

    async def _stream_predict_answer_text(self) -> AsyncGenerator[str, None]:
        async for item in self.predict_answer_stream:
            if isinstance(item, TextAskResponseItem):
                # Accumulate the answer text. We assume that the answer
                # is streamed in order from the server
                self._answer_text += item.text
                yield item.text
            # Status code and citations are not part of the answer text but
            # we need to parse them while streaming the answer
            elif isinstance(item, StatusAskResponseItem):
                self._status_code = item.code
                continue
            elif isinstance(item, CitationsAskResponseItem):
                self._citations = item.citations
                continue
            else:
                logger.warning(f"Unexpected item in predict answer stream: {item}")
                continue


class NotEnoughContextResult:
    def __init__(
        self,
        find_results: KnowledgeboxFindResults,
    ):
        self.find_results = find_results

    async def stream(self):
        yield AskResultItem(item=FindResultsAskResponseItem(results=self.find_results))
        yield AskResultItem(
            item=TextAskResponseItem(text="Not enough context to answer.")
        )
        yield AskResultItem(
            item=StatusAskResponseItem(status=AnswerStatusCode.NO_CONTEXT)
        )


async def ask(
    kbid: str,
    ask_request: AskRequest,
    user_id: str,
    client_type: NucliaDBClientType,
    origin: str,
    resource: Optional[str] = None,
) -> AskResult:
    start_time = time()
    nuclia_learning_id: Optional[str] = None
    chat_history = ask_request.context or []
    user_context = ask_request.extra_context or []
    user_query = ask_request.query
    rephrased_query = None
    prompt_context: PromptContext = {}
    prompt_context_order: PromptContextOrder = {}

    if len(chat_history) > 0 or len(user_context) > 0:

        rephrased_query = await rephrase_query(
            kbid,
            chat_history=chat_history,
            query=user_query,
            user_id=user_id,
            user_context=user_context,
            generative_model=ask_request.generative_model,
        )

    # Retrieval is not needed if we are chatting on a specific
    # resource and the full_resource strategy is enabled
    needs_retrieval = True
    if resource is not None:
        ask_request.resource_filters = [resource]
        if any(
            strategy.name == "full_resource" for strategy in ask_request.rag_strategies
        ):
            needs_retrieval = False

    if needs_retrieval:
        find_results, query_parser = await get_find_results(
            kbid=kbid,
            query=rephrased_query or user_query,
            chat_request=ask_request,
            ndb_client=client_type,
            user=user_id,
            origin=origin,
        )

        if len(find_results.resources) == 0:
            return NotEnoughContextResult()

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

    user_prompt = None
    if ask_request.prompt is not None:
        user_prompt = UserPrompt(prompt=ask_request.prompt)

    chat_model = ChatModel(
        user_id=user_id,
        query_context=prompt_context,
        query_context_order=prompt_context_order,
        chat_history=chat_history,
        question=user_query,
        truncate=True,
        user_prompt=user_prompt,
        citations=ask_request.citations,
        generative_model=ask_request.generative_model,
        max_tokens=query_parser.get_max_tokens_answer(),
        query_context_images=prompt_context_images,
    )

    predict = get_predict()
    nuclia_learning_id, predict_generator = await predict.chat_query_v2(
        kbid, chat_model
    )

    return AskResult(
        ask_request=ask_request,
        find_results=find_results,
        nuclia_learning_id=nuclia_learning_id,
        user_id=user_id,
        client_type=client_type,
        origin=origin,
        start_time=start_time,
        user_query=user_query,
        rephrased_query=rephrased_query,
        predict_answer_stream=predict_generator,
        prompt_context=prompt_context,
        prompt_context_order=prompt_context_order,
    )
