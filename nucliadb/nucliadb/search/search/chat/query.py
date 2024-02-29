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
from dataclasses import dataclass
from time import monotonic as time
from typing import AsyncGenerator, AsyncIterator, Optional

from nucliadb_protos.nodereader_pb2 import RelationSearchRequest, RelationSearchResponse

from nucliadb.search import logger
from nucliadb.search.predict import AnswerStatusCode
from nucliadb.search.requesters.utils import Method, node_query
from nucliadb.search.search.chat.prompt import PromptContextBuilder
from nucliadb.search.search.exceptions import IncompleteFindResultsError
from nucliadb.search.search.find import find
from nucliadb.search.search.merge import merge_relations_results
from nucliadb.search.settings import settings
from nucliadb.search.utilities import get_predict
from nucliadb_models.search import (
    Author,
    ChatContextMessage,
    ChatModel,
    ChatOptions,
    ChatRequest,
    FindRequest,
    KnowledgeboxFindResults,
    NucliaDBClientType,
    PromptContext,
    PromptContextOrder,
    Relations,
    RephraseModel,
    SearchOptions,
    UserPrompt,
)
from nucliadb_protos import audit_pb2
from nucliadb_telemetry.errors import capture_exception
from nucliadb_utils.helpers import async_gen_lookahead
from nucliadb_utils.utilities import get_audit

NOT_ENOUGH_CONTEXT_ANSWER = "Not enough data to answer this."
AUDIT_TEXT_RESULT_SEP = " \n\n "
START_OF_CITATIONS = b"_CIT_"


class FoundStatusCode:
    def __init__(self, default: AnswerStatusCode = AnswerStatusCode.SUCCESS):
        self._value = AnswerStatusCode.SUCCESS

    def set(self, value: AnswerStatusCode) -> None:
        self._value = value

    @property
    def value(self) -> AnswerStatusCode:
        return self._value


@dataclass
class ChatResult:
    nuclia_learning_id: Optional[str]
    answer_stream: AsyncIterator[bytes]
    status_code: FoundStatusCode
    find_results: KnowledgeboxFindResults
    prompt_context: PromptContext
    prompt_context_order: PromptContextOrder


async def rephrase_query(
    kbid: str,
    chat_history: list[ChatContextMessage],
    query: str,
    user_id: str,
    user_context: list[str],
    generative_model: Optional[str] = None,
) -> str:
    predict = get_predict()
    req = RephraseModel(
        question=query,
        chat_history=chat_history,
        user_id=user_id,
        user_context=user_context,
        generative_model=generative_model,
    )
    return await predict.rephrase_query(kbid, req)


async def format_generated_answer(
    answer_generator: AsyncGenerator[bytes, None], output_status_code: FoundStatusCode
):
    status_code: Optional[AnswerStatusCode] = None
    is_last_chunk = False
    async for answer_chunk, is_last_chunk in async_gen_lookahead(answer_generator):
        if is_last_chunk:
            try:
                status_code = _parse_answer_status_code(answer_chunk)
            except ValueError:
                # TODO: remove this in the future, it's
                # just for bw compatibility until predict
                # is updated to the new protocol
                status_code = AnswerStatusCode.SUCCESS
                yield answer_chunk
            else:
                # TODO: this should be needed but, in case we receive the status
                # code mixed with text, we strip it and return the text
                if len(answer_chunk) != len(status_code.encode()):
                    answer_chunk = answer_chunk.rstrip(status_code.encode())
                    yield answer_chunk
            break
        yield answer_chunk
    if not is_last_chunk:
        logger.warning("BUG: /chat endpoint without last chunk")

    output_status_code.set(status_code or AnswerStatusCode.SUCCESS)


async def get_find_results(
    *,
    kbid: str,
    query: str,
    chat_request: ChatRequest,
    ndb_client: NucliaDBClientType,
    user: str,
    origin: str,
) -> KnowledgeboxFindResults:
    find_request = FindRequest()
    find_request.resource_filters = chat_request.resource_filters
    find_request.features = []
    if ChatOptions.VECTORS in chat_request.features:
        find_request.features.append(SearchOptions.VECTOR)
    if ChatOptions.PARAGRAPHS in chat_request.features:
        find_request.features.append(SearchOptions.PARAGRAPH)
    if ChatOptions.RELATIONS in chat_request.features:
        find_request.features.append(SearchOptions.RELATIONS)
    find_request.query = query
    find_request.fields = chat_request.fields
    find_request.filters = chat_request.filters
    find_request.field_type_filter = chat_request.field_type_filter
    find_request.min_score = chat_request.min_score
    find_request.range_creation_start = chat_request.range_creation_start
    find_request.range_creation_end = chat_request.range_creation_end
    find_request.range_modification_start = chat_request.range_modification_start
    find_request.range_modification_end = chat_request.range_modification_end
    find_request.show = chat_request.show
    find_request.extracted = chat_request.extracted
    find_request.shards = chat_request.shards
    find_request.autofilter = chat_request.autofilter
    find_request.highlight = chat_request.highlight
    find_request.security = chat_request.security
    find_request.debug = chat_request.debug

    find_results, incomplete = await find(kbid, find_request, ndb_client, user, origin)
    if incomplete:
        raise IncompleteFindResultsError()
    return find_results


async def get_relations_results(
    *, kbid: str, chat_request: ChatRequest, text_answer: str
) -> Relations:
    try:
        predict = get_predict()
        detected_entities = await predict.detect_entities(kbid, text_answer)
        relation_request = RelationSearchRequest()
        relation_request.subgraph.entry_points.extend(detected_entities)
        relation_request.subgraph.depth = 1

        relations_results: list[RelationSearchResponse]
        (
            relations_results,
            _,
            _,
        ) = await node_query(
            kbid,
            Method.RELATIONS,
            relation_request,
            target_shard_replicas=chat_request.shards,
        )
        return await merge_relations_results(
            relations_results, relation_request.subgraph
        )
    except Exception as exc:
        capture_exception(exc)
        logger.exception("Error getting relations results")
        return Relations(entities={})


async def not_enough_context_generator():
    await asyncio.sleep(0)
    yield NOT_ENOUGH_CONTEXT_ANSWER.encode()
    yield AnswerStatusCode.NO_CONTEXT.encode()


async def chat(
    kbid: str,
    chat_request: ChatRequest,
    user_id: str,
    client_type: NucliaDBClientType,
    origin: str,
) -> ChatResult:
    start_time = time()
    nuclia_learning_id: Optional[str] = None
    chat_history = chat_request.context or []
    user_context = chat_request.extra_context or []
    user_query = chat_request.query
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
            generative_model=chat_request.generative_model,
        )

    find_results: KnowledgeboxFindResults = await get_find_results(
        kbid=kbid,
        query=rephrased_query or user_query,
        chat_request=chat_request,
        ndb_client=client_type,
        user=user_id,
        origin=origin,
    )

    status_code = FoundStatusCode()
    if len(find_results.resources) == 0:
        answer_stream = format_generated_answer(
            not_enough_context_generator(), status_code
        )
    else:
        prompt_context_builder = PromptContextBuilder(
            kbid=kbid,
            find_results=find_results,
            user_context=user_context,
            strategies=chat_request.rag_strategies,
            max_context_size=settings.max_prompt_context_chars,
        )
        prompt_context, prompt_context_order = await prompt_context_builder.build()
        user_prompt = None
        if chat_request.prompt is not None:
            user_prompt = UserPrompt(prompt=chat_request.prompt)

        chat_model = ChatModel(
            user_id=user_id,
            query_context=prompt_context,
            query_context_order=prompt_context_order,
            chat_history=chat_history,
            question=user_query,
            truncate=True,
            user_prompt=user_prompt,
            citations=chat_request.citations,
            generative_model=chat_request.generative_model,
        )
        predict = get_predict()
        nuclia_learning_id, predict_generator = await predict.chat_query(
            kbid, chat_model
        )

        async def _wrapped_stream():
            # so we can audit after streamed out answer
            text_answer = b""
            async for chunk in format_generated_answer(predict_generator, status_code):
                text_answer += chunk
                yield chunk

            await maybe_audit_chat(
                kbid=kbid,
                user_id=user_id,
                client_type=client_type,
                origin=origin,
                duration=time() - start_time,
                user_query=user_query,
                rephrased_query=rephrased_query,
                text_answer=text_answer,
                status_code=status_code.value,
                chat_history=chat_history,
                query_context=prompt_context,
                learning_id=nuclia_learning_id,
            )

        answer_stream = _wrapped_stream()

    return ChatResult(
        nuclia_learning_id=nuclia_learning_id,
        answer_stream=answer_stream,
        status_code=status_code,
        find_results=find_results,
        prompt_context=prompt_context,
        prompt_context_order=prompt_context_order,
    )


def _parse_answer_status_code(chunk: bytes) -> AnswerStatusCode:
    """
    Parses the status code from the last chunk of the answer.
    """
    try:
        return AnswerStatusCode(chunk.decode())
    except ValueError:
        # In some cases, even if the status code was yield separately
        # at the server side, the status code is appended to the previous chunk...
        # It may be a bug in the aiohttp.StreamResponse implementation,
        # but we haven't spotted it yet. For now, we just try to parse the status code
        # from the tail of the chunk.
        logger.debug(
            f"Error decoding status code from /chat's last chunk. Chunk: {chunk!r}"
        )
        if chunk == b"":
            raise
        if chunk.endswith(b"0"):
            return AnswerStatusCode.SUCCESS
        return AnswerStatusCode(chunk[-2:].decode())


async def maybe_audit_chat(
    *,
    kbid: str,
    user_id: str,
    client_type: NucliaDBClientType,
    origin: str,
    duration: float,
    user_query: str,
    rephrased_query: Optional[str],
    text_answer: bytes,
    status_code: Optional[AnswerStatusCode],
    chat_history: list[ChatContextMessage],
    query_context: list[str],
    learning_id: str,
):
    audit = get_audit()
    if audit is None:
        return

    audit_answer = parse_audit_answer(text_answer, status_code)

    # Append chat history and query context
    audit_context = [
        audit_pb2.ChatContext(author=message.author, text=message.text)
        for message in chat_history
    ]
    audit_context.append(
        audit_pb2.ChatContext(
            author=Author.NUCLIA,
            text=AUDIT_TEXT_RESULT_SEP.join(query_context),
        )
    )
    await audit.chat(
        kbid,
        user_id,
        client_type.to_proto(),
        origin,
        duration,
        question=user_query,
        rephrased_question=rephrased_query,
        context=audit_context,
        answer=audit_answer,
        learning_id=learning_id,
    )


def parse_audit_answer(
    raw_text_answer: bytes, status_code: Optional[AnswerStatusCode]
) -> Optional[str]:
    if status_code == AnswerStatusCode.NO_CONTEXT:
        # We don't want to audit "Not enough context to answer this." and instead set a None.
        return None
    # Split citations part from answer
    try:
        raw_audit_answer, _ = raw_text_answer.split(START_OF_CITATIONS)
    except ValueError:
        raw_audit_answer = raw_text_answer
    audit_answer = raw_audit_answer.decode()
    return audit_answer
