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
from time import monotonic as time
from typing import Optional

from nucliadb_protos.nodereader_pb2 import RelationSearchRequest, RelationSearchResponse

from nucliadb.search import logger
from nucliadb.search.predict import AnswerStatusCode
from nucliadb.search.requesters.utils import Method, node_query
from nucliadb.search.search.exceptions import IncompleteFindResultsError
from nucliadb.search.search.find import find
from nucliadb.search.search.merge import merge_relations_results
from nucliadb.search.search.query import QueryParser
from nucliadb.search.utilities import get_predict
from nucliadb_models.search import (
    Author,
    ChatContextMessage,
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
)
from nucliadb_protos import audit_pb2
from nucliadb_telemetry.errors import capture_exception
from nucliadb_utils.utilities import get_audit

NOT_ENOUGH_CONTEXT_ANSWER = "Not enough data to answer this."
AUDIT_TEXT_RESULT_SEP = " \n\n "
START_OF_CITATIONS = b"_CIT_"


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


async def get_find_results(
    *,
    kbid: str,
    query: str,
    chat_request: ChatRequest,
    ndb_client: NucliaDBClientType,
    user: str,
    origin: str,
) -> tuple[KnowledgeboxFindResults, QueryParser]:
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
    find_request.rephrase = chat_request.rephrase

    find_results, incomplete, query_parser = await find(
        kbid,
        find_request,
        ndb_client,
        user,
        origin,
        generative_model=chat_request.generative_model,
    )
    if incomplete:
        raise IncompleteFindResultsError()
    return find_results, query_parser


async def get_relations_results(
    *, kbid: str, text_answer: str, target_shard_replicas: Optional[list[str]]
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
            target_shard_replicas=target_shard_replicas,
        )
        return await merge_relations_results(
            relations_results, relation_request.subgraph
        )
    except Exception as exc:
        capture_exception(exc)
        logger.exception("Error getting relations results")
        return Relations(entities={})


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
    query_context: PromptContext,
    query_context_order: PromptContextOrder,
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
    query_context_paragaph_ids = list(query_context.keys())
    audit_context.append(
        audit_pb2.ChatContext(
            author=Author.NUCLIA,
            text=AUDIT_TEXT_RESULT_SEP.join(query_context_paragaph_ids),
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
        start_time: float,
        user_query: str,
        rephrased_query: Optional[str],
        chat_history: list[ChatContextMessage],
        learning_id: Optional[str],
        query_context: PromptContext,
        query_context_order: PromptContextOrder,
    ):
        self.kbid = kbid
        self.user_id = user_id
        self.client_type = client_type
        self.origin = origin
        self.start_time = start_time
        self.user_query = user_query
        self.rephrased_query = rephrased_query
        self.chat_history = chat_history
        self.learning_id = learning_id
        self.query_context = query_context
        self.query_context_order = query_context_order

    async def audit(self, text_answer: bytes, status_code: AnswerStatusCode):
        await maybe_audit_chat(
            kbid=self.kbid,
            user_id=self.user_id,
            client_type=self.client_type,
            origin=self.origin,
            duration=time() - self.start_time,
            user_query=self.user_query,
            rephrased_query=self.rephrased_query,
            text_answer=text_answer,
            status_code=status_code,
            chat_history=self.chat_history,
            query_context=self.query_context,
            query_context_order=self.query_context_order,
            learning_id=self.learning_id or "unknown",
        )


def sorted_prompt_context_list(
    context: PromptContext, order: PromptContextOrder
) -> list[str]:
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
