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
import json
import os
from enum import Enum
from typing import Any, AsyncIterator, Dict, Literal, Optional, Union
from unittest.mock import AsyncMock, Mock

import aiohttp
import backoff
from pydantic import BaseModel, Field, ValidationError

from nucliadb.search import logger
from nucliadb.tests.vectors import Q, Qm2023
from nucliadb_models.search import (
    ChatModel,
    FeedbackRequest,
    Ner,
    QueryInfo,
    RephraseModel,
    SentenceSearch,
    SummarizedResource,
    SummarizedResponse,
    SummarizeModel,
    TokenSearch,
)
from nucliadb_protos.utils_pb2 import RelationNode
from nucliadb_telemetry import errors, metrics
from nucliadb_utils import const
from nucliadb_utils.exceptions import LimitsExceededError
from nucliadb_utils.settings import nuclia_settings
from nucliadb_utils.utilities import Utility, has_feature, set_utility


class SendToPredictError(Exception):
    pass


class ProxiedPredictAPIError(Exception):
    def __init__(self, status: int, detail: str = ""):
        self.status = status
        self.detail = detail


class NUAKeyMissingError(Exception):
    pass


class RephraseError(Exception):
    pass


class RephraseMissingContextError(Exception):
    pass


DUMMY_RELATION_NODE = [
    RelationNode(value="Ferran", ntype=RelationNode.NodeType.ENTITY, subtype="PERSON"),
    RelationNode(value="Joan Antoni", ntype=RelationNode.NodeType.ENTITY, subtype="PERSON"),
]

DUMMY_REPHRASE_QUERY = "This is a rephrased query"
DUMMY_LEARNING_ID = "00"


PUBLIC_PREDICT = "/api/v1/predict"
PRIVATE_PREDICT = "/api/internal/predict"
VERSIONED_PRIVATE_PREDICT = "/api/v1/internal/predict"
SENTENCE = "/sentence"
TOKENS = "/tokens"
QUERY = "/query"
SUMMARIZE = "/summarize"
CHAT = "/chat"
REPHRASE = "/rephrase"
FEEDBACK = "/feedback"

NUCLIA_LEARNING_ID_HEADER = "NUCLIA-LEARNING-ID"


predict_observer = metrics.Observer(
    "predict_engine",
    labels={"type": ""},
    error_mappings={
        "over_limits": LimitsExceededError,
        "predict_api_error": SendToPredictError,
    },
)


RETRIABLE_EXCEPTIONS = (aiohttp.client_exceptions.ClientConnectorError,)
MAX_TRIES = 2


class AnswerStatusCode(str, Enum):
    SUCCESS = "0"
    ERROR = "-1"
    NO_CONTEXT = "-2"

    def prettify(self) -> str:
        return {
            AnswerStatusCode.SUCCESS: "success",
            AnswerStatusCode.ERROR: "error",
            AnswerStatusCode.NO_CONTEXT: "no_context",
        }[self]


class TextGenerativeResponse(BaseModel):
    type: Literal["text"] = "text"
    text: str


class JSONGenerativeResponse(BaseModel):
    type: Literal["object"] = "object"
    object: Dict[str, Any]


class MetaGenerativeResponse(BaseModel):
    type: Literal["meta"] = "meta"
    input_tokens: int
    output_tokens: int
    timings: dict[str, float]


class CitationsGenerativeResponse(BaseModel):
    type: Literal["citations"] = "citations"
    citations: dict[str, Any]


class StatusGenerativeResponse(BaseModel):
    type: Literal["status"] = "status"
    code: str
    details: Optional[str] = None


GenerativeResponse = Union[
    TextGenerativeResponse,
    JSONGenerativeResponse,
    MetaGenerativeResponse,
    CitationsGenerativeResponse,
    StatusGenerativeResponse,
]


class GenerativeChunk(BaseModel):
    chunk: GenerativeResponse = Field(..., discriminator="type")


async def start_predict_engine():
    if nuclia_settings.dummy_predict:
        predict_util = DummyPredictEngine()
    else:
        predict_util = PredictEngine(
            nuclia_settings.nuclia_inner_predict_url,
            nuclia_settings.nuclia_public_url,
            nuclia_settings.nuclia_service_account,
            nuclia_settings.nuclia_zone,
            nuclia_settings.onprem,
            nuclia_settings.local_predict,
            nuclia_settings.local_predict_headers,
        )
    await predict_util.initialize()
    set_utility(Utility.PREDICT, predict_util)


def convert_relations(data: dict[str, list[dict[str, str]]]) -> list[RelationNode]:
    result = []
    for token in data["tokens"]:
        text = token["text"]
        klass = token["ner"]
        result.append(RelationNode(value=text, ntype=RelationNode.NodeType.ENTITY, subtype=klass))
    return result


class PredictEngine:
    def __init__(
        self,
        cluster_url: Optional[str] = None,
        public_url: Optional[str] = None,
        nuclia_service_account: Optional[str] = None,
        zone: Optional[str] = None,
        onprem: bool = False,
        local_predict: bool = False,
        local_predict_headers: Optional[dict[str, str]] = None,
    ):
        self.nuclia_service_account = nuclia_service_account
        self.cluster_url = cluster_url
        if public_url is not None:
            self.public_url: Optional[str] = public_url.format(zone=zone)
        else:
            self.public_url = None
        self.zone = zone
        self.onprem = onprem
        self.local_predict = local_predict
        self.local_predict_headers = local_predict_headers

    async def initialize(self):
        self.session = aiohttp.ClientSession()

    async def finalize(self):
        await self.session.close()

    def check_nua_key_is_configured_for_onprem(self):
        if self.onprem and (self.nuclia_service_account is None and self.local_predict is False):
            raise NUAKeyMissingError()

    def get_predict_url(self, endpoint: str, kbid: str) -> str:
        if not endpoint.startswith("/"):
            endpoint = "/" + endpoint
        if self.onprem:
            # On-prem NucliaDB uses the public URL for the predict API. Examples:
            # /api/v1/predict/chat/{kbid}
            # /api/v1/predict/rephrase/{kbid}
            return f"{self.public_url}{PUBLIC_PREDICT}{endpoint}/{kbid}"
        else:
            if has_feature(const.Features.VERSIONED_PRIVATE_PREDICT):
                return f"{self.cluster_url}{VERSIONED_PRIVATE_PREDICT}{endpoint}"
            else:
                return f"{self.cluster_url}{PRIVATE_PREDICT}{endpoint}"

    def get_predict_headers(self, kbid: str) -> dict[str, str]:
        if self.onprem:
            headers = {"X-STF-NUAKEY": f"Bearer {self.nuclia_service_account}"}
            if self.local_predict_headers is not None:
                headers.update(self.local_predict_headers)
            return headers
        else:
            return {"X-STF-KBID": kbid}

    async def check_response(self, resp: aiohttp.ClientResponse, expected_status: int = 200) -> None:
        if resp.status == expected_status:
            return

        if resp.status == 402:
            data = await resp.json()
            raise LimitsExceededError(402, data["detail"])
        try:
            data = await resp.json()
            try:
                detail = data["detail"]
            except (KeyError, TypeError):
                detail = data
        except (
            json.decoder.JSONDecodeError,
            aiohttp.client_exceptions.ContentTypeError,
        ):
            detail = await resp.text()
        if str(resp.status).startswith("5"):
            logger.error(f"Predict API error at {resp.url}: {detail}")
        else:
            logger.info(f"Predict API error at {resp.url}: {detail}")
        raise ProxiedPredictAPIError(status=resp.status, detail=detail)

    @backoff.on_exception(
        backoff.expo,
        RETRIABLE_EXCEPTIONS,
        jitter=backoff.random_jitter,
        max_tries=MAX_TRIES,
    )
    async def make_request(self, method: str, **request_args):
        func = getattr(self.session, method.lower())
        return await func(**request_args)

    @predict_observer.wrap({"type": "feedback"})
    async def send_feedback(
        self,
        kbid: str,
        item: FeedbackRequest,
        x_nucliadb_user: str,
        x_ndb_client: str,
        x_forwarded_for: str,
    ):
        try:
            self.check_nua_key_is_configured_for_onprem()
        except NUAKeyMissingError:
            logger.warning("Nuclia Service account is not defined so could not send the feedback")
            return

        data = item.model_dump()
        data["user_id"] = x_nucliadb_user
        data["client"] = x_ndb_client
        data["forwarded"] = x_forwarded_for

        resp = await self.make_request(
            "POST",
            url=self.get_predict_url(FEEDBACK, kbid),
            json=data,
            headers=self.get_predict_headers(kbid),
        )
        await self.check_response(resp, expected_status=204)

    @predict_observer.wrap({"type": "rephrase"})
    async def rephrase_query(self, kbid: str, item: RephraseModel) -> str:
        try:
            self.check_nua_key_is_configured_for_onprem()
        except NUAKeyMissingError:
            error = "Nuclia Service account is not defined so could not rephrase query"
            logger.warning(error)
            raise SendToPredictError(error)

        resp = await self.make_request(
            "POST",
            url=self.get_predict_url(REPHRASE, kbid),
            json=item.model_dump(),
            headers=self.get_predict_headers(kbid),
        )
        await self.check_response(resp, expected_status=200)
        return await _parse_rephrase_response(resp)

    @predict_observer.wrap({"type": "chat"})
    async def chat_query(self, kbid: str, item: ChatModel) -> tuple[str, AsyncIterator[bytes]]:
        try:
            self.check_nua_key_is_configured_for_onprem()
        except NUAKeyMissingError:
            error = "Nuclia Service account is not defined so the chat operation could not be performed"
            logger.warning(error)
            raise SendToPredictError(error)

        resp = await self.make_request(
            "POST",
            url=self.get_predict_url(CHAT, kbid),
            json=item.model_dump(),
            headers=self.get_predict_headers(kbid),
            timeout=None,
        )
        await self.check_response(resp, expected_status=200)
        ident = resp.headers.get(NUCLIA_LEARNING_ID_HEADER)
        return ident, get_answer_generator(resp)

    @predict_observer.wrap({"type": "chat_ndjson"})
    async def chat_query_ndjson(
        self, kbid: str, item: ChatModel
    ) -> tuple[str, AsyncIterator[GenerativeChunk]]:
        """
        Chat query using the new stream format
        Format specs: https://github.com/ndjson/ndjson-spec
        """
        try:
            self.check_nua_key_is_configured_for_onprem()
        except NUAKeyMissingError:
            error = "Nuclia Service account is not defined so the chat operation could not be performed"
            logger.warning(error)
            raise SendToPredictError(error)

        # The ndjson format is triggered by the Accept header
        headers = self.get_predict_headers(kbid)
        headers["Accept"] = "application/x-ndjson"

        resp = await self.make_request(
            "POST",
            url=self.get_predict_url(CHAT, kbid),
            json=item.model_dump(),
            headers=headers,
            timeout=None,
        )
        await self.check_response(resp, expected_status=200)
        ident = resp.headers.get(NUCLIA_LEARNING_ID_HEADER)
        return ident, get_chat_ndjson_generator(resp)

    @predict_observer.wrap({"type": "query"})
    async def query(
        self,
        kbid: str,
        sentence: str,
        generative_model: Optional[str] = None,
        rephrase: Optional[bool] = False,
    ) -> QueryInfo:
        try:
            self.check_nua_key_is_configured_for_onprem()
        except NUAKeyMissingError:
            error = "Nuclia Service account is not defined so could not ask query endpoint"
            logger.warning(error)
            raise SendToPredictError(error)

        params = {
            "text": sentence,
            "rephrase": str(rephrase),
        }
        if generative_model is not None:
            params["generative_model"] = generative_model

        resp = await self.make_request(
            "GET",
            url=self.get_predict_url(QUERY, kbid),
            params=params,
            headers=self.get_predict_headers(kbid),
        )
        await self.check_response(resp, expected_status=200)
        data = await resp.json()
        return QueryInfo(**data)

    @predict_observer.wrap({"type": "entities"})
    async def detect_entities(self, kbid: str, sentence: str) -> list[RelationNode]:
        try:
            self.check_nua_key_is_configured_for_onprem()
        except NUAKeyMissingError:
            logger.warning(
                "Nuclia Service account is not defined so could not retrieve entities from the query"
            )
            return []

        resp = await self.make_request(
            "GET",
            url=self.get_predict_url(TOKENS, kbid),
            params={"text": sentence},
            headers=self.get_predict_headers(kbid),
        )
        await self.check_response(resp, expected_status=200)
        data = await resp.json()
        return convert_relations(data)

    @predict_observer.wrap({"type": "summarize"})
    async def summarize(self, kbid: str, item: SummarizeModel) -> SummarizedResponse:
        try:
            self.check_nua_key_is_configured_for_onprem()
        except NUAKeyMissingError:
            error = "Nuclia Service account is not defined. Summarize operation could not be performed"
            logger.warning(error)
            raise SendToPredictError(error)
        resp = await self.make_request(
            "POST",
            url=self.get_predict_url(SUMMARIZE, kbid),
            json=item.model_dump(),
            headers=self.get_predict_headers(kbid),
            timeout=None,
        )
        await self.check_response(resp, expected_status=200)
        data = await resp.json()
        return SummarizedResponse.model_validate(data)


class DummyPredictEngine(PredictEngine):
    def __init__(self):
        self.onprem = True
        self.cluster_url = "http://localhost:8000"
        self.public_url = "http://localhost:8000"
        self.calls = []
        self.generated_answer = [
            b"valid ",
            b"answer ",
            b" to",
            AnswerStatusCode.SUCCESS.encode(),
        ]
        self.ndjson_answer = [
            b'{"chunk": {"type": "text", "text": "valid "}}\n',
            b'{"chunk": {"type": "text", "text": "answer "}}\n',
            b'{"chunk": {"type": "text", "text": "to"}}\n',
            b'{"chunk": {"type": "status", "code": "0"}}\n',
        ]
        self.max_context = 1000

    async def initialize(self):
        pass

    async def finalize(self):
        pass

    def get_predict_headers(self, kbid: str) -> dict[str, str]:
        return {}

    async def make_request(self, method: str, **request_args):
        response = Mock(status=200)
        response.json = AsyncMock(return_value={"foo": "bar"})
        response.headers = {NUCLIA_LEARNING_ID_HEADER: DUMMY_LEARNING_ID}
        return response

    async def send_feedback(
        self,
        kbid: str,
        item: FeedbackRequest,
        x_nucliadb_user: str,
        x_ndb_client: str,
        x_forwarded_for: str,
    ):
        self.calls.append(("send_feedback", item))
        return

    async def rephrase_query(self, kbid: str, item: RephraseModel) -> str:
        self.calls.append(("rephrase_query", item))
        return DUMMY_REPHRASE_QUERY

    async def chat_query(self, kbid: str, item: ChatModel) -> tuple[str, AsyncIterator[bytes]]:
        self.calls.append(("chat_query", item))

        async def generate():
            for i in self.generated_answer:
                yield i

        return (DUMMY_LEARNING_ID, generate())

    async def chat_query_ndjson(
        self, kbid: str, item: ChatModel
    ) -> tuple[str, AsyncIterator[GenerativeChunk]]:
        self.calls.append(("chat_query_ndjson", item))

        async def generate():
            for item in self.ndjson_answer:
                yield GenerativeChunk.model_validate_json(item)

        return (DUMMY_LEARNING_ID, generate())

    async def query(
        self,
        kbid: str,
        sentence: str,
        generative_model: Optional[str] = None,
        rephrase: Optional[bool] = False,
    ) -> QueryInfo:
        self.calls.append(("query", sentence))
        if os.environ.get("TEST_SENTENCE_ENCODER") == "multilingual-2023-02-21":  # pragma: no cover
            return QueryInfo(
                language="en",
                stop_words=[],
                semantic_threshold=0.7,
                visual_llm=True,
                max_context=self.max_context,
                entities=TokenSearch(tokens=[Ner(text="text", ner="PERSON", start=0, end=2)], time=0.0),
                sentence=SentenceSearch(data=Qm2023, time=0.0),
                query=sentence,
            )
        else:
            return QueryInfo(
                language="en",
                stop_words=[],
                semantic_threshold=0.7,
                visual_llm=True,
                max_context=self.max_context,
                entities=TokenSearch(tokens=[Ner(text="text", ner="PERSON", start=0, end=2)], time=0.0),
                sentence=SentenceSearch(data=Q, time=0.0),
                query=sentence,
            )

    async def detect_entities(self, kbid: str, sentence: str) -> list[RelationNode]:
        self.calls.append(("detect_entities", sentence))
        dummy_data = os.environ.get("TEST_RELATIONS", None)
        if dummy_data is not None:  # pragma: no cover
            return convert_relations(json.loads(dummy_data))
        else:
            return DUMMY_RELATION_NODE

    async def summarize(self, kbid: str, item: SummarizeModel) -> SummarizedResponse:
        self.calls.append(("summarize", (kbid, item)))
        response = SummarizedResponse(
            summary="global summary",
        )
        for rid in item.resources.keys():
            rsummary = []
            for field_id, field_text in item.resources[rid].fields.items():
                rsummary.append(f"{field_id}: {field_text}")
            response.resources[rid] = SummarizedResource(summary="\n\n".join(rsummary), tokens=10)
        return response


def get_answer_generator(response: aiohttp.ClientResponse):
    """
    Returns an async generator that yields the chunks of the response
    in the same way as received from the server.
    See: https://docs.aiohttp.org/en/stable/streams.html#aiohttp.StreamReader.iter_chunks
    """

    async def _iter_answer_chunks(gen):
        buffer = b""
        async for chunk, end_of_chunk in gen:
            buffer += chunk
            if end_of_chunk:
                yield buffer
                buffer = b""

    return _iter_answer_chunks(response.content.iter_chunks())


def get_chat_ndjson_generator(
    response: aiohttp.ClientResponse,
) -> AsyncIterator[GenerativeChunk]:
    async def _parse_generative_chunks(gen):
        async for chunk in gen:
            try:
                yield GenerativeChunk.model_validate_json(chunk.strip())
            except ValidationError as ex:
                errors.capture_exception(ex)
                logger.error(f"Invalid chunk received: {chunk}")
                continue

    return _parse_generative_chunks(response.content)


async def _parse_rephrase_response(
    resp: aiohttp.ClientResponse,
) -> str:
    """
    Predict api is returning a json payload that is a string with the following format:
    <rephrased_query><status_code>
    where status_code is "0" for success, "-1" for error and "-2" for no context
    it will raise an exception if the status code is not 0
    """
    content = await resp.json()
    if content.endswith("0"):
        return content[:-1]
    elif content.endswith("-1"):
        raise RephraseError(content[:-2])
    elif content.endswith("-2"):
        raise RephraseMissingContextError(content[:-2])
    else:
        # bw compatibility
        return content
