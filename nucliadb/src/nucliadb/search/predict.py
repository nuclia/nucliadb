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
import base64
import json
import logging
import os
import random
from enum import Enum
from typing import Any, AsyncGenerator, Optional
from unittest.mock import AsyncMock, Mock

import aiohttp
import backoff
from nuclia_models.predict.generative_responses import GenerativeChunk
from pydantic import ValidationError

from nucliadb.common import datamanagers
from nucliadb.search import logger
from nucliadb.search.predict_models import (
    AppliedDataAugmentation,
    AugmentedField,
    RunAgentsRequest,
    RunAgentsResponse,
)
from nucliadb.tests.vectors import Q, Qm2023
from nucliadb_models.internal.predict import (
    Ner,
    QueryInfo,
    RerankModel,
    RerankResponse,
    SentenceSearch,
    TokenSearch,
)
from nucliadb_models.search import (
    ChatModel,
    RephraseModel,
    SummarizedResource,
    SummarizedResponse,
    SummarizeModel,
)
from nucliadb_protos.resources_pb2 import FieldMetadata
from nucliadb_protos.utils_pb2 import RelationNode
from nucliadb_telemetry import errors, metrics
from nucliadb_utils.exceptions import LimitsExceededError
from nucliadb_utils.settings import nuclia_settings
from nucliadb_utils.utilities import Utility, set_utility


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
DUMMY_LEARNING_MODEL = "chatgpt"


PUBLIC_PREDICT = "/api/v1/predict"
PRIVATE_PREDICT = "/api/internal/predict"
SENTENCE = "/sentence"
TOKENS = "/tokens"
QUERY = "/query"
SUMMARIZE = "/summarize"
CHAT = "/chat"
REPHRASE = "/rephrase"
FEEDBACK = "/feedback"
RERANK = "/rerank"

NUCLIA_LEARNING_ID_HEADER = "NUCLIA-LEARNING-ID"
NUCLIA_LEARNING_MODEL_HEADER = "NUCLIA-LEARNING-MODEL"


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
    NO_RETRIEVAL_DATA = "-3"

    def prettify(self) -> str:
        return {
            AnswerStatusCode.SUCCESS: "success",
            AnswerStatusCode.ERROR: "error",
            AnswerStatusCode.NO_CONTEXT: "no_context",
            AnswerStatusCode.NO_RETRIEVAL_DATA: "no_retrieval_data",
        }[self]


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
            return f"{self.cluster_url}{PRIVATE_PREDICT}{endpoint}"

    def get_predict_headers(self, kbid: str) -> dict[str, str]:
        if self.onprem:
            headers = {"X-STF-NUAKEY": f"Bearer {self.nuclia_service_account}"}
            if self.local_predict_headers is not None:
                headers.update(self.local_predict_headers)
            return headers
        else:
            return {"X-STF-KBID": kbid}

    async def check_response(
        self, kbid: str, resp: aiohttp.ClientResponse, expected_status: int = 200
    ) -> None:
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

        is_5xx_error = resp.status > 499
        # NOTE: 512 is a special status code sent by learning predict api indicating that the error
        # is related to an external generative model, so we don't want to log it as an error
        is_external_generative_error = resp.status == 512
        log_level = logging.ERROR if is_5xx_error and not is_external_generative_error else logging.INFO
        logger.log(
            log_level,
            "Predict API error",
            extra=dict(
                kbid=kbid,
                url=resp.url,
                status_code=resp.status,
                detail=detail,
            ),
        )
        raise ProxiedPredictAPIError(status=resp.status, detail=detail)

    @backoff.on_exception(
        backoff.expo,
        RETRIABLE_EXCEPTIONS,
        jitter=backoff.random_jitter,
        max_tries=MAX_TRIES,
    )
    async def make_request(self, method: str, **request_args) -> aiohttp.ClientResponse:
        func = getattr(self.session, method.lower())
        return await func(**request_args)

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
        await self.check_response(kbid, resp, expected_status=200)
        return await _parse_rephrase_response(resp)

    @predict_observer.wrap({"type": "chat_ndjson"})
    async def chat_query_ndjson(
        self, kbid: str, item: ChatModel
    ) -> tuple[str, str, AsyncGenerator[GenerativeChunk, None]]:
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
        await self.check_response(kbid, resp, expected_status=200)
        ident = resp.headers.get(NUCLIA_LEARNING_ID_HEADER) or "unknown"
        model = resp.headers.get(NUCLIA_LEARNING_MODEL_HEADER) or "unknown"
        return ident, model, get_chat_ndjson_generator(resp)

    @predict_observer.wrap({"type": "query"})
    async def query(
        self,
        kbid: str,
        sentence: str,
        semantic_model: Optional[str] = None,
        generative_model: Optional[str] = None,
        rephrase: bool = False,
        rephrase_prompt: Optional[str] = None,
    ) -> QueryInfo:
        """
        Query endpoint: returns information to be used by NucliaDB at retrieval time, for instance:
        - The embeddings
        - The entities
        - The stop words
        - The semantic threshold
        - etc.

        :param kbid: KnowledgeBox ID
        :param sentence: The query sentence
        :param semantic_model: The semantic model to use to generate the embeddings
        :param generative_model: The generative model that will be used to generate the answer
        :param rephrase: If the query should be rephrased before calculating the embeddings for a better retrieval
        :param rephrase_prompt: Custom prompt to use for rephrasing
        """
        try:
            self.check_nua_key_is_configured_for_onprem()
        except NUAKeyMissingError:
            error = "Nuclia Service account is not defined so could not ask query endpoint"
            logger.warning(error)
            raise SendToPredictError(error)

        params: dict[str, Any] = {
            "text": sentence,
            "rephrase": str(rephrase),
        }
        if rephrase_prompt is not None:
            params["rephrase_prompt"] = rephrase_prompt
        if semantic_model is not None:
            params["semantic_models"] = [semantic_model]
        if generative_model is not None:
            params["generative_model"] = generative_model

        resp = await self.make_request(
            "GET",
            url=self.get_predict_url(QUERY, kbid),
            params=params,
            headers=self.get_predict_headers(kbid),
        )
        await self.check_response(kbid, resp, expected_status=200)
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
        await self.check_response(kbid, resp, expected_status=200)
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
        await self.check_response(kbid, resp, expected_status=200)
        data = await resp.json()
        return SummarizedResponse.model_validate(data)

    @predict_observer.wrap({"type": "rerank"})
    async def rerank(self, kbid: str, item: RerankModel) -> RerankResponse:
        try:
            self.check_nua_key_is_configured_for_onprem()
        except NUAKeyMissingError:
            error = "Nuclia Service account is not defined. Rerank operation could not be performed"
            logger.warning(error)
            raise SendToPredictError(error)
        resp = await self.make_request(
            "POST",
            url=self.get_predict_url(RERANK, kbid),
            json=item.model_dump(),
            headers=self.get_predict_headers(kbid),
        )
        await self.check_response(kbid, resp, expected_status=200)
        data = await resp.json()
        return RerankResponse.model_validate(data)

    @predict_observer.wrap({"type": "run_agents"})
    async def run_agents(self, kbid: str, item: RunAgentsRequest) -> RunAgentsResponse:
        try:
            self.check_nua_key_is_configured_for_onprem()
        except NUAKeyMissingError:
            error = "Nuclia Service account is not defined. Summarize operation could not be performed"
            logger.warning(error)
            raise SendToPredictError(error)
        resp = await self.make_request(
            "POST",
            url=self.get_predict_url("/run-agents", kbid),
            json=item.model_dump(),
            headers=self.get_predict_headers(kbid),
        )
        await self.check_response(kbid, resp, expected_status=200)
        data = await resp.json()
        return RunAgentsResponse.model_validate(data)


class DummyPredictEngine(PredictEngine):
    default_semantic_threshold = 0.7

    def __init__(self):
        self.onprem = True
        self.cluster_url = "http://localhost:8000"
        self.public_url = "http://localhost:8000"
        self.calls = []
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
        json_data = {"foo": "bar"}
        response.json = AsyncMock(return_value=json_data)
        response.read = AsyncMock(return_value=json.dumps(json_data).encode("utf-8"))
        response.headers = {NUCLIA_LEARNING_ID_HEADER: DUMMY_LEARNING_ID}
        return response

    async def rephrase_query(self, kbid: str, item: RephraseModel) -> str:
        self.calls.append(("rephrase_query", item))
        return DUMMY_REPHRASE_QUERY

    async def chat_query_ndjson(
        self, kbid: str, item: ChatModel
    ) -> tuple[str, str, AsyncGenerator[GenerativeChunk, None]]:
        self.calls.append(("chat_query_ndjson", item))

        async def generate():
            for item in self.ndjson_answer:
                yield GenerativeChunk.model_validate_json(item)

        return (DUMMY_LEARNING_ID, DUMMY_LEARNING_MODEL, generate())

    async def query(
        self,
        kbid: str,
        sentence: str,
        semantic_model: Optional[str] = None,
        generative_model: Optional[str] = None,
        rephrase: bool = False,
        rephrase_prompt: Optional[str] = None,
    ) -> QueryInfo:
        self.calls.append(("query", sentence))

        if os.environ.get("TEST_SENTENCE_ENCODER") == "multilingual-2023-02-21":  # pragma: no cover
            base_vector = Qm2023
        else:
            base_vector = Q

        # populate data with existing vectorsets
        async with datamanagers.with_ro_transaction() as txn:
            semantic_thresholds = {}
            vectors = {}
            timings = {}
            async for vectorset_id, config in datamanagers.vectorsets.iter(txn, kbid=kbid):
                semantic_thresholds[vectorset_id] = self.default_semantic_threshold
                vectorset_dimension = config.vectorset_index_config.vector_dimension
                if vectorset_dimension > len(base_vector):
                    padding = vectorset_dimension - len(base_vector)
                    vectors[vectorset_id] = base_vector + [random.random()] * padding
                else:
                    vectors[vectorset_id] = base_vector[:vectorset_dimension]

                timings[vectorset_id] = 0.010

        # and fake data with the passed one too
        model = semantic_model or "<PREDICT-DEFAULT-SEMANTIC-MODEL>"
        semantic_thresholds[model] = self.default_semantic_threshold
        vectors[model] = base_vector
        timings[model] = 0.0

        return QueryInfo(
            language="en",
            stop_words=[],
            semantic_thresholds=semantic_thresholds,
            visual_llm=True,
            max_context=self.max_context,
            entities=TokenSearch(tokens=[Ner(text="text", ner="PERSON", start=0, end=2)], time=0.0),
            sentence=SentenceSearch(
                vectors=vectors,
                timings=timings,
            ),
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

    async def rerank(self, kbid: str, item: RerankModel) -> RerankResponse:
        self.calls.append(("rerank", (kbid, item)))
        # as we don't have information about the retrieval scores, return a
        # random score given by the dict iteration
        response = RerankResponse(
            context_scores={paragraph_id: i for i, paragraph_id in enumerate(item.context.keys())}
        )
        return response

    async def run_agents(self, kbid: str, item: RunAgentsRequest) -> RunAgentsResponse:
        self.calls.append(("run_agents", (kbid, item)))
        fm = FieldMetadata()
        ada = AppliedDataAugmentation()
        serialized_fm = base64.b64encode(fm.SerializeToString()).decode("utf-8")
        augmented_field = AugmentedField(
            metadata=serialized_fm,  # type: ignore
            applied_data_augmentation=ada,
            input_nuclia_tokens=1.0,
            output_nuclia_tokens=1.0,
            time=1.0,
        )
        response = RunAgentsResponse(results={"field_id": augmented_field})
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
) -> AsyncGenerator[GenerativeChunk, None]:
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
