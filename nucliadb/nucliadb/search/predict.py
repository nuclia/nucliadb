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
from typing import AsyncIterator, Dict, List, Optional, Tuple

import aiohttp
import backoff
from async_lru import alru_cache
from nucliadb_protos.knowledgebox_pb2 import KBConfiguration
from nucliadb_protos.utils_pb2 import RelationNode

from nucliadb.ingest.orm.knowledgebox import KB_CONFIGURATION
from nucliadb.ingest.tests.vectors import Q, Qm2023
from nucliadb.ingest.txn_utils import get_transaction
from nucliadb.search import logger
from nucliadb_models.search import (
    AskDocumentModel,
    ChatModel,
    FeedbackRequest,
    RephraseModel,
)
from nucliadb_telemetry import metrics
from nucliadb_utils.exceptions import LimitsExceededError
from nucliadb_utils.settings import nuclia_settings
from nucliadb_utils.utilities import Utility, set_utility


class SendToPredictError(Exception):
    pass


class PredictVectorMissing(Exception):
    pass


class NUAKeyMissingError(Exception):
    pass


class RephraseError(Exception):
    pass


class RephraseMissingContextError(Exception):
    pass


DUMMY_RELATION_NODE = [
    RelationNode(value="Ferran", ntype=RelationNode.NodeType.ENTITY, subtype="PERSON"),
    RelationNode(
        value="Joan Antoni", ntype=RelationNode.NodeType.ENTITY, subtype="PERSON"
    ),
]

DUMMY_REPHRASE_QUERY = "This is a rephrased query"
DUMMY_LEARNING_ID = "00"


PUBLIC_PREDICT = "/api/v1/predict"
PRIVATE_PREDICT = "/api/internal/predict"
SENTENCE = "/sentence"
TOKENS = "/tokens"
CHAT = "/chat"
ASK_DOCUMENT = "/ask_document"
REPHRASE = "/rephrase"
FEEDBACK = "/feedback"


predict_observer = metrics.Observer(
    "predict_engine",
    labels={"type": ""},
    error_mappings={
        "over_limits": LimitsExceededError,
        "predict_api_error": SendToPredictError,
        "empty_vectors": PredictVectorMissing,
    },
)


RETRIABLE_EXCEPTIONS = (aiohttp.client_exceptions.ClientConnectorError,)
MAX_TRIES = 2


class AnswerStatusCode(str, Enum):
    SUCCESS = "0"
    ERROR = "-1"
    NO_CONTEXT = "-2"


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


def convert_relations(data: Dict[str, List[Dict[str, str]]]) -> List[RelationNode]:
    result = []
    for token in data["tokens"]:
        text = token["text"]
        klass = token["ner"]
        result.append(
            RelationNode(value=text, ntype=RelationNode.NodeType.ENTITY, subtype=klass)
        )
    return result


class DummyPredictEngine:
    def __init__(self):
        self.calls = []
        self.generated_answer = [
            b"valid ",
            b"answer ",
            b" to",
            AnswerStatusCode.SUCCESS.encode(),
        ]

    async def initialize(self):
        pass

    async def finalize(self):
        pass

    async def send_feedback(
        self,
        kbid: str,
        item: FeedbackRequest,
        x_nucliadb_user: str,
        x_ndb_client: str,
        x_forwarded_for: str,
    ):
        self.calls.append(item)
        return

    async def rephrase_query(self, kbid: str, item: RephraseModel) -> str:
        self.calls.append(item)
        return DUMMY_REPHRASE_QUERY

    async def chat_query(
        self, kbid: str, item: ChatModel
    ) -> Tuple[str, AsyncIterator[bytes]]:
        self.calls.append(item)

        async def generate():
            for i in self.generated_answer:
                yield i

        return (DUMMY_LEARNING_ID, generate())

    async def ask_document(
        self, kbid: str, query: str, blocks: list[list[str]], user_id: str
    ) -> str:
        self.calls.append((query, blocks, user_id))
        answer = os.environ.get("TEST_ASK_DOCUMENT") or "Answer to your question"
        return answer

    async def convert_sentence_to_vector(self, kbid: str, sentence: str) -> List[float]:
        self.calls.append(sentence)
        if (
            os.environ.get("TEST_SENTENCE_ENCODER") == "multilingual-2023-02-21"
        ):  # pragma: no cover
            return Qm2023
        else:
            return Q

    async def detect_entities(self, kbid: str, sentence: str) -> List[RelationNode]:
        self.calls.append(sentence)
        dummy_data = os.environ.get("TEST_RELATIONS", None)
        if dummy_data is not None:  # pragma: no cover
            return convert_relations(json.loads(dummy_data))
        else:
            return DUMMY_RELATION_NODE


class PredictEngine:
    def __init__(
        self,
        cluster_url: Optional[str] = None,
        public_url: Optional[str] = None,
        nuclia_service_account: Optional[str] = None,
        zone: Optional[str] = None,
        onprem: bool = False,
        local_predict: bool = False,
        local_predict_headers: Optional[Dict[str, str]] = None,
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

    async def get_configuration(self, kbid: str) -> Optional[KBConfiguration]:
        if self.onprem is False:
            return None
        return await self._get_configuration(kbid)

    @alru_cache(maxsize=None)
    async def _get_configuration(self, kbid: str) -> Optional[KBConfiguration]:
        txn = await get_transaction()
        config_key = KB_CONFIGURATION.format(kbid=kbid)
        payload = await txn.get(config_key)
        if payload is None:
            return None

        kb_pb = KBConfiguration()
        kb_pb.ParseFromString(payload)
        return kb_pb

    def check_nua_key_is_configured_for_onprem(self):
        if self.onprem and (
            self.nuclia_service_account is None and self.local_predict is False
        ):
            raise NUAKeyMissingError()

    def get_predict_url(self, endpoint: str) -> str:
        if self.onprem:
            return f"{self.public_url}{PUBLIC_PREDICT}{endpoint}"
        else:
            return f"{self.cluster_url}{PRIVATE_PREDICT}{endpoint}"

    async def get_predict_headers(self, kbid: str) -> dict[str, str]:
        if self.onprem:
            headers = {"X-STF-NUAKEY": f"Bearer {self.nuclia_service_account}"}
            config = await self.get_configuration(kbid)
            if self.local_predict_headers is not None:
                headers.update(self.local_predict_headers)
            if config is not None:
                if config.anonymization_model:
                    headers["X-STF-ANONYMIZATION-MODEL"] = config.anonymization_model
                if config.semantic_model:
                    headers["X-STF-SEMANTIC-MODEL"] = config.semantic_model
                if config.ner_model:
                    headers["X-STF-NER-MODEL"] = config.ner_model
                if config.generative_model:
                    headers["X-STF-GENERATIVE-MODEL"] = config.generative_model
                if config.visual_labeling:
                    headers["X-STF-VISUAL-LABELING"] = config.visual_labeling
            return headers
        else:
            return {"X-STF-KBID": kbid}

    async def check_response(
        self, resp: aiohttp.ClientResponse, expected_status: int = 200
    ) -> None:
        if resp.status == expected_status:
            return

        if resp.status == 402:
            data = await resp.json()
            raise LimitsExceededError(402, data["detail"])

        error = (await resp.read()).decode()
        logger.error(f"Predict API error at {resp.url}: {error}")
        raise SendToPredictError(f"{resp.status}: {error}")

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=MAX_TRIES)
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
            logger.warning(
                "Nuclia Service account is not defined so could not send the feedback"
            )
            return

        data = item.dict()
        data["user_id"] = x_nucliadb_user
        data["client"] = x_ndb_client
        data["forwarded"] = x_forwarded_for

        resp = await self.make_request(
            "POST",
            url=self.get_predict_url(FEEDBACK),
            json=data,
            headers=await self.get_predict_headers(kbid),
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
            url=self.get_predict_url(REPHRASE),
            json=item.dict(),
            headers=await self.get_predict_headers(kbid),
        )
        await self.check_response(resp, expected_status=200)
        return await _parse_rephrase_response(resp)

    @predict_observer.wrap({"type": "chat"})
    async def chat_query(
        self, kbid: str, item: ChatModel
    ) -> Tuple[str, AsyncIterator[bytes]]:
        try:
            self.check_nua_key_is_configured_for_onprem()
        except NUAKeyMissingError:
            error = "Nuclia Service account is not defined so the chat operation could not be performed"
            logger.warning(error)
            raise SendToPredictError(error)

        resp = await self.make_request(
            "POST",
            url=self.get_predict_url(CHAT),
            json=item.dict(),
            headers=await self.get_predict_headers(kbid),
        )
        await self.check_response(resp, expected_status=200)
        ident = resp.headers.get("NUCLIA-LEARNING-ID")
        return ident, get_answer_generator(resp)

    @predict_observer.wrap({"type": "ask_document"})
    async def ask_document(
        self, kbid: str, question: str, blocks: list[list[str]], user_id: str
    ) -> str:
        try:
            self.check_nua_key_is_configured_for_onprem()
        except NUAKeyMissingError:
            error = "Nuclia Service account is not defined so could not ask document"
            logger.warning(error)
            raise SendToPredictError(error)

        item = AskDocumentModel(question=question, blocks=blocks, user_id=user_id)
        resp = await self.make_request(
            "POST",
            url=self.get_predict_url(ASK_DOCUMENT),
            json=item.dict(),
            headers=await self.get_predict_headers(kbid),
            timeout=None,
        )
        await self.check_response(resp, expected_status=200)
        return await resp.text()

    @predict_observer.wrap({"type": "sentence"})
    async def convert_sentence_to_vector(self, kbid: str, sentence: str) -> List[float]:
        try:
            self.check_nua_key_is_configured_for_onprem()
        except NUAKeyMissingError:
            logger.warning(
                "Nuclia Service account is not defined so could not retrieve vectors for the query"
            )
            return []

        resp = await self.make_request(
            "GET",
            url=self.get_predict_url(SENTENCE),
            params={"text": sentence},
            headers=await self.get_predict_headers(kbid),
        )
        await self.check_response(resp, expected_status=200)
        data = await resp.json()
        if len(data["data"]) == 0:
            raise PredictVectorMissing()
        return data["data"]

    @predict_observer.wrap({"type": "entities"})
    async def detect_entities(self, kbid: str, sentence: str) -> List[RelationNode]:
        try:
            self.check_nua_key_is_configured_for_onprem()
        except NUAKeyMissingError:
            logger.warning(
                "Nuclia Service account is not defined so could not retrieve entities from the query"
            )
            return []

        resp = await self.make_request(
            "GET",
            url=self.get_predict_url(TOKENS),
            params={"text": sentence},
            headers=await self.get_predict_headers(kbid),
        )
        await self.check_response(resp, expected_status=200)
        data = await resp.json()

        return convert_relations(data)


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
