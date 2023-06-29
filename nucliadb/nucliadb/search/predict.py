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
from typing import AsyncIterator, Dict, List, Optional, Tuple

import aiohttp
from nucliadb_protos.utils_pb2 import RelationNode

from nucliadb.ingest.tests.vectors import Q, Qm2023
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
            for i in [b"valid ", b"answer ", b" to"]:
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
    ):
        self.nuclia_service_account = nuclia_service_account
        self.cluster_url = cluster_url
        if public_url is not None:
            self.public_url: Optional[str] = public_url.format(zone=zone)
        else:
            self.public_url = None
        self.zone = zone
        self.onprem = onprem

    async def initialize(self):
        self.session = aiohttp.ClientSession()

    async def finalize(self):
        await self.session.close()

    def check_nua_key_is_configured_for_onprem(self):
        if self.onprem and self.nuclia_service_account is None:
            raise NUAKeyMissingError()

    def get_predict_url(self, endpoint: str) -> str:
        if self.onprem:
            return f"{self.public_url}{PUBLIC_PREDICT}{endpoint}"
        else:
            return f"{self.cluster_url}{PRIVATE_PREDICT}{endpoint}"

    def get_predict_headers(self, kbid: str) -> dict[str, str]:
        if self.onprem:
            return {"X-STF-NUAKEY": f"Bearer {self.nuclia_service_account}"}
        else:
            return {"X-STF-KBID": kbid}

    async def check_response(self, resp, expected: int = 200) -> None:
        if resp.status == expected:
            return
        if resp.status == 402:
            data = await resp.json()
            raise LimitsExceededError(402, data["detail"])
        else:
            raise SendToPredictError(f"{resp.status}: {await resp.read()}")

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

        resp = await self.session.post(
            url=self.get_predict_url(FEEDBACK),
            json=data,
            headers=self.get_predict_headers(kbid),
        )
        await self.check_response(resp, expected=204)

    @predict_observer.wrap({"type": "rephrase"})
    async def rephrase_query(self, kbid: str, item: RephraseModel) -> str:
        try:
            self.check_nua_key_is_configured_for_onprem()
        except NUAKeyMissingError:
            error = "Nuclia Service account is not defined so could not rephrase query"
            logger.warning(error)
            raise SendToPredictError(error)

        resp = await self.session.post(
            url=self.get_predict_url(REPHRASE),
            json=item.dict(),
            headers=self.get_predict_headers(kbid),
        )
        await self.check_response(resp, expected=200)
        return await resp.text()

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

        resp = await self.session.post(
            url=self.get_predict_url(CHAT),
            json=item.dict(),
            headers=self.get_predict_headers(kbid),
        )
        await self.check_response(resp, expected=200)
        ident = resp.headers.get("NUCLIA-LEARNING-ID")
        return ident, resp.content.iter_any()

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
        resp = await self.session.post(
            url=self.get_predict_url(ASK_DOCUMENT),
            json=item.dict(),
            headers=self.get_predict_headers(kbid),
            timeout=None,
        )
        await self.check_response(resp, expected=200)
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

        resp = await self.session.get(
            url=self.get_predict_url(SENTENCE),
            params={"text": sentence},
            headers=self.get_predict_headers(kbid),
        )
        await self.check_response(resp, expected=200)
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

        resp = await self.session.get(
            url=self.get_predict_url(TOKENS),
            params={"text": sentence},
            headers=self.get_predict_headers(kbid),
        )
        await self.check_response(resp, expected=200)
        data = await resp.json()

        return convert_relations(data)
