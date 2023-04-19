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
from typing import AsyncIterator, Dict, List, Optional, Tuple, Union

import aiohttp
from nucliadb_protos.utils_pb2 import RelationNode

from nucliadb.ingest.tests.vectors import Q, Qm2023
from nucliadb.search import logger
from nucliadb_models.search import ChatModel, FeedbackRequest, RephraseModel
from nucliadb_telemetry import metrics
from nucliadb_utils.exceptions import LimitsExceededError


class SendToPredictError(Exception):
    pass


class PredictVectorMissing(Exception):
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


def convert_relations(data: Dict[str, List[Dict[str, str]]]) -> List[RelationNode]:
    result = []
    for token in data["tokens"]:
        text = token["text"]
        klass = token["ner"]
        result.append(
            RelationNode(value=text, ntype=RelationNode.NodeType.ENTITY, subtype=klass)
        )
    return result


class PredictEngine:
    def __init__(
        self,
        cluster_url: Optional[str] = None,
        public_url: Optional[str] = None,
        nuclia_service_account: Optional[str] = None,
        zone: Optional[str] = None,
        onprem: bool = False,
        dummy: bool = False,
    ):
        self.nuclia_service_account = nuclia_service_account
        self.cluster_url = cluster_url
        if public_url is not None:
            self.public_url: Optional[str] = public_url.format(zone=zone)
        else:
            self.public_url = None
        self.zone = zone
        self.onprem = onprem
        self.dummy = dummy
        # TODO: self.calls is only for testing purposes.
        # It should be removed
        self.calls: List[Union[str, ChatModel, RephraseModel]] = []

    async def initialize(self):
        self.session = aiohttp.ClientSession()

    async def finalize(self):
        await self.session.close()

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
        data = item.dict()
        data["user_id"] = x_nucliadb_user
        data["client"] = x_ndb_client
        data["forwarded"] = x_forwarded_for

        if self.onprem is False:
            # Upload the payload
            resp = await self.session.post(
                url=f"{self.cluster_url}{PRIVATE_PREDICT}{FEEDBACK}",
                json=data,
                headers={"X-STF-KBID": kbid},
            )
        else:
            if self.nuclia_service_account is None:
                logger.warning(
                    "Nuclia Service account is not defined so could not retrieve vectors for the query"
                )
                return []
            # Upload the payload
            headers = {"X-STF-NUAKEY": f"Bearer {self.nuclia_service_account}"}
            resp = await self.session.post(
                url=f"{self.public_url}{PUBLIC_PREDICT}{FEEDBACK}",
                json=data,
                headers=headers,
            )
        await self.check_response(resp, expected=204)

    @predict_observer.wrap({"type": "rephrase"})
    async def rephrase_query(self, kbid: str, item: RephraseModel) -> str:
        if self.dummy:
            self.calls.append(item)
            return DUMMY_REPHRASE_QUERY

        if self.onprem is False:
            # Upload the payload
            resp = await self.session.post(
                url=f"{self.cluster_url}{PRIVATE_PREDICT}{REPHRASE}",
                json=item.dict(),
                headers={"X-STF-KBID": kbid},
            )
        else:
            if self.nuclia_service_account is None:
                error = (
                    "Nuclia Service account is not defined so could not rephrase query"
                )
                logger.warning(error)
                raise SendToPredictError(error)
            # Upload the payload
            headers = {"X-STF-NUAKEY": f"Bearer {self.nuclia_service_account}"}
            resp = await self.session.post(
                url=f"{self.public_url}{PUBLIC_PREDICT}{REPHRASE}",
                json=item.dict(),
                headers=headers,
            )
        await self.check_response(resp, expected=200)
        return await resp.text()

    @predict_observer.wrap({"type": "chat"})
    async def chat_query(
        self, kbid: str, item: ChatModel
    ) -> Tuple[str, AsyncIterator[bytes]]:
        if self.dummy:
            self.calls.append(item)

            async def generate():
                for i in [b"valid ", b"answer ", b" to"]:
                    yield i

            return (DUMMY_LEARNING_ID, generate())

        if self.onprem is False:
            # Upload the payload
            resp = await self.session.post(
                url=f"{self.cluster_url}{PRIVATE_PREDICT}{CHAT}",
                json=item.dict(),
                headers={"X-STF-KBID": kbid},
            )
        else:
            if self.nuclia_service_account is None:
                error = "Nuclia Service account is not defined so could not retrieve vectors for the query"
                logger.warning(error)
                raise SendToPredictError(error)
            # Upload the payload
            headers = {"X-STF-NUAKEY": f"Bearer {self.nuclia_service_account}"}
            resp = await self.session.post(
                url=f"{self.public_url}{PUBLIC_PREDICT}{CHAT}",
                json=item.dict(),
                headers=headers,
            )
        await self.check_response(resp, expected=200)
        ident = resp.headers.get("NUCLIA-LEARNING-ID")
        return ident, resp.content.iter_any()

    @predict_observer.wrap({"type": "sentence"})
    async def convert_sentence_to_vector(self, kbid: str, sentence: str) -> List[float]:
        if self.dummy:
            self.calls.append(sentence)
            if os.environ.get("TEST_SENTENCE_ENCODER") == "multilingual-2023-02-21":
                return Qm2023
            else:
                return Q

        if self.onprem is False:
            # Upload the payload
            resp = await self.session.get(
                url=f"{self.cluster_url}{PRIVATE_PREDICT}{SENTENCE}?text={sentence}",
                headers={"X-STF-KBID": kbid},
            )
        else:
            if self.nuclia_service_account is None:
                logger.warning(
                    "Nuclia Service account is not defined so could not retrieve vectors for the query"
                )
                return []
            # Upload the payload
            headers = {"X-STF-NUAKEY": f"Bearer {self.nuclia_service_account}"}
            resp = await self.session.get(
                url=f"{self.public_url}{PUBLIC_PREDICT}{SENTENCE}?text={sentence}",
                headers=headers,
            )
        await self.check_response(resp, expected=200)
        data = await resp.json()
        if len(data["data"]) == 0:
            raise PredictVectorMissing()
        return data["data"]

    @predict_observer.wrap({"type": "entities"})
    async def detect_entities(self, kbid: str, sentence: str) -> List[RelationNode]:
        # If token is offered
        if self.dummy:
            self.calls.append(sentence)
            dummy_data = os.environ.get("TEST_RELATIONS", None)
            if dummy_data is not None:
                return convert_relations(json.loads(dummy_data))
            else:
                return DUMMY_RELATION_NODE

        if self.onprem is False:
            # Upload the payload
            resp = await self.session.get(
                url=f"{self.cluster_url}{PRIVATE_PREDICT}{TOKENS}?text={sentence}",
                headers={"X-STF-KBID": kbid},
            )
        else:
            if self.nuclia_service_account is None:
                logger.warning(
                    "Nuclia Service account is not defined so could not retrieve entities from the query"
                )
                return []
            # Upload the payload
            headers = {"X-STF-NUAKEY": f"Bearer {self.nuclia_service_account}"}
            resp = await self.session.get(
                url=f"{self.public_url}{PUBLIC_PREDICT}{TOKENS}?text={sentence}",
                headers=headers,
            )
        await self.check_response(resp, expected=200)
        data = await resp.json()

        return convert_relations(data)
