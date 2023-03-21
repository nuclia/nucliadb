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
from typing import AsyncIterator, List, Optional

import aiohttp
from nucliadb_protos.utils_pb2 import RelationNode

from nucliadb.ingest.tests.vectors import Q
from nucliadb.search import logger
from nucliadb_models.search import ChatModel, FeedbackRequest


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


PUBLIC_PREDICT = "/api/v1/predict"
PRIVATE_PREDICT = "/api/internal/predict"
SENTENCE = "/sentence"
TOKENS = "/tokens"
CHAT = "/chat"
FEEDBACK = "/feedback"


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
        # TODO: Should we accumulate sentences requested,
        # or is this only for testing purposes?
        self.calls: List[str] = []

    async def initialize(self):
        self.session = aiohttp.ClientSession()

    async def finalize(self):
        await self.session.close()

    async def send_feedback(
        self,
        kbid: str,
        item: FeedbackRequest,
        x_nucliadb_user: str,
        x_ndb_client: str,
        x_forwarded_for: str,
    ):
        data = item.dict()
        data["user"] = x_nucliadb_user
        data["client"] = x_ndb_client
        data["forwarded"] = x_forwarded_for

        if self.onprem is False:
            # Upload the payload
            resp = await self.session.post(
                url=f"{self.cluster_url}{PRIVATE_PREDICT}{CHAT}",
                json=json.dumps(data),
                headers={"X-STF-KBID": kbid},
            )
            if resp.status != 200:
                raise SendToPredictError(f"{resp.status}: {await resp.read()}")
        else:
            if self.nuclia_service_account is None:
                logger.warning(
                    "Nuclia Service account is not defined so could not retrieve vectors for the query"
                )
                return []
            # Upload the payload
            headers = {"X-STF-NUAKEY": f"Bearer {self.nuclia_service_account}"}
            resp = await self.session.post(
                url=f"{self.public_url}{PUBLIC_PREDICT}{CHAT}",
                json=json.dumps(data),
                headers=headers,
            )
            if resp.status != 200:
                raise SendToPredictError(f"{resp.status}: {await resp.read()}")

    async def chat_query(self, kbid: str, item: ChatModel) -> AsyncIterator[bytes]:
        # If token is offered

        if self.onprem is False:
            # Upload the payload
            resp = await self.session.post(
                url=f"{self.cluster_url}{PRIVATE_PREDICT}{CHAT}",
                json=item.dict(),
                headers={"X-STF-KBID": kbid},
            )
            if resp.status != 200:
                raise SendToPredictError(f"{resp.status}: {await resp.read()}")
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
            if resp.status != 200:
                raise SendToPredictError(f"{resp.status}: {await resp.read()}")
        async for data in resp.content.iter_any():
            yield data

    async def convert_sentence_to_vector(self, kbid: str, sentence: str) -> List[float]:
        # If token is offered
        if self.dummy:
            self.calls.append(sentence)
            return Q

        if self.onprem is False:
            # Upload the payload
            resp = await self.session.get(
                url=f"{self.cluster_url}{PRIVATE_PREDICT}{SENTENCE}?text={sentence}",
                headers={"X-STF-KBID": kbid},
            )
            if resp.status == 200:
                data = await resp.json()
            else:
                raise SendToPredictError(f"{resp.status}: {await resp.read()}")
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
            if resp.status == 200:
                data = await resp.json()
            else:
                raise SendToPredictError(f"{resp.status}: {await resp.read()}")
        if len(data["data"]) == 0:
            raise PredictVectorMissing()
        return data["data"]

    async def detect_entities(self, kbid: str, sentence: str) -> List[RelationNode]:
        # If token is offered
        if self.dummy:
            self.calls.append(sentence)
            return DUMMY_RELATION_NODE

        if self.onprem is False:
            # Upload the payload
            resp = await self.session.get(
                url=f"{self.cluster_url}{PRIVATE_PREDICT}{TOKENS}?text={sentence}",
                headers={"X-STF-KBID": kbid},
            )
            if resp.status == 200:
                data = await resp.json()
            else:
                raise SendToPredictError(f"{resp.status}: {await resp.read()}")
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
            if resp.status == 200:
                data = await resp.json()
            else:
                raise SendToPredictError(f"{resp.status}: {await resp.read()}")

        result = []
        for token in data["tokens"]:
            text = token["text"]
            klass = token["ner"]
            result.append(
                RelationNode(
                    value=text, ntype=RelationNode.NodeType.ENTITY, subtype=klass
                )
            )
        return result
