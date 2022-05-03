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
from typing import Any, Dict, List, Optional

import aiohttp


class SendToPredictError(Exception):
    pass


PUBLIC_PREDICT = "/api/v1/predict"
PRIVATE_PREDICT = "/api/internal/predict"
SENTENCE = "/sentence"


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
        self.public_url = public_url
        self.zone = zone
        self.onprem = onprem
        self.dummy = dummy
        self.calls: List[Dict[str, Any]] = []

    async def initialize(self):
        self.session = aiohttp.ClientSession()

    async def finalize(self):
        await self.session.close()

    async def convert_sentence_to_vector(self, kbid: str, sentence: str) -> List[float]:
        # If token is offered
        if self.dummy:
            self.calls.append(sentence)
            return [0] * 768

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
            # Upload the payload
            headers = {"Authorization": f"Bearer {self.nuclia_service_account}"}
            resp = await self.session.get(
                url=f"{self.public_url}{PUBLIC_PREDICT}{SENTENCE}?text={sentence}",
                headers=headers,
            )
            if resp.status == 200:
                data = await resp.json()
            else:
                raise SendToPredictError(f"{resp.status}: {await resp.read()}")
        return data["data"]
