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
import logging

import httpx

from nucliadb_telemetry.metrics import Observer

logger = logging.getLogger(__name__)

pinecone_observer = Observer(
    "pinecone_client",
    labels={"type": ""},
)

BASE_URL = "https://api.pinecone.io/"


class PineconeClient:
    def __init__(self, api_key: str, http_session: httpx.AsyncClient):
        self.api_key = api_key
        self.session = http_session

    @pinecone_observer.wrap({"type": "create_index"})
    async def create_index(self, name: str, dimension: int) -> str:
        payload = {
            "name": name,
            "dimension": dimension,
            "metric": "dotproduct",
            "spec": {"serverless": {"cloud": "aws", "region": "us-east-1"}},
        }
        headers = {"Api-Key": self.api_key}
        response = await self.session.post("/indexes", json=payload, headers=headers)
        response.raise_for_status()
        response_json = response.json()
        return response_json["host"]

    @pinecone_observer.wrap({"type": "delete_index"})
    async def delete_index(self, name: str) -> None:
        headers = {"Api-Key": self.api_key}
        response = await self.session.delete(f"/indexes/{name}", headers=headers)
        if response.status_code == 404:
            logger.warning("Pinecone index not found.", extra={"index_name": name})
            return
        response.raise_for_status()


class PineconeSession:
    """
    Wrapper that manages the singletone session around all Pinecone http api interactions.
    """

    def __init__(self):
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        self.http_session = httpx.AsyncClient(base_url=BASE_URL, headers=self.headers)

    async def finalize(self):
        if self.http_session.is_closed:
            return
        await self.http_session.aclose()

    def get_client(self, api_key: str) -> PineconeClient:
        return PineconeClient(api_key=api_key, http_session=self.http_session)
