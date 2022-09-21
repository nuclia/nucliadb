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

import base64
from typing import TYPE_CHECKING, AsyncIterator, List, Optional

from nucliadb_protos.writer_pb2 import BrokerMessage, ExportRequest

from nucliadb_models.resource import KnowledgeBoxObj, ResourceList
from nucliadb_models.writer import CreateResourcePayload, ResourceCreated

if TYPE_CHECKING:
    from nucliadb_client.client import NucliaDBClient

import httpx

from nucliadb_client.resource import Resource

KB_PREFIX = "kb"


class KnowledgeBox:
    http_reader_v1: httpx.Client
    http_writer_v1: httpx.Client
    http_manager_v1: httpx.Client

    def __init__(self, kbid: str, client: "NucliaDBClient", slug: Optional[str] = None):
        self.kbid = kbid
        self.client = client
        self.http_reader_v1 = httpx.Client(
            base_url=f"{client.http_reader_v1.base_url}{KB_PREFIX}/{kbid}",
            headers={"X-NUCLIADB-ROLES": "READER"},
            follow_redirects=True,
        )
        self.http_writer_v1 = httpx.Client(
            base_url=f"{client.http_reader_v1.base_url}{KB_PREFIX}/{kbid}",
            headers={"X-NUCLIADB-ROLES": "WRITER"},
            follow_redirects=True,
        )
        self.http_manager_v1 = httpx.Client(
            base_url=f"{client.http_reader_v1.base_url}{KB_PREFIX}/{kbid}",
            headers={"X-NUCLIADB-ROLES": "MANAGER"},
            follow_redirects=True,
        )
        self.slug = slug

    def get(self) -> KnowledgeBoxObj:
        response = self.http_manager_v1.get("").content
        return KnowledgeBoxObj.parse_raw(response)

    def list_elements(self, page: int = 0, size: int = 20) -> List[Resource]:
        response = self.http_reader_v1.get(f"resources?page={page}&size={size}")
        response_obj = ResourceList.parse_raw(response.content)
        result = []
        for resource in response_obj.resources:
            result.append(Resource(rid=resource.id, kb=self))
        return result

    def create_resource(self, payload: CreateResourcePayload) -> Resource:
        response = self.http_writer_v1.post(
            f"resources", content=payload.json().encode()
        )
        response_obj = ResourceCreated.parse_raw(response.content)
        return Resource(rid=response_obj.uuid, kb=self)

    def delete(self):
        resp = self.http_manager_v1.delete("")
        return resp.status_code == 200

    def parse_bm(self, payload: bytes) -> Resource:
        pb = BrokerMessage()
        pb.ParseFromString(payload)
        res = Resource(rid=pb.uuid, kb=self)
        res._bm = pb
        return res

    async def export(self) -> AsyncIterator[str]:
        assert self.client.writer_stub_async
        req = ExportRequest()
        req.kbid = self.kbid
        async for bm in self.client.writer_stub_async.Export(req):  # type: ignore
            yield base64.b64encode(bm.SerializeToString()).decode()

    def init_async_grpc(self):
        self.client.init_async_grpc()
