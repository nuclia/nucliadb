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

import asyncio
import base64
import tarfile
import tempfile
from enum import Enum
from io import BytesIO
from typing import TYPE_CHECKING, AsyncIterator, List, Optional
from uuid import uuid4

from nucliadb_protos.resources_pb2 import CloudFile
from nucliadb_protos.writer_pb2 import (
    BinaryData,
    BrokerMessage,
    ExportRequest,
    FileRequest,
    GetEntitiesRequest,
    GetEntitiesResponse,
    GetLabelsRequest,
    GetLabelsResponse,
    SetEntitiesRequest,
    SetLabelsRequest,
    UploadBinaryData,
)

from nucliadb_client.utils import collect_cfs
from nucliadb_models.resource import KnowledgeBoxObj, ResourceList
from nucliadb_models.search import (
    KnowledgeboxCounters,
    KnowledgeboxSearchResults,
    KnowledgeboxShards,
    SearchRequest,
)
from nucliadb_models.writer import CreateResourcePayload, ResourceCreated

if TYPE_CHECKING:
    from nucliadb_client.client import NucliaDBClient

import aiofiles
import httpx

from nucliadb_client.resource import Resource

KB_PREFIX = "kb"


class CODEX(str, Enum):
    RESOURCE = "RES:"
    LABELS = "LAB:"
    ENTITIES = "ENT:"


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
            base_url=f"{client.http_writer_v1.base_url}{KB_PREFIX}/{kbid}",
            headers={"X-NUCLIADB-ROLES": "WRITER"},
            follow_redirects=True,
        )
        self.http_search_v1 = httpx.Client(
            base_url=f"{client.http_search_v1.base_url}{KB_PREFIX}/{kbid}",
            headers={"X-NUCLIADB-ROLES": "READER"},
            follow_redirects=True,
        )
        self.http_manager_v1 = httpx.Client(
            base_url=f"{client.http_manager_v1.base_url}{KB_PREFIX}/{kbid}",
            headers={"X-NUCLIADB-ROLES": "MANAGER"},
            follow_redirects=True,
        )
        self.slug = slug

    def get(self) -> KnowledgeBoxObj:
        response = self.http_manager_v1.get("").content
        return KnowledgeBoxObj.parse_raw(response)

    def counters(self) -> Optional[KnowledgeboxCounters]:
        response = self.http_search_v1.get(
            "counters", headers={"X-NUCLIADB-ROLES": "MANAGER"}
        )
        if response.status_code != 200:
            return None
        return KnowledgeboxCounters.parse_raw(response.content)

    def shards(self) -> KnowledgeboxShards:
        response = self.http_search_v1.get(
            "shards", headers={"X-NUCLIADB-ROLES": "MANAGER"}
        )
        assert response.status_code == 200
        return KnowledgeboxShards.parse_raw(response.content)

    def list_resources(self, page: int = 0, size: int = 20) -> List[Resource]:
        response = self.http_reader_v1.get(f"resources?page={page}&size={size}")
        assert response.status_code == 200
        response_obj = ResourceList.parse_raw(response.content)
        result = []
        for resource in response_obj.resources:
            result.append(Resource(rid=resource.id, kb=self, slug=resource.slug))
        return result

    def iter_resources(self, page_size: int = 20):
        page = 0
        last_page = False
        while not last_page:
            resources = self.list_resources(page=page, size=page_size)
            for resource in resources:
                yield resource
            if len(resources) == 0:
                last_page = True
            page = page + 1

    def create_resource(self, payload: CreateResourcePayload) -> Resource:
        response = self.http_writer_v1.post(
            f"resources", content=payload.json().encode()
        )
        response_obj = ResourceCreated.parse_raw(response.content)
        return Resource(rid=response_obj.uuid, kb=self)

    def search(self, query: str) -> KnowledgeboxSearchResults:
        payload = SearchRequest()
        payload.query = query
        response = self.http_search_v1.post(f"search", content=payload.json().encode())
        assert response.status_code == 200
        return KnowledgeboxSearchResults.parse_raw(response.content)

    def delete(self):
        resp = self.http_manager_v1.delete("")
        return resp.status_code == 200

    async def import_tar_bz2(self, filename):
        with tarfile.open(filename, mode="r:bz2") as tar:
            for member in tar.getmembers():
                buffer = tar.extractfile(member.name)

                async def upload_generator(buffer: BytesIO, member: tarfile.TarInfo):
                    chunk_size = 1_000_000
                    buffer.seek(0)
                    count = 0
                    ubd = UploadBinaryData()
                    ubd.count = count
                    ubd.metadata.size = member.size
                    ubd.metadata.kbid = self.kbid

                    # Replace the exported kbid from the key with the kbid we are importing to
                    exported_key = member.name
                    exported_kbid = exported_key.split("/")[1]
                    ubd.metadata.key = exported_key.replace(exported_kbid, self.kbid, 1)

                    yield ubd

                    data = buffer.read(chunk_size)
                    while data != b"":
                        count += 1
                        ubd = UploadBinaryData()
                        ubd.count = count
                        ubd.payload = data
                        data = buffer.read(chunk_size)
                        yield ubd

                await self.client.writer_stub_async.UploadFile(upload_generator(buffer, member))  # type: ignore

    async def import_export(self, line: str):
        type_line = line[:4]
        payload = base64.b64decode(line[4:])
        if type_line == CODEX.RESOURCE:
            pb_bm = BrokerMessage()
            pb_bm.ParseFromString(payload)
            res = Resource(rid=pb_bm.uuid, kb=self, slug=pb_bm.basic.slug)
            res._bm = pb_bm
            await res.commit()
        elif type_line == CODEX.ENTITIES:
            pb_er = GetEntitiesResponse()
            pb_er.ParseFromString(payload)
            for group, entities in pb_er.groups.items():
                ser_pb = SetEntitiesRequest()
                ser_pb.kb.uuid = self.kbid
                ser_pb.group = group
                ser_pb.entities.CopyFrom(entities)
                await self.client.writer_stub_async.SetEntities(ser_pb)  # type:  ignore
        elif type_line == CODEX.LABELS:
            pb_lr = GetLabelsResponse()
            pb_lr.ParseFromString(payload)
            for labelset, labelset_obj in pb_lr.labels.labelset.items():
                slr_pb = SetLabelsRequest()
                slr_pb.kb.uuid = self.kbid
                slr_pb.id = labelset
                slr_pb.labelset.CopyFrom(labelset_obj)
                await self.client.writer_stub_async.SetLabels(slr_pb)  # type:  ignore

    async def resources(self) -> AsyncIterator[BrokerMessage]:
        assert self.client.writer_stub_async
        req = ExportRequest()
        req.kbid = self.kbid
        async for bm in self.client.writer_stub_async.Export(req):  # type: ignore
            yield bm

    async def entities(self) -> GetEntitiesResponse:
        assert self.client.writer_stub_async
        req = GetEntitiesRequest()
        req.kb.uuid = self.kbid
        entities_response: GetEntitiesResponse = await self.client.writer_stub_async.GetEntities(req)  # type: ignore
        return entities_response

    async def labels(self) -> GetLabelsResponse:
        assert self.client.writer_stub_async
        req = GetLabelsRequest()
        req.kb.uuid = self.kbid
        label_response: GetLabelsResponse = (
            await self.client.writer_stub_async.GetLabels(req)  # type: ignore
        )
        return label_response

    async def download_file(self, cf: CloudFile, destination: str):
        assert self.client.writer_stub_async
        req = FileRequest()
        if cf.bucket_name is not None:
            req.bucket = cf.bucket_name
        if cf.uri is not None:
            req.key = cf.uri
        async with aiofiles.open(destination, "wb") as download_file_obj:
            data: BinaryData
            async for data in self.client.writer_stub_async.DownloadFile(req):  # type: ignore
                await download_file_obj.write(data.data)

    def init_async_grpc(self):
        self.client.init_async_grpc()

    async def generator(self, binaries: List[CloudFile]) -> AsyncIterator[str]:
        self.init_async_grpc()
        async for bm in self.resources():
            collect_cfs(bm, binaries)

            yield CODEX.RESOURCE + base64.b64encode(
                bm.SerializeToString()
            ).decode() + "\n"
        entities = await self.entities()
        yield CODEX.ENTITIES + base64.b64encode(
            entities.SerializeToString()
        ).decode() + "\n"
        labels = await self.labels()
        yield CODEX.LABELS + base64.b64encode(
            labels.SerializeToString()
        ).decode() + "\n"

    async def export(self, dump: str):
        """
        Write all exported resources, labels and entities into the `dump` file.
        Then download all the binaries into a `{dump}.tar.bz2` file.
        """
        binaries: List[CloudFile] = []
        loop = asyncio.get_running_loop()
        async with aiofiles.open(f"{dump}", "w+") as dump_file:
            async for line in self.generator(binaries):
                await dump_file.write(line)

            with tempfile.TemporaryDirectory() as tempfolder:
                filename = f"{tempfolder}/{uuid4().hex}"
                with tarfile.open(f"{dump}.tar.bz2", mode="w:bz2") as tar:
                    for cf in binaries:
                        await self.download_file(cf, filename)
                        await loop.run_in_executor(
                            None,
                            tar.add,
                            filename,
                            cf.uri,
                        )
