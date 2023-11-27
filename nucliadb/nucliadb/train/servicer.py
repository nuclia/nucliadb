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
import traceback
from typing import Optional

import aiohttp
from nucliadb_protos.knowledgebox_pb2 import Labels
from nucliadb_protos.train_pb2 import (
    GetFieldsRequest,
    GetInfoRequest,
    GetLabelsetsCountRequest,
    GetParagraphsRequest,
    GetResourcesRequest,
    GetSentencesRequest,
    LabelsetsCount,
    TrainInfo,
)
from nucliadb_protos.writer_pb2 import (
    GetEntitiesRequest,
    GetEntitiesResponse,
    GetLabelsRequest,
    GetLabelsResponse,
)

from nucliadb.train.settings import settings
from nucliadb.train.utils import get_shard_manager
from nucliadb_protos import train_pb2_grpc
from nucliadb_telemetry import errors


class TrainServicer(train_pb2_grpc.TrainServicer):
    async def initialize(self):
        self.session = aiohttp.ClientSession()
        self.proc = get_shard_manager()  # XXX this is odd use here

    async def finalize(self):
        try:
            await self.session.close()
        except Exception:
            pass

    async def GetSentences(self, request: GetSentencesRequest, context=None):
        async for sentence in self.proc.kb_sentences(request):
            yield sentence

    async def GetParagraphs(self, request: GetParagraphsRequest, context=None):
        async for paragraph in self.proc.kb_paragraphs(request):
            yield paragraph

    async def GetFields(self, request: GetFieldsRequest, context=None):
        async for field in self.proc.kb_fields(request):
            yield field

    async def GetResources(self, request: GetResourcesRequest, context=None):
        async for resource in self.proc.kb_resources(request):
            yield resource

    async def GetInfo(self, request: GetInfoRequest, context=None):  # type: ignore
        result = TrainInfo()
        url = settings.internal_counter_api.format(kbid=request.kb.uuid)
        headers = {"X-NUCLIADB-ROLES": "READER"}
        resp = await self.session.get(url, headers=headers)
        resp.raise_for_status()
        data = await resp.json()
        result.resources = data["resources"]
        result.paragraphs = data["paragraphs"]
        result.fields = data["fields"]
        result.sentences = data["sentences"]
        return result

    async def GetEntities(  # type: ignore
        self, request: GetEntitiesRequest, context=None
    ) -> GetEntitiesResponse:
        kbid = request.kb.uuid
        response = GetEntitiesResponse()
        txn = await self.proc.driver.begin()

        entities_manager = await self.proc.get_kb_entities_manager(txn, kbid)
        if entities_manager is None:
            await txn.abort()
            response.status = GetEntitiesResponse.Status.NOTFOUND
            return response

        try:
            await entities_manager.get_entities(response)
        except Exception as e:
            errors.capture_exception(e)
            traceback.print_exc()
            response.status = GetEntitiesResponse.Status.ERROR
        else:
            response.kb.uuid = kbid
            response.status = GetEntitiesResponse.Status.OK

        await txn.abort()
        return response

    async def GetOntology(  # type: ignore
        self, request: GetLabelsRequest, context=None
    ) -> GetLabelsResponse:
        txn = await self.proc.driver.begin()
        kbobj = await self.proc.get_kb_obj(txn, request.kb.uuid)
        labels: Optional[Labels] = None
        if kbobj is not None:
            labels = await kbobj.get_labels()
        await txn.abort()
        response = GetLabelsResponse()
        if kbobj is None:
            response.status = GetLabelsResponse.Status.NOTFOUND
        else:
            response.kb.uuid = kbobj.kbid
            if labels is not None:
                response.labels.CopyFrom(labels)

        return response

    async def GetOntologyCount(  # type: ignore
        self, request: GetLabelsetsCountRequest, context=None
    ) -> LabelsetsCount:
        url = settings.internal_search_api.format(kbid=request.kb.uuid)
        facets = [f"faceted=/p/{labelset}" for labelset in request.paragraph_labelsets]
        facets.extend(
            [f"faceted=/l/{labelset}" for labelset in request.resource_labelsets]
        )
        query = "&".join(facets)
        headers = {"X-NUCLIADB-ROLES": "READER"}
        async with aiohttp.ClientSession() as sess:
            async with sess.get(f"{url}?{query}", headers=headers) as resp:
                data = await resp.json()
                data.get("paragraphs", {})

        res = LabelsetsCount()
        for labelset, labels in data["paragraphs"]["facets"].items():
            for label in labels["facetresults"]:
                label_tag = "/".join(label["tag"].split("/")[3:])
                res.labelsets[labelset].paragraphs[label_tag] = label["total"]

        for labelset, labels in data["fulltext"]["facets"].items():
            for label in labels["facetresults"]:
                label_tag = "/".join(label["tag"].split("/")[3:])
                res.labelsets[labelset].resources[label_tag] = label["total"]
        return res
