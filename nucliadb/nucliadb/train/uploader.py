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
from typing import Optional

import aiohttp
from nucliadb_protos.knowledgebox_pb2 import Labels
from nucliadb_protos.train_pb2 import (
    EnabledMetadata,
    GetFieldsRequest,
    GetParagraphsRequest,
    GetResourcesRequest,
    GetSentencesRequest,
)
from nucliadb_protos.writer_pb2 import (
    GetEntitiesRequest,
    GetEntitiesResponse,
    GetLabelsRequest,
    GetLabelsResponse,
)

from nucliadb.ingest.orm.processor import Processor
from nucliadb.ingest.utils import get_driver
from nucliadb.train import SERVICE_NAME
from nucliadb.train.models import RequestData
from nucliadb.train.settings import settings
from nucliadb_utils.utilities import get_audit, get_cache, get_storage


class UploadServicer:
    async def initialize(self):
        storage = await get_storage(service_name=SERVICE_NAME)
        audit = get_audit()
        driver = await get_driver()
        cache = await get_cache()
        self.proc = Processor(driver=driver, storage=storage, audit=audit, cache=cache)
        await self.proc.initialize()

    async def finalize(self):
        await self.proc.finalize()

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
        for resource in self.proc.kb_resources(request):
            yield resource

    async def GetEntities(  # type: ignore
        self, request: GetEntitiesRequest, context=None
    ) -> GetEntitiesResponse:
        txn = await self.proc.driver.begin()
        kbobj = await self.proc.get_kb_obj(txn, request.kb)
        response = GetEntitiesResponse()
        if kbobj is not None:
            await kbobj.get_entities(response)
            response.kb.uuid = kbobj.kbid
            response.status = GetEntitiesResponse.Status.OK
        await txn.abort()
        if kbobj is None:
            response.status = GetEntitiesResponse.Status.NOTFOUND
        return response

    async def GetOntology(  # type: ignore
        self, request: GetLabelsRequest, context=None
    ) -> GetLabelsResponse:
        txn = await self.proc.driver.begin()
        kbobj = await self.proc.get_kb_obj(txn, request.kb)
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


async def start_upload(request: str, kb: str):
    us = UploadServicer()
    await us.initialize()

    url = settings.nuclia_learning_url

    if settings.nuclia_learning_apikey is None:
        raise AttributeError("API Key required for uploading")

    async with aiohttp.ClientSession(
        headers={
            "X-NUCLIA-LEARNING-APIKEY": settings.nuclia_learning_apikey,
            "X-NUCLIA-LEARNING-REQUEST": request,
        }
    ) as sess:
        req = await sess.get(f"{url}/request")
        request_data = RequestData.parse_raw(await req.read())

        metadata = EnabledMetadata(**request_data.metadata.dict())

        if request_data.sentences:
            pbsr = GetSentencesRequest()
            pbsr.kb.uuid = kb
            pbsr.metadata.CopyFrom(metadata)

            async for sentence in us.GetSentences(pbsr):
                payload = sentence.SerializeToString()
                await sess.post(f"{url}/sentence", data=payload)

        if request_data.paragraphs:
            pbpr = GetParagraphsRequest()
            pbpr.kb.uuid = kb
            pbpr.metadata.CopyFrom(metadata)

            async for paragraph in us.GetParagraphs(pbpr):
                payload = paragraph.SerializeToString()
                await sess.post(f"{url}/paragraph", data=payload)

        if request_data.resources:
            pbrr = GetResourcesRequest()
            pbrr.kb.uuid = kb
            pbrr.metadata.CopyFrom(metadata)

            async for resource in us.GetResources(pbrr):
                payload = resource.SerializeToString()
                await sess.post(f"{url}/resource", data=payload)

        if request_data.fields:
            pbfr = GetFieldsRequest()
            pbfr.kb.uuid = kb
            pbfr.metadata.CopyFrom(metadata)

            async for field in us.GetFields(pbfr):
                payload = field.SerializeToString()
                await sess.post(f"{url}/resource", data=payload)

        if request_data.entities:
            pber = GetEntitiesRequest()
            pber.kb.uuid = kb

            entities = await us.GetEntities(pber)
            payload = entities.SerializeToString()
            await sess.post(f"{url}/entities", data=payload)

        if request_data.labels:
            pblr = GetLabelsRequest()
            pblr.kb.uuid = kb

            ontology = await us.GetOntology(pblr)
            payload = ontology.SerializeToString()
            await sess.post(f"{url}/ontology", data=payload)

    await us.finalize()
