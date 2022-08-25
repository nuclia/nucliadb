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
from datetime import datetime

import pytest
from grpc.aio import AioRpcError  # type: ignore
from nucliadb_protos.noderesources_pb2 import Resource, ShardId
from nucliadb_protos.nodewriter_pb2 import IndexMessage

from nucliadb_node import SERVICE_NAME
from nucliadb_node.app import App
from nucliadb_node.settings import settings
from nucliadb_utils.settings import indexing_settings
from nucliadb_utils.utilities import get_storage


@pytest.mark.asyncio
async def test_indexing(sidecar: App, shard: str):
    # Upload a payload

    pb = Resource()
    pb.shard_id = shard
    pb.resource.shard_id = shard
    pb.resource.uuid = "1"
    pb.metadata.modified.FromDatetime(datetime.now())
    pb.metadata.created.FromDatetime(datetime.now())
    pb.texts["title"].text = "My title"
    pb.texts["title"].labels.extend(["/c/label1", "/c/label2"])
    pb.texts["description"].text = "My description is amazing"
    pb.texts["description"].labels.extend(["/c/label3", "/c/label4"])
    pb.status = Resource.ResourceStatus.PROCESSED
    pb.paragraphs["title"].paragraphs["title/0-10"].start = 0
    pb.paragraphs["title"].paragraphs["title/0-10"].end = 10
    pb.paragraphs["title"].paragraphs["title/0-10"].field = "title"
    pb.paragraphs["title"].paragraphs["title/0-10"].sentences[
        "title/0-10/0-10"
    ].vector.extend([1.0] * 768)

    # Create the message
    storage = await get_storage(service_name=SERVICE_NAME)
    assert settings.force_host_id is not None
    index: IndexMessage = await storage.indexing(pb, settings.force_host_id, shard, 1)

    # Push on stream
    assert indexing_settings.index_jetstream_target is not None
    await sidecar.worker.js.publish(
        indexing_settings.index_jetstream_target.format(node=settings.force_host_id),
        index.SerializeToString(),
    )

    sipb = ShardId()
    sipb.id = shard

    response = await sidecar.reader.get_count(sipb)
    if response is not None and response > 0:
        processed = True
    else:
        processed = False

    while processed is False:
        await asyncio.sleep(1)
        response = await sidecar.reader.get_count(sipb)
        if response is not None and response > 0:
            processed = True
        else:
            processed = False

    assert response == 2
    await asyncio.sleep(1)

    storage = await get_storage(service_name=SERVICE_NAME)
    with pytest.raises(KeyError):
        await storage.get_indexing(index)


@pytest.mark.asyncio
async def test_indexing_not_found(sidecar: App):
    # Upload a payload

    pb = Resource()
    pb.shard_id = "shard"
    pb.resource.shard_id = "shard"
    pb.resource.uuid = "1"
    pb.metadata.modified.FromDatetime(datetime.now())
    pb.metadata.created.FromDatetime(datetime.now())
    pb.texts["title"].text = "My title"
    pb.texts["title"].labels.extend(["/c/label1", "/c/label2"])
    pb.texts["description"].text = "My description is amazing"
    pb.texts["description"].labels.extend(["/c/label3", "/c/label4"])
    pb.status = Resource.ResourceStatus.PROCESSED
    pb.paragraphs["title"].paragraphs["title/0-10"].start = 0
    pb.paragraphs["title"].paragraphs["title/0-10"].end = 10
    pb.paragraphs["title"].paragraphs["title/0-10"].field = "title"
    pb.paragraphs["title"].paragraphs["title/0-10"].sentences[
        "title/0-10/0-10"
    ].vector.extend([1.0] * 768)

    # Create the message
    storage = await get_storage(service_name=SERVICE_NAME)
    assert settings.force_host_id is not None
    index: IndexMessage = await storage.indexing(pb, settings.force_host_id, "shard", 1)

    # Push on stream
    assert indexing_settings.index_jetstream_target is not None
    await sidecar.worker.js.publish(
        indexing_settings.index_jetstream_target.format(node=settings.force_host_id),
        index.SerializeToString(),
    )

    sipb = ShardId()
    sipb.id = "shard"

    with pytest.raises(AioRpcError):
        await sidecar.reader.get_count(sipb)
