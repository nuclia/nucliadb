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
from typing import Optional

import pytest
from grpc.aio import AioRpcError  # type: ignore
from nucliadb_protos.noderesources_pb2 import Resource, Shard, ShardId
from nucliadb_protos.nodewriter_pb2 import IndexMessage

from nucliadb_node import SERVICE_NAME, shadow_shards
from nucliadb_node.app import App
from nucliadb_node.settings import settings
from nucliadb_node.shadow_shards import OperationCode
from nucliadb_utils.settings import indexing_settings
from nucliadb_utils.utilities import get_storage


@pytest.mark.asyncio
async def test_indexing(sidecar: App, shard: str):
    node = settings.force_host_id
    assert node is not None

    resource = resource_payload(shard)
    index = await create_indexing_message(resource, shard)
    await send_indexing_message(sidecar, index, node)

    sipb = ShardId()
    sipb.id = shard

    pbshard: Optional[Shard] = await sidecar.reader.get_shard(sipb)
    if pbshard is not None and pbshard.resources > 0:
        processed = True
    else:
        processed = False

    while processed is False:
        await asyncio.sleep(0.1)
        pbshard = await sidecar.reader.get_shard(sipb)
        if pbshard is not None and pbshard.resources > 0:
            processed = True
        else:
            processed = False

    assert pbshard is not None
    assert pbshard.resources == 2
    await asyncio.sleep(0.1)

    storage = await get_storage(service_name=SERVICE_NAME)
    with pytest.raises(KeyError):
        await storage.get_indexing(index)


@pytest.mark.asyncio
async def test_indexing_not_found(sidecar: App):
    node = settings.force_host_id
    assert node is not None

    shard = "fake-shard"
    resource = resource_payload(shard)
    index = await create_indexing_message(resource, shard)
    await send_indexing_message(sidecar, index, node)

    sipb = ShardId()
    sipb.id = shard

    with pytest.raises(AioRpcError):
        await sidecar.reader.get_shard(sipb)


@pytest.mark.asyncio
async def test_indexing_shadow_shard(data_path, sidecar: App, shadow_shard: str):
    node_id = settings.force_host_id
    pb = resource_payload(shadow_shard)

    # Add a set resource operation
    storage = await get_storage(service_name=SERVICE_NAME)
    assert settings.force_host_id is not None
    index: IndexMessage = await storage.indexing(pb, node_id, shadow_shard, 1)  # type: ignore

    # Add a delete operation
    deletepb: IndexMessage = IndexMessage()
    deletepb.node = node_id  # type: ignore
    deletepb.shard = shadow_shard
    deletepb.txid = 123
    deletepb.resource = "bar"
    deletepb.typemessage = IndexMessage.TypeMessage.DELETION

    # Push them on stream
    assert indexing_settings.index_jetstream_target is not None
    await sidecar.worker.js.publish(
        indexing_settings.index_jetstream_target.format(node=node_id),
        index.SerializeToString(),
    )
    await sidecar.worker.js.publish(
        indexing_settings.index_jetstream_target.format(node=node_id),
        deletepb.SerializeToString(),
    )

    ssm = shadow_shards.get_manager()

    ops = []
    for _ in range(10):
        print("Waiting for sidecar to consume messages...")
        await asyncio.sleep(1)
        ops = [op async for op in ssm.iter_operations(shadow_shard)]
        if len(ops) == 2:
            break
    assert len(ops) == 2
    assert ops[0][0] == OperationCode.SET
    assert ops[0][1] == pb
    assert ops[1][0] == OperationCode.DELETE
    assert ops[1][1] == "bar"

    await asyncio.sleep(1)

    # Check that indexing messages have been deleted from storage
    storage = await get_storage(service_name=SERVICE_NAME)
    with pytest.raises(KeyError):
        await storage.get_indexing(index)


def resource_payload(shard: str) -> Resource:
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
    return pb


async def create_indexing_message(resource: Resource, shard: str) -> IndexMessage:
    storage = await get_storage(service_name=SERVICE_NAME)
    assert settings.force_host_id is not None
    index: IndexMessage = await storage.indexing(
        resource, settings.force_host_id, shard, 1
    )
    return index


async def send_indexing_message(sidecar: App, index: IndexMessage, node: str):
    # Push on stream
    assert indexing_settings.index_jetstream_target is not None
    await sidecar.worker.js.publish(
        indexing_settings.index_jetstream_target.format(node=node),
        index.SerializeToString(),
    )
