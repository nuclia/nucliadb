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

import nats
import pytest
from grpc.aio import AioRpcError  # type: ignore
from nucliadb_protos.noderesources_pb2 import Resource, Shard, ShardId
from nucliadb_protos.nodewriter_pb2 import IndexedMessage, IndexMessage, TypeMessage
from nucliadb_utils.utilities import get_storage

from nucliadb_node import SERVICE_NAME, shadow_shards
from nucliadb_node.pull import Worker
from nucliadb_node.settings import indexing_settings, settings
from nucliadb_node.shadow_shards import OperationCode

TEST_PARTITION = "111"


@pytest.mark.asyncio
async def test_indexing(worker, shard: str, reader):
    node = settings.force_host_id

    resource = resource_payload(shard)
    index = await create_indexing_message(resource, "kb", shard, node)  # type: ignore
    await send_indexing_message(worker, index, node)  # type: ignore

    sipb = ShardId()
    sipb.id = shard

    pbshard: Optional[Shard] = await reader.get_shard(sipb)
    if pbshard is not None and pbshard.resources > 0:
        processed = True
    else:
        processed = False

    while processed is False:  # pragma: no cover
        await asyncio.sleep(0.1)
        pbshard = await reader.get_shard(sipb)
        if pbshard is not None and pbshard.resources > 0:
            processed = True
        else:
            processed = False

    assert pbshard is not None
    assert pbshard.resources == 2
    await asyncio.sleep(0.1)

    storage = await get_storage(service_name=SERVICE_NAME)
    # should still work because we leave it around now
    assert await storage.get_indexing(index) is not None


@pytest.mark.asyncio
async def test_indexing_not_found(worker, reader):
    node = settings.force_host_id
    shard = "fake-shard"

    resource = resource_payload(shard)
    index = await create_indexing_message(resource, "kb", shard, node)
    await send_indexing_message(worker, index, node)  # type: ignore

    sipb = ShardId()
    sipb.id = shard

    with pytest.raises(AioRpcError):
        await reader.get_shard(sipb)


@pytest.mark.asyncio
async def test_indexing_shadow_shard(data_path, worker, shadow_shard: str):
    storage = await get_storage(service_name=SERVICE_NAME)
    node_id = settings.force_host_id

    # Add a set resource operation
    pb = resource_payload(shadow_shard)
    setpb = await create_indexing_message(pb, "kb", shadow_shard, node_id)  # type: ignore
    await send_indexing_message(worker, setpb, node_id)  # type: ignore

    # Add a delete operation
    deletepb = delete_indexing_message("bar", shadow_shard, node_id)  # type: ignore
    await send_indexing_message(worker, deletepb, node_id)  # type: ignore

    # Check that they were stored in a shadow shard
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
    assert await storage.get_indexing(setpb) is not None


@pytest.mark.asyncio
async def test_indexing_publishes_to_sidecar_index_stream(worker, shard: str, natsd):
    node_id = settings.force_host_id
    assert node_id

    indexpb = await create_indexing_message(
        resource_payload(shard), "kb", shard, node_id
    )

    await send_indexing_message(worker, indexpb, node_id)  # type: ignore

    msg = await get_indexed_message(natsd)

    assert msg.subject == f"indexed.{TEST_PARTITION}"
    indexedpb = IndexedMessage()
    indexedpb.ParseFromString(msg.data)
    assert indexedpb.node == indexpb.node
    assert indexedpb.shard == indexpb.shard
    assert indexedpb.txid == indexpb.txid
    assert indexedpb.typemessage == indexpb.typemessage
    assert indexedpb.reindex_id == indexpb.reindex_id


def resource_payload(shard: str) -> Resource:
    pb = Resource()
    pb.shard_id = shard
    pb.resource.shard_id = shard
    pb.resource.uuid = "uuid"
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


def delete_indexing_message(uuid: str, shard: str, node_id: str):
    deletepb: IndexMessage = IndexMessage()
    deletepb.node = node_id
    deletepb.shard = shard
    deletepb.txid = 123
    deletepb.resource = uuid
    deletepb.typemessage = TypeMessage.DELETION
    return deletepb


async def create_indexing_message(
    resource: Resource, kb: str, shard: str, node: str
) -> IndexMessage:
    storage = await get_storage(service_name=SERVICE_NAME)
    index: IndexMessage = await storage.indexing(
        resource, txid=1, partition=TEST_PARTITION, kb=kb, logical_shard=shard
    )
    index.node = node
    index.shard = shard
    return index


async def send_indexing_message(worker: Worker, index: IndexMessage, node: str):
    # Push on stream
    assert indexing_settings.index_jetstream_target is not None
    await worker.js.publish(
        indexing_settings.index_jetstream_target.format(node=node),
        index.SerializeToString(),
    )


async def get_indexed_message(natsd):
    nc = await nats.connect(servers=[natsd])
    future = asyncio.Future()

    async def cb(msg):
        nonlocal future
        future.set_result(msg)

    js = nc.jetstream()
    await js.subscribe(
        stream=indexing_settings.indexed_jetstream_stream,
        subject=indexing_settings.indexed_jetstream_target.format(
            partition=TEST_PARTITION
        ),
        queue="indexed-{partition}".format(partition=TEST_PARTITION),
        cb=cb,
        config=nats.js.api.ConsumerConfig(
            deliver_policy=nats.js.api.DeliverPolicy.NEW,
        ),
    )
    msg = None
    try:
        msg = await asyncio.wait_for(future, 1)
    except TimeoutError:  # pragma: no cover
        pass
    finally:
        await nc.flush()
        await nc.close()
        return msg
