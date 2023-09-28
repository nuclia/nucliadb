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
import uuid
from datetime import datetime
from typing import Optional

import pytest
from grpc.aio import AioRpcError  # type: ignore
from nucliadb_protos.noderesources_pb2 import Resource, Shard, ShardId
from nucliadb_protos.nodewriter_pb2 import IndexMessage, TypeMessage
from nucliadb_protos.writer_pb2 import Notification

from nucliadb_node import SERVICE_NAME
from nucliadb_node.pull import Worker
from nucliadb_node.settings import settings
from nucliadb_utils import const
from nucliadb_utils.utilities import get_pubsub, get_storage

TEST_PARTITION = "111"


@pytest.mark.asyncio
@pytest.mark.parametrize("shard", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_indexing(worker, shard: str, reader):
    node = settings.force_host_id

    resource = resource_payload(shard)
    index = await create_indexing_message(resource, "kb", shard, node)  # type: ignore
    await send_indexing_message(worker, index, node)  # type: ignore

    sipb = ShardId()
    sipb.id = shard

    pbshard: Optional[Shard] = await reader.get_shard(sipb)
    if pbshard is not None and pbshard.fields > 0:
        processed = True
    else:
        processed = False

    while processed is False:  # pragma: no cover
        await asyncio.sleep(0.1)
        pbshard = await reader.get_shard(sipb)
        if pbshard is not None and pbshard.fields > 0:
            processed = True
        else:
            processed = False

    assert pbshard is not None
    assert pbshard.fields == 2
    await asyncio.sleep(0.1)

    storage = await get_storage(service_name=SERVICE_NAME)
    # should still work because we leave it around now
    assert await storage.get_indexing(index) is not None


@pytest.mark.asyncio
async def test_indexing_not_found(worker, reader):
    node = settings.force_host_id
    shard = "fake-shard"

    with pytest.raises(AioRpcError):
        await reader.get_shard(ShardId(id=shard))

    resource = resource_payload(shard)
    index = await create_indexing_message(resource, "kb", shard, node)
    await send_indexing_message(worker, index, node)  # type: ignore

    # Make sure message is consumed even if the shard doesn't exist
    await wait_for_indexed_message("kb")


@pytest.mark.asyncio
@pytest.mark.parametrize("shard", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_indexing_publishes_to_sidecar_index_stream(worker, shard: str, natsd):
    node_id = settings.force_host_id
    assert node_id

    indexpb = await create_indexing_message(
        resource_payload(shard), "kb", shard, node_id
    )

    waiting_task = asyncio.create_task(wait_for_indexed_message("kb"))
    await asyncio.sleep(0.1)

    await send_indexing_message(worker, indexpb, node_id)  # type: ignore

    msg = await waiting_task
    assert msg is not None, "Message has not been received"

    assert msg.subject == const.PubSubChannels.RESOURCE_NOTIFY.format(kbid="kb")
    notification = Notification()
    notification.ParseFromString(msg.data)
    assert notification.partition == int(indexpb.partition)
    assert notification.seqid == indexpb.txid
    assert notification.uuid == indexpb.resource
    assert notification.kbid == indexpb.kbid


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
    await worker.js.publish(
        const.Streams.INDEX.subject.format(node=node),
        index.SerializeToString(),
    )


async def wait_for_indexed_message(kbid: str):
    pubsub = await get_pubsub()
    assert pubsub is not None
    future = asyncio.Future()  # type: ignore
    request_id = str(uuid.uuid4())

    async def cb(msg):
        nonlocal future
        future.set_result(msg)

    await pubsub.subscribe(
        handler=cb,
        key=const.PubSubChannels.RESOURCE_NOTIFY.format(kbid=kbid),
        subscription_id=request_id,
    )
    msg = None
    try:
        msg = await asyncio.wait_for(future, 10)
    except TimeoutError:  # pragma: no cover
        print("Timeout while waiting for resource indexing notification")
    finally:
        return msg
