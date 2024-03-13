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
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import Optional
from unittest.mock import AsyncMock, patch

import pytest
from grpc.aio import AioRpcError
from nucliadb_protos.noderesources_pb2 import Resource, Shard, ShardId
from nucliadb_protos.nodewriter_pb2 import IndexMessage, IndexMessageSource, TypeMessage
from nucliadb_protos.writer_pb2 import Notification

from nucliadb_node import SERVICE_NAME
from nucliadb_node.pull import Worker
from nucliadb_node.settings import settings
from nucliadb_node.signals import SuccessfulIndexingPayload, successful_indexing
from nucliadb_node.tests.fixtures import Reader
from nucliadb_utils import const
from nucliadb_utils.utilities import get_pubsub, get_storage

TEST_PARTITION = "111"


@pytest.mark.asyncio
@pytest.mark.parametrize("shard", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_indexing(worker, shard: str, reader: Reader):
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

    start = datetime.now()
    while processed is False:  # pragma: no cover
        await asyncio.sleep(0.01)
        pbshard = await reader.get_shard(sipb)
        if pbshard is not None and pbshard.fields > 0:
            processed = True
        else:
            processed = False

        elapsed = datetime.now() - start
        assert elapsed.seconds < 10, "too much time waiting"

    assert pbshard is not None
    assert pbshard.fields == 2
    await asyncio.sleep(0.01)

    storage = await get_storage(service_name=SERVICE_NAME)
    # should still work because we leave it around now
    assert await storage.get_indexing(index) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("shard", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_prioritary_indexing_on_one_shard(
    worker,
    shard: str,
    reader: Reader,
):
    """Add some resources from the processor and some more from the writer.
    Validate the writer messages take priority over the processor ones.

    """
    node = settings.force_host_id
    assert node is not None

    resource = resource_payload(shard)
    processor_index = await create_indexing_message(
        resource, "kb", shard, node, source=IndexMessageSource.PROCESSOR
    )
    writer_index = await create_indexing_message(
        resource, "kb", shard, node, source=IndexMessageSource.WRITER
    )

    sources = []

    async def side_effect(pb: IndexMessage):
        nonlocal sources
        sources.append(pb.source)
        await asyncio.sleep(0.01)
        return True

    mock = AsyncMock(side_effect=side_effect)
    with patch("nucliadb_node.indexer.PriorityIndexer._index_message", new=mock):
        for i in range(3):
            await send_indexing_message(worker, processor_index, node)  # type: ignore
        for i in range(3):
            await send_indexing_message(worker, writer_index, node)  # type: ignore

        processing = True
        start = datetime.now()
        while processing:
            await asyncio.sleep(0.01)
            processing = mock.await_count < 6

            elapsed = datetime.now() - start
            assert elapsed.seconds < 10, "too much time waiting"

    assert sources == [
        IndexMessageSource.PROCESSOR,
        IndexMessageSource.WRITER,
        IndexMessageSource.WRITER,
        IndexMessageSource.WRITER,
        IndexMessageSource.PROCESSOR,
        IndexMessageSource.PROCESSOR,
    ]


class TestConcurrentIndexingFailureRecovery:
    """Index messages from writer and from processor into a bunch of shards.

    Make some percentage of the messages fail while indexing and validate
    failure recovery and correct processing without suplicates or missing
    messages.

    """

    @dataclass
    class IndexLogEntry:
        shard_id: str
        seqid: int
        source: str

    @staticmethod
    @contextmanager
    def successful_indexing_context(listener_id: str, async_cb):
        successful_indexing.add_listener(listener_id, async_cb)
        yield
        successful_indexing.remove_listener(listener_id)

    @pytest.fixture
    def node_id(self, worker):
        node = settings.force_host_id
        assert node is not None
        yield node

    @pytest.fixture
    def index_log(self):
        index_log = []

        async def on_indexing(payload: SuccessfulIndexingPayload):
            nonlocal index_log
            entry = self.IndexLogEntry(
                shard_id=payload.index_message.shard,
                seqid=payload.seqid,
                source=IndexMessageSource.Name(payload.index_message.source),
            )
            index_log.append(entry)

        with self.successful_indexing_context(
            "test_concurrent_indexing_failure_recovery", on_indexing
        ):
            yield index_log

    @pytest.fixture
    def faulty_worker(self, worker):
        fail_ratio = 0.1
        work_done_count = 0

        async def run_or_fail(func, fail_ratio: float, *args, **kwargs):
            nonlocal work_done_count
            work_done_count += 1
            if work_done_count % round(1 / fail_ratio) == 0:
                raise Exception("Intended exception to test failure recovery")
            else:
                await func(*args, **kwargs)

        original_set_resource = worker.writer.set_resource
        worker.writer.set_resource = partial(
            run_or_fail, original_set_resource, fail_ratio
        )
        original_delete_resource = worker.writer.delete_resource
        worker.writer.delete_resource = partial(
            run_or_fail, original_delete_resource, fail_ratio
        )

        yield worker

        worker.writer.set_resource = original_set_resource
        worker.writer.delete_resource = original_delete_resource

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "bunch_of_shards", ("EXPERIMENTAL", "STABLE"), indirect=True
    )
    async def test(
        self,
        bunch_of_shards: list[str],
        node_id: str,
        reader: Reader,
        faulty_worker,
        index_log,
    ):
        resources_per_shard = 10
        seqids_by_shard: dict[str, list[int]] = {
            shard_id: [] for shard_id in bunch_of_shards
        }

        for shard_id in bunch_of_shards:
            for _ in range(resources_per_shard):
                resource = resource_payload(shard_id)
                writer_index = await create_indexing_message(
                    resource, "kb", shard_id, node_id, source=IndexMessageSource.WRITER
                )
                processor_index = await create_indexing_message(
                    resource, "kb", shard_id, node_id, source=IndexMessageSource.WRITER
                )
                seqid_writer = await send_indexing_message(
                    faulty_worker, writer_index, node_id
                )
                seqid_processor = await send_indexing_message(
                    faulty_worker, processor_index, node_id
                )
                seqids_by_shard.setdefault(shard_id, []).extend(
                    [seqid_writer, seqid_processor]
                )

        start = datetime.now()
        while True:
            await asyncio.sleep(0.1)

            shards_processed = []

            for shard_id in bunch_of_shards:
                sipb = ShardId()
                sipb.id = shard_id

                shard_pb = await reader.get_shard(sipb)
                is_processed = (
                    shard_pb is not None
                    and shard_pb.fields == shard_pb.sentences == resources_per_shard * 2
                )
                shards_processed.append(is_processed)

            if all(shards_processed):
                break

            elapsed = datetime.now() - start
            assert elapsed.seconds < 60, "too much time waiting"

        await asyncio.sleep(0.5)

        # check all messages have been indexed
        expected_messages = len(bunch_of_shards) * resources_per_shard * 2
        assert len(index_log) == expected_messages

        # check correct order
        seqids_log: dict[str, list[int]] = {}
        for log in index_log:
            seqids_log.setdefault(log.shard_id, []).append(log.seqid)

        assert seqids_by_shard == seqids_log


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
    await asyncio.sleep(0.01)

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
    pb.resource.uuid = str(uuid.uuid4())
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
    resource: Resource,
    kb: str,
    shard: str,
    node: str,
    source: IndexMessageSource.ValueType = IndexMessageSource.PROCESSOR,
) -> IndexMessage:
    storage = await get_storage(service_name=SERVICE_NAME)
    storage_key = await storage.indexing(
        resource, txid=1, partition=TEST_PARTITION, kb=kb, logical_shard=shard
    )
    index = IndexMessage()
    index.txid = 1
    index.typemessage = TypeMessage.CREATION
    index.storage_key = storage_key
    index.kbid = kb
    index.partition = TEST_PARTITION
    index.node = node
    index.shard = shard
    index.source = source
    return index


async def send_indexing_message(worker: Worker, index: IndexMessage, node: str) -> int:
    # Push on stream
    info = await worker.nats_connection_manager.js.publish(
        const.Streams.INDEX.subject.format(node=node),
        index.SerializeToString(),
    )
    return info.seq


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
