import molotov
from molotov.session import get_context

import os
import uuid
from datetime import datetime

from nucliadb_protos import (
    nodewriter_pb2,
    nodewriter_pb2_grpc,
    noderesources_pb2,
    utils_pb2,
)


GRPC_URL = os.environ.get(
    "NODE_WRITER_GRPC_URL", "127.0.0.1:40101"
)  # "ipv6:[::1]:40101")
TEST_KB = []


async def get_kb_id(session):
    """Creates a shard for the test"""
    if len(TEST_KB) > 0:
        return TEST_KB[0]
    # creating a shard and a kb uuid
    stub = nodewriter_pb2_grpc.NodeWriterStub(session)
    request = nodewriter_pb2.NewShardRequest()
    request.similarity = utils_pb2.VectorSimilarity.COSINE
    kb_id = str(uuid.uuid4())
    request.kbid = kb_id
    request.release_channel = utils_pb2.ReleaseChannel.STABLE
    response = await stub.NewShard(request)
    shard_id = response.id
    response = await stub.ListShards(nodewriter_pb2.EmptyQuery())
    assert shard_id in [r.id for r in response.ids], response.ids

    TEST_KB.append((shard_id, kb_id))
    return shard_id, kb_id


def create_resource(shard_id):
    """Create a new resource"""
    # TODO: add more realistic content in the resource
    rid = noderesources_pb2.ResourceID(uuid=str(uuid.uuid4()))
    rid.shard_id = shard_id
    metadata = noderesources_pb2.IndexMetadata()
    metadata.created.FromDatetime(datetime.now())
    metadata.modified.FromDatetime(datetime.now())
    resource = noderesources_pb2.Resource(resource=rid, metadata=metadata)
    resource.shard_id = shard_id
    return resource


@molotov.scenario(weight=90)
async def writer(session, session_factory="grpc", grpc_url=GRPC_URL):
    """Creating a resource"""

    shard_id, kb_id = await get_kb_id(session)
    stub = nodewriter_pb2_grpc.NodeWriterStub(session)

    worker_id = get_context(session).worker_id
    # send 10 resources within the same channel+worker
    for i in range(10):
        session.print(f"[W:{worker_id}] Creating resource in shard {shard_id}")
        resource = create_resource(shard_id)
        response = await stub.SetResource(resource)
        assert response.status == 0, response


@molotov.scenario(weight=5)
async def gc(session, session_factory="grpc", grpc_url=GRPC_URL):
    """Triggering a GC"""
    shard_id, kb_id = await get_kb_id(session)

    stub = nodewriter_pb2_grpc.NodeWriterStub(session)
    await stub.GC(noderesources_pb2.ShardId(id=shard_id))
    # why do we have EmptyResponse ? how do I assert that GC worked


@molotov.scenario(weight=5)
async def merge(session, session_factory="grpc", grpc_url=GRPC_URL):
    """Triggering a GC"""
    shard_id, kb_id = await get_kb_id(session)

    stub = nodewriter_pb2_grpc.NodeWriterStub(session)

    # this locks in the merge worker do_work()
    await stub.Merge(noderesources_pb2.ShardId(id=shard_id))
