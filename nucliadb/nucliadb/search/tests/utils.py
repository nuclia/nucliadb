import asyncio

from nucliadb_protos.nodereader_pb2 import GetShardRequest
from nucliadb_protos.noderesources_pb2 import Shard

from nucliadb.ingest.orm import NODES
from nucliadb.ingest.orm.node import Node
from nucliadb.ingest.utils import get_driver


async def wait_for_shard(knowledgebox_ingest: str, count: int) -> str:
    # Make sure is indexed
    driver = await get_driver()
    txn = await driver.begin()
    shard = await Node.get_current_active_shard(txn, knowledgebox_ingest)
    if shard is None:
        raise Exception("Could not find shard")
    await txn.abort()

    checks: dict[str, bool] = {}
    for replica in shard.shard.replicas:
        if replica.shard.id not in checks:
            checks[replica.shard.id] = False

    for i in range(30):
        for replica in shard.shard.replicas:
            node_obj = NODES.get(replica.node)
            if node_obj is not None:
                req = GetShardRequest()
                req.shard_id.id = replica.shard.id
                count_shard: Shard = await node_obj.reader.GetShard(req)  # type: ignore
                if count_shard.resources >= count:
                    checks[replica.shard.id] = True
                else:
                    checks[replica.shard.id] = False

        if all(checks.values()):
            break
        await asyncio.sleep(1)

    assert all(checks.values())
    return knowledgebox_ingest


async def inject_message(
    processor, knowledgebox_ingest, message, count: int = 1
) -> str:
    await processor.process(message=message, seqid=count)
    await wait_for_shard(knowledgebox_ingest, count)
    return knowledgebox_ingest
