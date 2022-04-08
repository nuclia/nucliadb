from nucliadb_protos.nodewriter_pb2 import EmptyQuery, ShardCreated
import nucliadb_node_binding
import asyncio


async def main():
    req = EmptyQuery()
    req_str = req.SerializeToString()

    writer = nucliadb_node_binding.NodeWriter.new()
    shard = await writer.new_shard()
    pb = ShardCreated()
    pb.ParseFromString(bytearray(shard))


asyncio.run(main())
