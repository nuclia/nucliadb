from nucliadb_protos.nodewriter_pb2 import EmptyQuery, ShardCreated
import nucliadb_node_binding
import asyncio


async def main():
    writer = nucliadb_node_binding.NodeWriter.new()
    shard = await writer.new_shard()
    pb = ShardCreated()
    pb.ParseFromString(bytearray(shard))
    print(pb)


asyncio.run(main())
