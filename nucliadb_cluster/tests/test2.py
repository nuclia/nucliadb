import asyncio
import json
import binascii
from sys import byteorder
from typing import List, Optional

# from nucliadb_ingest import logger
# from nucliadb_ingest.orm.node import (
#    ClusterMember,
#    DefinedNodesNucliaDBSearch,
#    chitchat_update_node,
# )
# from nucliadb_ingest.sentry import SENTRY
# from nucliadb_search.settings import settings

# if SENTRY:
#    from sentry_sdk import capture_exception


def start_chitchat():
    chitchat = ChitchatNucliaDBSearch("0.0.0.0", 31337)
    asyncio.run(chitchat.start())

    return chitchat


class ChitchatNucliaDBSearch:
    chitchat_update_srv: Optional[asyncio.Task] = None

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.chitchat_update_srv = None

    async def start(self):
        print("enter chitchat.start()")
        self.chitchat_update_srv = await asyncio.start_server(
            self.socket_reader, host=self.host, port=self.port
        )
        print("tcp server created ")
        async with self.chitchat_update_srv:
            print("awaiting connections from rust part of cluster")
            await asyncio.create_task(self.chitchat_update_srv.serve_forever())

    async def socket_reader(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        while True:
            try:
                print("wait data in socket")
                mgr_message = await reader.read(
                    4096
                )  # TODO: add message types enum with proper deserialization
                if len(mgr_message) == 0:
                    print("empty message received")
                    continue
                if len(mgr_message) == 4:
                    print("check message received: {}".format(mgr_message.hex()))
                    hash = binascii.crc32(mgr_message)
                    print(f"calculated hash: {hash}")
                    response = hash.to_bytes(4, byteorder="big")
                    print(f"Hash response: {response}")
                    writer.write(response)
                    await writer.drain()
                    continue
                else:
                    print(f"update message received: {mgr_message}")
                    members: List[ClusterMember] = json.loads(
                        mgr_message.decode("utf8").replace("'", '"')
                    )
                    print(f"updated members: {members}")
                    if len(members) != 0:
                        await chitchat_update_node(members)
                        writer.write(len(members))
                        await writer.drain()
                    else:
                        print("connection closed by writer")
                        break
            except IOError as e:
                print(f"exception: {e}")
                # if SENTRY:
                #    capture_exception(e)
                # logger.exception(f"error while reading update from unix socket: {e}")

    async def close(self):
        self.chitchat_update_srv.cancel()


if __name__ == "__main__":
    chitchat = start_chitchat()
    loop = asyncio.get_event_loop()
    loop.run_forever()
