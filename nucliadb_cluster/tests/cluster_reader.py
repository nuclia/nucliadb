#just for local tests
from nucliadb_cluster_rust import Member
import asyncio, socket
import json
from codecs import StreamReader, StreamWriter
from typing import Optional, List

class UnSockReader:
    socket_reader: Optional[asyncio.Task] = None
    
    def __init__(self, path):
        self.sock_path = path
        self.socket_reader = None
    
    async def handle_connection(self, reader: StreamReader, _):
        print("Handle new connection via unix socket")
        while True:
            try:
                update_readed = await reader.read(512)
                if len(update_readed) == 0:
                    print("connection closed by cluster manager")
                    break
                json_data: List[Member] = json.loads(update_readed.decode("utf8").replace("'", '"'))
                print(json.dumps(json_data, indent=2, sort_keys=True))
            except IOError as e:
                print(f"error while reading from unix socket: {e}")

    def run_server(self):
        self.socket_reader = asyncio.start_unix_server(self.handle_connection, self.sock_path)
        asyncio.create_task(self.socket_reader, name="socket_reader")
        loop = asyncio.get_event_loop()
        loop.run_forever()

if __name__ == "__main__":
    reader = UnSockReader("/tmp/rust_python.sock")
    reader.run_server()