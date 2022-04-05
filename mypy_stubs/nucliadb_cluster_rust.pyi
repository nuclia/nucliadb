from typing import Any, List

class Cluster:
    def __init__(
        self,
        node_id: str,
        remote_host: str,
        node_type: str,
        timeout: int,
        interval: int,
    ): ...
    def add_peer_node(self, url: str) -> None: ...
    async def start(self) -> None: ...
    def get_members(self) -> List[Any]: ...
