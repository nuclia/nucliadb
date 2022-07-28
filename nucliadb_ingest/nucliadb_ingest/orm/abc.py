from typing import Any, Optional

from nucliadb_protos.noderesources_pb2 import Resource as PBBrainResource
from nucliadb_protos.writer_pb2 import ShardObject as PBShard

from nucliadb_ingest.maindb.driver import Transaction


class AbstractShard:
    def __init__(self, sharduuid: str, shard: PBShard, node: Optional[Any] = None):
        pass

    async def delete_resource(self, uuid: str, txid: int):
        pass

    async def add_resource(
        self, resource: PBBrainResource, txid: int, reindex_id: Optional[str] = None
    ) -> int:
        pass


class AbstractNode:
    @classmethod
    def create_shard_klass(cls, shard_id: str, pbshard: PBShard) -> AbstractShard:
        pass

    @classmethod
    async def create_shard_by_kbid(cls, txn: Transaction, kbid: str) -> AbstractShard:
        pass

    @classmethod
    async def actual_shard(cls, txn: Transaction, kbid: str) -> Optional[AbstractShard]:
        pass
