from dataclasses import dataclass


@dataclass
class IndexNodeMetadata:
    node_id: str
    name: str
    address: str
    shard_count: int
