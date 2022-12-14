import base64
from nucliadb.ingest.orm.processor import Processor
from nucliadb.train.generator import generate_batch
from nucliadb.train.utils import get_processor
from nucliadb_protos.train_pb2 import TrainSet
from uuid import uuid4
from email.utils import formatdate


async def get_kb_partitions(kbid: str, prefix: str):
    proc = get_processor()
    shards = await proc.get_kb_shards(kbid=kbid)
    valid_shards = []
    for shard in shards.shards:
        if shard.shard.startswith(prefix):
            valid_shards.append(shard.shard)
    return valid_shards


def get_train(trainset: str) -> TrainSet:
    train = TrainSet()
    train.ParseFromString(trainset)
    return train


async def create_response_dict(trainset: TrainSet, length: bool = True):
    res = {
        "ETag": uuid4().hex,
        "last-modified": formatdate(timeval=None, localtime=False, usegmt=True),
    }
    if length:
        res["content-length"] = str(1000000)

    return res


async def stream_data_from_trainset(trainset: TrainSet):
    async for batch in generate_batch(trainset):
        yield batch
