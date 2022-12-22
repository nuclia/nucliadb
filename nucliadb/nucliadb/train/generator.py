from fastapi import HTTPException
from nucliadb.train.generators.field_classifier import (
    generate_field_classification_payloads,
)
from nucliadb.train.generators.token_classifier import (
    generate_token_classification_payloads,
)
from nucliadb_protos.knowledgebox_pb2 import LabelSet
from nucliadb_protos.train_pb2 import TrainSet, Type

from nucliadb.train.generators.paragraph_classifier import (
    generate_paragraph_classification_payloads,
)
from nucliadb.train.utils import get_nodes_manager
from nucliadb_protos.writer_pb2 import (
    GetLabelSetResponse,
)


async def generate_train_data(kbid: str, shard: str, trainset: TrainSet):
    # Get the data structure to generate data
    node_manager = get_nodes_manager()
    node, shard_replica_id = await node_manager.get_reader(kbid, shard)

    if trainset.type == Type.PARAGRAPH_CLASSIFICATION:
        if len(trainset.filter.labels) != 1:
            raise HTTPException(
                status_code=422,
                detail="Paragraph Classification should be of 1 labelset",
            )

        async for data in generate_paragraph_classification_payloads(
            kbid, trainset, node, shard_replica_id
        ):
            payload = data.SerializeToString()
            yield len(payload).to_bytes(4, byteorder="big", signed=False)
            yield payload

    if trainset.type == Type.FIELD_CLASSIFICATION:

        if len(trainset.filter.labels) != 1:
            raise HTTPException(
                status_code=422,
                detail="Paragraph Classification should be of 1 labelset",
            )

        async for data in generate_field_classification_payloads(
            kbid, trainset, node, shard_replica_id
        ):
            payload = data.SerializeToString()
            yield len(payload).to_bytes(4, byteorder="big", signed=False)
            yield payload

    if trainset.type == Type.TOKEN_CLASSIFICATION:

        async for data in generate_token_classification_payloads(
            kbid, trainset, node, shard_replica_id
        ):
            payload = data.SerializeToString()
            yield len(payload).to_bytes(4, byteorder="big", signed=False)
            yield payload
