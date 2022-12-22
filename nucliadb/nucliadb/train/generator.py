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
    if trainset.split == 0.0:
        trainset.split = 0.25

    if trainset.type == Type.PARAGRAPH_CLASSIFICATION:
        txn = await node_manager.driver.begin()

        kbobj = await node_manager.get_kb_obj(txn, kbid)
        if len(trainset.filter.labels) != 1:
            raise HTTPException(
                status_code=422,
                detail="Paragraph Classification should be of 1 labelset",
            )

        _, _, labelset = trainset.filter.labels[0].split("/")  # Its /l/labelset
        if kbobj is not None:
            labelset_response = GetLabelSetResponse()
            await kbobj.get_labelset(labelset, labelset_response)
            labelset_object = labelset_response.labelset
            if LabelSet.LabelSetKind.PARAGRAPHS not in labelset_object.kind:
                raise HTTPException(
                    status_code=400,
                    detail="Labelset is not Paragraph Type",
                )

        else:
            await txn.abort()
            raise HTTPException(
                status_code=422,
                detail="Invalid KBID",
            )
        await txn.abort()

        async for data in generate_paragraph_classification_payloads(
            kbid, trainset, node, shard_replica_id, labelset_object
        ):
            payload = data.SerializeToString()
            yield len(payload).to_bytes(4, byteorder="big", signed=False)
            yield payload

    if trainset.type == Type.FIELD_CLASSIFICATION:

        txn = await node_manager.driver.begin()

        kbobj = await node_manager.get_kb_obj(txn, kbid)

        if len(trainset.filter.labels) != 1:
            raise HTTPException(
                status_code=422,
                detail="Paragraph Classification should be of 1 labelset",
            )

        _, _, labelset = trainset.filter.labels[0].split("/")  # Its /l/labelset
        if kbobj is not None:
            labelset_response = GetLabelSetResponse()
            await kbobj.get_labelset(labelset, labelset_response)
            labelset_object = labelset_response.labelset
            if LabelSet.LabelSetKind.RESOURCES not in labelset_object.kind:
                raise HTTPException(
                    status_code=400,
                    detail="Labelset is not Resource Type",
                )
        else:
            await txn.abort()
            raise HTTPException(
                status_code=422,
                detail="Invalid KBID",
            )
        await txn.abort()

        if len(trainset.filter.labels) != 1:
            raise HTTPException(
                status_code=422,
                detail="Paragraph Classification should be of 1 labelset",
            )

        async for data in generate_field_classification_payloads(
            kbid, trainset, node, shard_replica_id, labelset_object
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
