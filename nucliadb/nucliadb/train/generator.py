from typing import Any, List

from fastapi import HTTPException
from nucliadb.train.generators.field_classifier import (
    generate_field_classification_payloads,
)
from nucliadb_protos.knowledgebox_pb2 import LabelSet
from nucliadb_protos.train_pb2 import Token, TokenClassificationBatch, TrainSet, Type

from nucliadb.train.generators.paragraph_classifier import (
    generate_paragraph_classification_payloads,
)
from nucliadb.train.utils import get_nodes_manager
from nucliadb_protos.writer_pb2 import GetLabelSetResponse


def token_classification_batch(batch: List[Any]):
    tcb = TokenClassificationBatch()
    for record in batch:
        token = Token()
        token.paragraph = record.text
        token.token.extend(record.token)
        tcb.data.append(token)
    return tcb


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

    # elif trainset.type == Type.TOKEN_CLASSIFICATION:
    #     for batch in generate_token_classification_payload(
    #         trainset, node, shard_replica_id
    #     ):
    #         tcb = token_classification_batch(batch)
    #         payload = tcb.SerializeToString()
    #         yield len(payload).to_bytes(4, byteorder="big", signed=False)
    #         yield payload


# async def generate_token_classification_payload(train: TrainSet):
#     # Query how many tokens has each entity group

#     # Batch size split on number of tokens

#     # Get paragraphs for each token
#     pass


# async def generate_sentence_classification_payloads(
#     trainset: TrainSet, node: Node, shard_replica_id: str
# ) -> AsyncIterator[Union[TrainResponse, SentenceClassificationBatch]]:
#     # Query how many paragraphs has each label
#     request = ParagraphSearchRequest()
#     request.id = shard_replica_id
#     request.only_faceted = True
#     request.filter.tags.extend(trainset.filter)
#     request.faceted.tags.extend()
#     resp = await node.reader.ParagraphSearch(request)

#     # for each label get the

#     # Batch size split on number of labels
#     paragraphs = []  # List of paragraphs
#     paragraphs = random.Random(trainset.seed).shuffle(paragraphs)
#     index = round(len(paragraphs) * trainset.split)
#     train = paragraphs[:index]
#     test = paragraphs[index:]

#     tr = TrainResponse()
#     tr.train = len(train)
#     tr.test = len(test)
#     tr.type = Type.PARAGRAPH_CLASSIFICATION
#     yield tr

#     # Get paragraphs for each classification
#     for batch in get_paragraphs_from_classification(train):
#         tcb = paragraph_classification_batch(batch)
#         yield tcb
