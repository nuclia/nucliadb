from typing import Any, List
from nucliadb_protos.train_pb2 import (
    TokenClassificationBatch,
    TrainSet,
    Token,
    ParagraphClassificationBatch,
    Classification,
)
import pyarrow as pa
from io import BytesIO
import asyncio
import time


def token_classification_batch(batch: List[Any]):
    tcb = TokenClassificationBatch()
    for record in batch:
        token = Token()
        token.paragraph = record.text
        token.token.extend(record.token)
        tcb.data.append(token)
    return tcb


def paragraph_classification_batch(batch: List[Any]):
    pcb = ParagraphClassificationBatch()
    for record in batch:
        token = Classification()
        token.paragraph = record.text
        token.classification.extend(record.classification)
        pcb.data.append(token)
    return pcb


async def generate_train_data(kbid: str, shard: str, trainset: TrainSet):
    # Get the data structure to generate data

    # NER / LABELS
    if trainset.type == TrainSet.Type.PARAGRAPH_CLASSIFICATION:
        # Query how many paragraphs has each label

        # Batch size split on number of labels

        # Get paragraphs for each classification
        for batch in get_paragraphs_from_classificaation(trainset):
            tcb = paragraph_classification_batch(batch)
            payload = tcb.SerializeToString()
            yield len(payload).to_bytes(4, byteorder="big", signed=False)
            yield payload

    elif trainset.type == TrainSet.Type.TOKEN_CLASSIFICATION:
        # Query how many tokens has each entity group

        # Batch size split on number of tokens

        # Get paragraphs for each token
        for batch in get_token_classification(trainset):
            tcb = token_classification_batch(batch)
            payload = tcb.SerializeToString()
            yield len(payload).to_bytes(4, byteorder="big", signed=False)
            yield payload


#
