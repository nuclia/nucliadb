from typing import Any, List
from nucliadb_protos.train_pb2 import (
    TokenClassificationBatch,
    TrainResponse,
    TrainSet,
    Token,
    ParagraphClassificationBatch,
    Classification,
    Type
)
from io import BytesIO
import asyncio
import time
import random


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
    if trainset.type == Type.PARAGRAPH_CLASSIFICATION:
        # Query how many paragraphs has each label
        label1 has 10 paragraphs
        label2 has 30 paragraphs
        More without paragraphs labels



        # Batch size split on number of labels
        paragraphs = []  # List of paragraphs
        paragraphs = random.Random(trainset.seed).shuffle(paragraphs)
        index = round(len(paragraphs) * trainset.split)
        train = paragraphs[:index]
        test = paragraphs[index:]
        
        tr = TrainResponse()
        tr.train = len(train)
        tr.test = len(test)
        tr.type = Type.PARAGRAPH_CLASSIFICATION
        payload = tr.SerializeToString()
        yield len(payload).to_bytes(4, byteorder="big", signed=False)
        yield payload

        # Get paragraphs for each classification
        for batch in get_paragraphs_from_classification(train):
            tcb = paragraph_classification_batch(batch)
            payload = tcb.SerializeToString()
            yield len(payload).to_bytes(4, byteorder="big", signed=False)
            yield payload

    elif trainset.type == Type.TOKEN_CLASSIFICATION:
        # Query how many tokens has each entity group

        # Batch size split on number of tokens

        # Get paragraphs for each token
        for batch in get_token_classification(trainset):
            tcb = token_classification_batch(batch)
            payload = tcb.SerializeToString()
            yield len(payload).to_bytes(4, byteorder="big", signed=False)
            yield payload


#
