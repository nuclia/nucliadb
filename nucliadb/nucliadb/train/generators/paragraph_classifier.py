from collections import Counter
from typing import Any, AsyncIterator, List, Union
from fastapi import HTTPException
from nucliadb.ingest.orm.resource import KB_REVERSE
from nucliadb.train.generators.utils import get_resource_from_cache
from nucliadb.train.utils import get_nodes_manager
from nucliadb_protos.nodereader_pb2 import (
    GetShardRequest,
    ParagraphResult,
    ParagraphSearchRequest,
    ParagraphSearchResponse,
    SearchRequest,
)
from nucliadb_protos.train_pb2 import (
    LabelFrom,
    SentenceClassificationBatch,
    TextLabel,
    TokenClassificationBatch,
    TrainResponse,
    TrainSet,
    Token,
    ParagraphClassificationBatch,
    Classification,
    Type,
)
from nucliadb.ingest.orm.node import Node
from io import BytesIO
import asyncio
import time
import random
import numpy as np

from sklearn.model_selection import train_test_split
from nucliadb.train import logger
from nucliadb_protos.train_pb2 import Label


async def get_paragraph(kbid: str, result: str) -> str:

    if result.count("/") == 5:
        rid, field_type, field, split_str, start_end = result.split("/")
        split = int(split_str)
        start_str, end_str = start_end.split("-")
    else:
        rid, field_type, field, start_end = result.split("/")
        split = None
        start_str, end_str = start_end.split("-")
    start = int(start_str)
    end = int(end_str)

    orm_resource = await get_resource_from_cache(kbid, rid)

    if orm_resource is None:
        logger.error(f"{rid} does not exist on DB")
        return ""

    field_type_int = KB_REVERSE[field_type]
    field_obj = await orm_resource.get_field(field, field_type_int, load=False)
    extracted_text = await field_obj.get_extracted_text()
    if extracted_text is None:
        logger.warn(
            f"{rid} {field} {field_type_int} extracted_text does not exist on DB"
        )
        return ""

    if split is not None:
        text = extracted_text.split_text[split]
        splitted_text = text[start:end]
    else:
        splitted_text = extracted_text.text[start:end]

    return splitted_text


async def hydrate_paragraph_classification_train_test(
    kbid: str,
    x_train: List[str],
    x_test: List[str],
    y_train: List[List[str]],
    y_test: List[List[str]],
    trainset: TrainSet,
    labels: List[str],
):
    batch = ParagraphClassificationBatch()
    for x, y in zip(x_train, y_train):
        tl = TextLabel()
        label = labels[y]
        paragraph = await get_paragraph(kbid, x)

        tl.text = paragraph
        tl.labels.extend(labels)
        batch.data.append(tl)

        if len(batch.data) == trainset.batch_size:
            yield batch
            batch = ParagraphClassificationBatch()

    batch = ParagraphClassificationBatch()
    for x, y in zip(x_test, y_test):
        tl = TextLabel()
        paragraph, labels = await get_paragraph(kbid, x)

        tl.text = paragraph
        tl.labels.extend(labels)
        batch.data.append(tl)

        if len(batch.data) == trainset.batch_size:
            yield batch
            batch = ParagraphClassificationBatch()


async def generate_paragraph_classification_payloads(
    kbid: str, trainset: TrainSet, node: Node, shard_replica_id: str
) -> AsyncIterator[Union[TrainResponse, ParagraphClassificationBatch]]:

    labelset = trainset.filter.labels[0]
    # Query how many paragraphs has each label
    request = ParagraphSearchRequest()
    request.id = shard_replica_id
    request.filter.tags.append(labelset)
    request.faceted.tags.append(labelset)
    request.reload = True
    request.result_per_page = 10_000_000
    resp: ParagraphSearchResponse = await node.reader.ParagraphSearch(request)

    if labelset not in resp.facets:
        raise HTTPException(
            status_code=400, detail="There is no data with this labelset"
        )
    labelset_counts = Counter()
    for labelset_result in resp.facets.get(labelset).facetresults:
        labelset_counts[labelset_result.tag] = labelset_result.total

    labelset_counts = Counter()
    labels = []
    X = []
    Y = []
    for result in resp.results:
        paragraph_labels = []
        for label in result.labels:
            if label.startswith(labelset):
                if label not in labels:
                    labels.append(label)
                index = labels.index(label)
                paragraph_labels.append(index)
                labelset_counts[index] += 1

                X.append(result.paragraph)
                Y.append(index)

    # Check if min
    total = labelset_counts.total()
    if total < trainset.minresources:
        raise HTTPException(
            status_code=400, detail=f"There is no enough with this labelset {total}"
        )

    if min(labelset_counts.values()) < 2:
        raise HTTPException(status_code=400, detail=f"Labelset with less than 2")

    # We need batches of batch size, stratified with the available labels

    # First get the test-train set

    try:
        x_train, x_test, y_train, y_test = train_test_split(
            X,
            Y,
            test_size=trainset.split,
            random_state=trainset.seed,
            shuffle=True,
            stratify=Y,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    tr = TrainResponse()
    tr.train = len(x_train)
    tr.test = len(x_test)
    tr.type = Type.PARAGRAPH_CLASSIFICATION
    yield tr

    # Get paragraphs for each classification
    async for batch in hydrate_paragraph_classification_train_test(
        kbid, x_train, x_test, y_train, y_test, trainset, labels
    ):
        yield batch
