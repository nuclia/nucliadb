from collections import Counter
from typing import AsyncIterator, List, Union

import numpy as np
from fastapi import HTTPException
from nucliadb_protos.knowledgebox_pb2 import LabelSet, Labels
from nucliadb_protos.nodereader_pb2 import (
    DocumentSearchRequest,
    DocumentSearchResponse,
    ParagraphSearchRequest,
    ParagraphSearchResponse,
    StreamRequest,
)
from nucliadb_protos.train_pb2 import (
    Label,
    FieldClassificationBatch,
    TextLabel,
    TrainResponse,
    TrainSet,
    Type,
)
from scipy.sparse import csr_matrix, vstack
from sklearn.preprocessing import MultiLabelBinarizer

from nucliadb.ingest.orm.node import Node
from nucliadb.ingest.orm.resource import KB_REVERSE
from nucliadb.train import logger
from nucliadb.train.generators.stratification import IterativeStratification
from nucliadb.train.generators.utils import get_resource_from_cache


async def get_field_text(kbid: str, rid: str, field: str, field_type: str) -> str:
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

    text = ""
    for _, split in extracted_text.split_text.items():
        text += split
        text += " "
    text += extracted_text.text

    return text


async def hydrate_resource_classification_train_test(
    kbid: str,
    X: List[str],
    Y: csr_matrix,
    train_indexes: np.ndarray,
    test_indexes: np.ndarray,
    trainset: TrainSet,
    mlb: MultiLabelBinarizer,
):
    batch = FieldClassificationBatch()
    for index in train_indexes:
        tl = TextLabel()
        field_id = X[index]
        rid, field_type, field = field_id.split("/")
        paragraph_text = await get_field_text(kbid, rid, field, field_type)
        tl.text = paragraph_text

        text_labels = mlb.inverse_transform(Y[index])

        for label in text_labels[0]:
            _, _, labelset, label_title = label.split("/")
            tl.labels.append(Label(labelset=labelset, label=label_title))
        batch.data.append(tl)

        if len(batch.data) == trainset.batch_size:
            yield batch
            batch = FieldClassificationBatch()

    if len(batch.data):
        yield batch

    batch = FieldClassificationBatch()
    for index in test_indexes:
        tl = TextLabel()
        field_id = X[index]
        rid, field_type, field = field_id.split("/")
        paragraph_text = await get_field_text(kbid, rid, field, field_type)
        tl.text = paragraph_text

        text_labels = mlb.inverse_transform(Y[index])

        for label in text_labels[0]:
            _, _, labelset, label_title = label.split("/")
            tl.labels.append(Label(labelset=labelset, label=label_title))
        batch.data.append(tl)

        if len(batch.data) == trainset.batch_size:
            yield batch
            batch = FieldClassificationBatch()

    if len(batch.data):
        yield batch


async def generate_field_classification_payloads(
    kbid: str,
    trainset: TrainSet,
    node: Node,
    shard_replica_id: str,
    labelset_object: LabelSet,
) -> AsyncIterator[Union[TrainResponse, FieldClassificationBatch]]:

    labelset = trainset.filter.labels[0]
    labels = [f"{labelset}/{label.title}" for label in labelset_object.labels]
    mlb = MultiLabelBinarizer(classes=labels, sparse_output=True)

    # Query how many resources has each label
    request = StreamRequest()
    request.shard_id.id = shard_replica_id
    request.filter.tags.append(labelset)
    request.reload = True
    X = []
    Y = []
    async for document_item in node.stream_get_fields(request):
        local_labels = []
        for label in document_item.labels:
            if label.startswith(labelset):
                local_labels.append(label)

        labels_binary = mlb.fit_transform([local_labels])

        X.append(f"{document_item.uuid}{document_item.field}")
        Y.append(labels_binary)

    # Check if min
    total = len(X)
    if len(X) < trainset.minresources:
        raise HTTPException(
            status_code=400, detail=f"There is no enough with this labelset {total}"
        )

    Y = vstack(Y)

    stratifier = IterativeStratification(
        n_splits=2,
        order=2,
        sample_distribution_per_fold=[trainset.split, 1.0 - trainset.split],
        random_state=trainset.seed,
    )
    train_indexes, test_indexes = next(stratifier.split(X, Y))

    tr = TrainResponse()
    tr.train = len(train_indexes)
    tr.test = len(test_indexes)
    tr.type = Type.FIELD_CLASSIFICATION
    yield tr

    # Get paragraphs for each classification
    async for batch in hydrate_resource_classification_train_test(
        kbid, X, Y, train_indexes, test_indexes, trainset, mlb
    ):
        yield batch
