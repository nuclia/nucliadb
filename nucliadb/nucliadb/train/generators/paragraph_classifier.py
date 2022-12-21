from collections import Counter
from typing import AsyncIterator, List, Union

import numpy as np
from fastapi import HTTPException
from nucliadb_protos.knowledgebox_pb2 import Labels
from nucliadb_protos.nodereader_pb2 import (
    ParagraphSearchRequest,
    ParagraphSearchResponse,
)
from nucliadb_protos.train_pb2 import (
    Label,
    ParagraphClassificationBatch,
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
    X: List[str],
    Y: csr_matrix,
    train_indexes: np.ndarray,
    test_indexes: np.ndarray,
    trainset: TrainSet,
    mlb: MultiLabelBinarizer,
):
    batch = ParagraphClassificationBatch()
    for index in train_indexes:
        tl = TextLabel()
        paragraph_id = X[index]
        text_labels = mlb.inverse_transform(Y[index])
        paragraph_text = await get_paragraph(kbid, paragraph_id)

        tl.text = paragraph_text
        for label in text_labels[0]:
            _, _, labelset, label_title = label.split("/")
            tl.labels.append(Label(labelset=labelset, label=label_title))
        batch.data.append(tl)

        if len(batch.data) == trainset.batch_size:
            yield batch
            batch = ParagraphClassificationBatch()

    if len(batch.data):
        yield batch

    batch = ParagraphClassificationBatch()
    for index in test_indexes:
        tl = TextLabel()
        paragraph_id = X[index]
        text_labels = mlb.inverse_transform(Y[index])
        paragraph_text = await get_paragraph(kbid, paragraph_id)

        tl.text = paragraph_text
        for label in text_labels[0]:
            _, _, labelset, label_title = label.split("/")
            tl.labels.append(Label(labelset=labelset, label=label_title))
        batch.data.append(tl)

        if len(batch.data) == trainset.batch_size:
            yield batch
            batch = ParagraphClassificationBatch()

    if len(batch.data):
        yield batch


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

    labelset_counts = Counter()
    for labelset_result in resp.facets.get(labelset).facetresults:
        labelset_counts[labelset_result.tag] = labelset_result.total

    mlb = MultiLabelBinarizer(classes=list(labelset_counts.keys()), sparse_output=True)

    X = []
    Y = []
    for result in resp.results:
        for label in result.labels:
            local_labels = []
            if label.startswith(labelset):
                local_labels.append(label)

        labels_binary = mlb.fit_transform([local_labels])

        X.append(result.paragraph)
        Y.append(labels_binary)

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
    tr.type = Type.PARAGRAPH_CLASSIFICATION
    yield tr

    # Get paragraphs for each classification
    async for batch in hydrate_paragraph_classification_train_test(
        kbid, X, Y, train_indexes, test_indexes, trainset, mlb
    ):
        yield batch
