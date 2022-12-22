from collections import Counter, OrderedDict
from typing import AsyncIterator, Dict, Iterator, List, Tuple, Union
from nucliadb_models.common import Paragraph

import numpy as np
from fastapi import HTTPException
from nucliadb_protos.knowledgebox_pb2 import EntitiesGroup, LabelSet, Labels
from nucliadb_protos.nodereader_pb2 import (
    ParagraphSearchRequest,
    ParagraphSearchResponse,
    StreamRequest,
)
from nucliadb_protos.train_pb2 import (
    Label,
    ParagraphClassificationBatch,
    TextLabel,
    TokenClassificationBatch,
    TokensClassification,
    TrainResponse,
    TrainSet,
    Type,
)
from scipy.sparse import csr_matrix, vstack
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MultiLabelBinarizer

from nucliadb.ingest.orm.node import Node
from nucliadb.ingest.orm.resource import KB_REVERSE
from nucliadb.train import logger
from nucliadb.train.generators.stratification import IterativeStratification
from nucliadb.train.generators.utils import get_resource_from_cache

NERS_DICT = Dict[str, Dict[str, List[Tuple[int, int]]]]
POSITION_DICT = OrderedDict[Tuple[int, int], Tuple[str, str]]
MAIN = "__main__"


async def get_field_text(
    kbid: str, rid: str, field: str, field_type: str, valid_entity_groups: List[str]
) -> Tuple[Dict[str, str], Dict[str, POSITION_DICT], Dict[str, List[Tuple[int, int]]]]:
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

    split_text: Dict[str, str] = extracted_text.split_text
    split_text[MAIN] = extracted_text.text

    split_ners: Dict[
        str, NERS_DICT
    ] = {}  # Dict of entity group , with entity and list of positions in field

    basic_data = await orm_resource.get_basic()
    invalid_tokens = []
    invalid_tokens_split: Dict[str, List[str]] = {}
    # Check user definition of entities
    for userfieldmetadata in basic_data.fieldmetadata:
        if (
            userfieldmetadata.field.field == field
            and userfieldmetadata.field.field_type == field_type_int
        ):
            for token in userfieldmetadata.token:
                if token.klass in valid_entity_groups:
                    if token.cancelled_by_user:
                        if token.split in (None, ""):
                            split = MAIN
                        else:
                            split = token.split
                        invalid_tokens_split[token.split].append(
                            (token.klass, token.token, token.start, token.end)
                        )
                    else:
                        if token.split in (None, ""):
                            split = MAIN
                        else:
                            split = token.split
                        split_ners[token.split].setdefault(token.klass, {}).setdefault(
                            token.token, []
                        )
                        split_ners[token.split][token.klass][token.token].append(
                            (token.start, token.end)
                        )

    field_metadata = await field_obj.get_field_metadata()
    # Check computed definition of entities
    for entity_key, positions in field_metadata.metadata.positions.items():
        if entity_group in valid_entity_groups:
            entity_group, entity = entity_key.split("/")
            split_ners[MAIN].setdefault(entity_group, {}).setdefault(entity, [])
            for position in positions.position:
                split_ners[MAIN][entity_group][entity].append(
                    (position.start, position.end)
                )

    for split, split_metadata in field_metadata.split_metadata.items():
        for entity_key, positions in split_metadata.positions.items():
            if entity_group in valid_entity_groups:
                entity_group, entity = entity_key.split("/")
                split_ners.setdefault(split, {}).setdefault(
                    entity_group, {}
                ).setdefault(entity, [])
                for position in positions.position:
                    split_ners[split][entity_group][entity].append(
                        (position.start, position.end)
                    )

    for split, invalid_tokens in invalid_tokens_split.items():
        for (token.klass, token.token, token.start, token.end) in invalid_tokens:
            if token.klass in split_ners.get(split, {}):
                if token.token in split_ners.get(split, {}).get(token.klass, {}):
                    if (token.start, token.end) in split_ners[split][token.klass][
                        token.token
                    ]:
                        split_ners[split][token.klass][token.token].remove(
                            (token.start, token.end)
                        )
                        if len(split_ners[split][token.klass][token.token]) == 0:
                            del split_ners[split][token.klass][token.token]
                        if len(split_ners[split][token.klass]) == 0:
                            del split_ners[split][token.klass]

    ordered_positions: Dict[str, POSITION_DICT] = {}
    for split, ners in split_ners.items():
        split_positions: Dict[Tuple[int, int], Tuple[str, str]] = {}
        for entity_group, entities in ners.items():
            for entity, positions in entities.items():
                for position in positions:
                    split_positions[position] = (entity_group, entity)

        ordered_positions[split] = OrderedDict(
            sorted(split_positions.items(), key=lambda x: x[0])
        )

    split_paragraphs: Dict[str, List[Tuple[int, int]]] = {}
    split_paragraphs[MAIN] = sorted(
        [(p.start, p.end) for p in field_metadata.metadata.paragraphs],
        key=lambda x: x[0],
    )
    for split, metadata in field_metadata.split_metadata.items():
        split_paragraphs[split] = sorted(
            [(p.start, p.end) for p in metadata.paragraphs],
            key=lambda x: x[0],
        )

    return (
        split_text,
        split_ners,
        split_paragraphs,
    )


def compute_segments(field_text: str, ners: POSITION_DICT, start: int, end: int):

    segments = []
    for position, ner in ners.items():
        if position[0] < start or position[1] > end:
            continue
        first_part = field_text[: position[0]]
        ner_part = field_text[position[0] : position[1]]
        second_part = field_text[position[1] :]
        for part in first_part.split():
            segments.append((part, "O"))
        ner_parts = ner_part.split()
        first = True
        for part in ner_parts:
            if first:
                segments.append((f"B-{part}", ner[0]))
                first = False
            else:
                segments.append((f"I-{part}", ner[0]))
        field_text = second_part

    for part in field_text.split():
        segments.append((part, "O"))
    return segments


def process_entities(text: str, ners: POSITION_DICT, paragraphs: List[Tuple[int, int]]):
    if len(paragraphs) > 0:
        for paragraph in paragraphs:
            segments = compute_segments(
                text[paragraph[0] : paragraph[1]], ners, paragraph[0], paragraph[1]
            )
            yield segments
    else:
        segments = compute_segments(text, ners, 0, len(text))
        yield segments


async def hydrate_token_classification_train_test(
    kbid: str,
    X: List[str],
    train_indexes: np.ndarray,
    test_indexes: np.ndarray,
    trainset: TrainSet,
):
    batch = TokenClassificationBatch()
    for index in train_indexes:
        field_id = X[index]
        rid, field_type, field = field_id.split("/")
        (
            split_text,
            split_ners,
            split_paragaphs,
        ) = await get_field_text(kbid, rid, field, field_type, trainset.filter.labels)

        for split, text in split_text.items():
            ners = split_ners.get(split, {})
            paragraphs = split_paragaphs.get(split, [])

            for segments in process_entities(text, ners, paragraphs):
                tc = TokensClassification()
                for segment in segments:
                    tc.token.append(segment[0])
                    tc.label.append(segment[1])
                batch.data.append(tc)
                if len(batch.data) == trainset.batch_size:
                    yield batch
                    batch = TokenClassificationBatch()

    if len(batch.data):
        yield batch
    batch = TokenClassificationBatch()
    for index in test_indexes:
        field_id = X[index]
        rid, field_type, field = field_id.split("/")
        (
            split_text,
            split_ners,
            split_paragaphs,
        ) = await get_field_text(kbid, rid, field, field_type, trainset.filter.labels)

        for split, text in split_text.items():
            ners = split_ners.get(split, {})
            paragraphs = split_paragaphs.get(split, [])

            for segments in process_entities(text, ners, paragraphs):
                tc = TokensClassification()
                for segment in segments:
                    tc.token.append(segment[0])
                    tc.label.append(segment[1])
                batch.data.append(tc)
                if len(batch.data) == trainset.batch_size:
                    yield batch
                    batch = TokenClassificationBatch()

    if len(batch.data):
        yield batch


async def generate_token_classification_payloads(
    kbid: str,
    trainset: TrainSet,
    node: Node,
    shard_replica_id: str,
) -> AsyncIterator[Union[TrainResponse, ParagraphClassificationBatch]]:

    # Query how many paragraphs has each label
    request = StreamRequest()
    request.shard_id.id = shard_replica_id
    for entitygroup in trainset.filter.labels:
        request.filter.tags.append(f"/e/{entitygroup}")
    request.reload = True
    X = []
    async for field_item in node.stream_get_fields(request):
        X.append(f"{field_item.uuid}{field_item.field}")

    # Check if min
    total = len(X)
    if len(X) < trainset.minresources:
        raise HTTPException(
            status_code=400, detail=f"There is no enough with this labelset {total}"
        )

    train_indexes, test_indexes = train_test_split(
        X, test_size=trainset.split, shuffle=True, random_state=trainset.seed
    )

    tr = TrainResponse()
    tr.train = len(train_indexes)
    tr.test = len(test_indexes)
    tr.type = Type.TOKEN_CLASSIFICATION
    yield tr

    # Get paragraphs for each classification
    async for batch in hydrate_token_classification_train_test(
        kbid, X, train_indexes, test_indexes, trainset
    ):
        yield batch
