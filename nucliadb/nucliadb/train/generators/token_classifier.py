# Copyright (C) 2021 Bosutech XXI S.L.
#
# nucliadb is offered under the AGPL v3.0 and as commercial software.
# For commercial licensing, contact us at info@nuclia.com.
#
# AGPL:
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

from collections import OrderedDict
from typing import AsyncIterator, Dict, List, Tuple, cast

from nucliadb_protos.dataset_pb2 import (
    TokenClassificationBatch,
    TokensClassification,
    TrainSet,
)
from nucliadb_protos.nodereader_pb2 import StreamFilter, StreamRequest

from nucliadb.common.cluster.abc import AbstractIndexNode
from nucliadb.ingest.orm.resource import KB_REVERSE
from nucliadb.train import logger
from nucliadb.train.generators.utils import get_resource_from_cache_or_db

NERS_DICT = Dict[str, Dict[str, List[Tuple[int, int]]]]
POSITION_DICT = OrderedDict[Tuple[int, int], Tuple[str, str]]
MAIN = "__main__"


async def get_field_text(
    kbid: str, rid: str, field: str, field_type: str, valid_entity_groups: List[str]
) -> Tuple[Dict[str, str], Dict[str, POSITION_DICT], Dict[str, List[Tuple[int, int]]]]:
    orm_resource = await get_resource_from_cache_or_db(kbid, rid)

    if orm_resource is None:
        logger.error(f"{rid} does not exist on DB")
        return {}, {}, {}

    field_type_int = KB_REVERSE[field_type]
    field_obj = await orm_resource.get_field(field, field_type_int, load=False)
    extracted_text = await field_obj.get_extracted_text()
    if extracted_text is None:
        logger.warning(
            f"{rid} {field} {field_type_int} extracted_text does not exist on DB"
        )
        return {}, {}, {}

    split_text: Dict[str, str] = extracted_text.split_text
    split_text[MAIN] = extracted_text.text

    split_ners: Dict[
        str, NERS_DICT
    ] = {}  # Dict of entity group , with entity and list of positions in field
    split_ners[MAIN] = {}

    basic_data = await orm_resource.get_basic()
    invalid_tokens_split: Dict[str, List[Tuple[str, str, int, int]]] = {}
    # Check user definition of entities
    if basic_data is not None:
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
                            invalid_tokens_split[split].append(
                                (token.klass, token.token, token.start, token.end)
                            )
                        else:
                            if token.split in (None, ""):
                                split = MAIN
                            else:
                                split = token.split
                            split_ners[split].setdefault(token.klass, {}).setdefault(
                                token.token, []
                            )
                            split_ners[split][token.klass][token.token].append(
                                (token.start, token.end)
                            )

    field_metadata = await field_obj.get_field_metadata()
    # Check computed definition of entities
    if field_metadata is not None:
        for entity_key, positions in field_metadata.metadata.positions.items():
            entities = entity_key.split("/")
            entity_group = entities[0]
            entity = "/".join(entities[1:])

            if entity_group in valid_entity_groups:
                split_ners[MAIN].setdefault(entity_group, {}).setdefault(entity, [])
                for position in positions.position:
                    split_ners[MAIN][entity_group][entity].append(
                        (position.start, position.end)
                    )

        for split, split_metadata in field_metadata.split_metadata.items():
            for entity_key, positions in split_metadata.positions.items():
                entities = entity_key.split("/")
                entity_group = entities[0]
                entity = "/".join(entities[1:])
                if entity_group in valid_entity_groups:
                    split_ners.setdefault(split, {}).setdefault(
                        entity_group, {}
                    ).setdefault(entity, [])
                    for position in positions.position:
                        split_ners[split][entity_group][entity].append(
                            (position.start, position.end)
                        )

    for split, invalid_tokens in invalid_tokens_split.items():
        for token.klass, token.token, token.start, token.end in invalid_tokens:
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
    if field_metadata is not None:
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
        ordered_positions,
        split_paragraphs,
    )


def compute_segments(field_text: str, ners: POSITION_DICT, start: int, end: int):
    segments = []
    for ner_position, ner in ners.items():
        if ner_position[0] < start or ner_position[1] > end:
            continue

        relative_ner_start = ner_position[0] - start
        relative_ner_end = ner_position[1] - start
        first_part = field_text[:relative_ner_start]
        ner_part = field_text[relative_ner_start:relative_ner_end]
        second_part = field_text[relative_ner_end:]
        for part in first_part.split():
            segments.append((part, "O"))
        ner_parts = ner_part.split()
        first = True
        for part in ner_parts:
            if first:
                segments.append((part, f"B-{ner[0]}"))
                first = False
            else:
                segments.append((part, f"I-{ner[0]}"))
        start += relative_ner_end
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


async def generate_token_classification_payloads(
    kbid: str,
    trainset: TrainSet,
    node: AbstractIndexNode,
    shard_replica_id: str,
) -> AsyncIterator[TokenClassificationBatch]:
    request = StreamRequest()
    request.shard_id.id = shard_replica_id
    for entitygroup in trainset.filter.labels:
        request.filter.tags.append(f"/e/{entitygroup}")
        request.filter.conjunction = StreamFilter.Conjunction.OR
    batch = TokenClassificationBatch()
    async for field_item in node.stream_get_fields(request):
        _, field_type, field = field_item.field.split("/")
        (
            split_text,
            ordered_positions,
            split_paragaphs,
        ) = await get_field_text(
            kbid,
            field_item.uuid,
            field,
            field_type,
            cast(List[str], trainset.filter.labels),
        )
        for split, text in split_text.items():
            ners: POSITION_DICT = ordered_positions.get(split, OrderedDict())
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
