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
from typing import AsyncGenerator, Optional, cast

from nidx_protos.nodereader_pb2 import StreamFilter, StreamRequest

from nucliadb.common.ids import FIELD_TYPE_STR_TO_PB
from nucliadb.common.nidx import get_nidx_searcher_client
from nucliadb.train import logger
from nucliadb.train.generators.utils import batchify, get_resource_from_cache_or_db
from nucliadb_models.filters import FilterExpression
from nucliadb_protos.dataset_pb2 import (
    TokenClassificationBatch,
    TokensClassification,
    TrainSet,
)

NERS_DICT = dict[str, dict[str, list[tuple[int, int]]]]
POSITION_DICT = OrderedDict[tuple[int, int], tuple[str, str]]
MAIN = "__main__"


def token_classification_batch_generator(
    kbid: str,
    trainset: TrainSet,
    shard_replica_id: str,
    filter_expression: Optional[FilterExpression],
) -> AsyncGenerator[TokenClassificationBatch, None]:
    generator = generate_token_classification_payloads(kbid, trainset, shard_replica_id)
    batch_generator = batchify(generator, trainset.batch_size, TokenClassificationBatch)
    return batch_generator


async def generate_token_classification_payloads(
    kbid: str,
    trainset: TrainSet,
    shard_replica_id: str,
) -> AsyncGenerator[TokensClassification, None]:
    request = StreamRequest()
    request.shard_id.id = shard_replica_id
    for entitygroup in trainset.filter.labels:
        request.filter.labels.append(f"/e/{entitygroup}")
        request.filter.conjunction = StreamFilter.Conjunction.OR
    async for field_item in get_nidx_searcher_client().Documents(request):
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
            cast(list[str], trainset.filter.labels),
        )
        for split, text in split_text.items():
            ners: POSITION_DICT = ordered_positions.get(split, OrderedDict())
            paragraphs = split_paragaphs.get(split, [])

            for segments in process_entities(text, ners, paragraphs):
                tc = TokensClassification()
                for segment in segments:
                    tc.token.append(segment[0])
                    tc.label.append(segment[1])

                yield tc


async def get_field_text(
    kbid: str, rid: str, field: str, field_type: str, valid_entity_groups: list[str]
) -> tuple[dict[str, str], dict[str, POSITION_DICT], dict[str, list[tuple[int, int]]]]:
    orm_resource = await get_resource_from_cache_or_db(kbid, rid)

    if orm_resource is None:
        logger.error(f"{rid} does not exist on DB")
        return {}, {}, {}

    field_type_int = FIELD_TYPE_STR_TO_PB[field_type]
    field_obj = await orm_resource.get_field(field, field_type_int, load=False)
    extracted_text = await field_obj.get_extracted_text()
    if extracted_text is None:
        logger.warning(f"{rid} {field} {field_type_int} extracted_text does not exist on DB")
        return {}, {}, {}

    split_text: dict[str, str] = extracted_text.split_text
    split_text[MAIN] = extracted_text.text

    split_ners: dict[
        str, NERS_DICT
    ] = {}  # Dict of entity group , with entity and list of positions in field
    split_ners[MAIN] = {}

    field_metadata = await field_obj.get_field_metadata()
    # Check computed definition of entities
    if field_metadata is not None:
        # Data Augmentation + Processor entities
        for data_augmentation_task_id, entities in field_metadata.metadata.entities.items():
            for entity in entities.entities:
                entity_text = entity.text
                entity_label = entity.label
                entity_positions = entity.positions
                if entity_label in valid_entity_groups:
                    split_ners[MAIN].setdefault(entity_label, {}).setdefault(entity_text, [])
                    for position in entity_positions:
                        split_ners[MAIN][entity_label][entity_text].append(
                            (position.start, position.end)
                        )

        for split, split_metadata in field_metadata.split_metadata.items():
            for data_augmentation_task_id, entities in split_metadata.entities.items():
                for entity in entities.entities:
                    entity_text = entity.text
                    entity_label = entity.label
                    entity_positions = entity.positions
                    if entity_label in valid_entity_groups:
                        split_ners.setdefault(split, {}).setdefault(entity_label, {}).setdefault(
                            entity_text, []
                        )
                        for position in entity_positions:
                            split_ners[split][entity_label][entity_text].append(
                                (position.start, position.end)
                            )

        # Legacy processor entities
        # TODO: Remove once processor doesn't use this anymore and remove the positions and ner fields from the message
        for entity_key, positions in field_metadata.metadata.positions.items():
            entities = entity_key.split("/")
            entity_group = entities[0]
            entity = "/".join(entities[1:])

            if entity_group in valid_entity_groups:
                split_ners[MAIN].setdefault(entity_group, {}).setdefault(entity, [])
                for position in positions.position:
                    split_ners[MAIN][entity_group][entity].append((position.start, position.end))

        for split, split_metadata in field_metadata.split_metadata.items():
            for entity_key, positions in split_metadata.positions.items():
                entities = entity_key.split("/")
                entity_group = entities[0]
                entity = "/".join(entities[1:])
                if entity_group in valid_entity_groups:
                    split_ners.setdefault(split, {}).setdefault(entity_group, {}).setdefault(entity, [])
                    for position in positions.position:
                        split_ners[split][entity_group][entity].append((position.start, position.end))

    ordered_positions: dict[str, POSITION_DICT] = {}
    for split, ners in split_ners.items():
        split_positions: dict[tuple[int, int], tuple[str, str]] = {}
        for entity_group, entities in ners.items():
            for entity, positions in entities.items():
                for position in positions:
                    split_positions[position] = (entity_group, entity)

        ordered_positions[split] = OrderedDict(sorted(split_positions.items(), key=lambda x: x[0]))

    split_paragraphs: dict[str, list[tuple[int, int]]] = {}
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


def process_entities(text: str, ners: POSITION_DICT, paragraphs: list[tuple[int, int]]):
    if len(paragraphs) > 0:
        for paragraph in paragraphs:
            segments = compute_segments(
                text[paragraph[0] : paragraph[1]], ners, paragraph[0], paragraph[1]
            )
            yield segments
    else:
        segments = compute_segments(text, ners, 0, len(text))
        yield segments
