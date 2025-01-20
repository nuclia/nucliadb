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
from __future__ import annotations

from typing import AsyncIterator, MutableMapping, Optional

from nucliadb.common import datamanagers
from nucliadb.ingest.orm.resource import Resource
from nucliadb_protos.resources_pb2 import (
    FieldID,
    FieldMetadata,
    ParagraphAnnotation,
)
from nucliadb_protos.train_pb2 import (
    EnabledMetadata,
    TrainField,
    TrainMetadata,
    TrainParagraph,
    TrainResource,
    TrainSentence,
)
from nucliadb_protos.train_pb2 import Position as TrainPosition


async def iterate_sentences(
    resource: Resource,
    enabled_metadata: EnabledMetadata,
) -> AsyncIterator[TrainSentence]:  # pragma: no cover
    fields = await resource.get_fields(force=True)
    metadata = TrainMetadata()
    userdefinedparagraphclass: dict[str, ParagraphAnnotation] = {}
    if enabled_metadata.labels:
        if resource.basic is None:
            resource.basic = await resource.get_basic()
        if resource.basic is not None:
            metadata.labels.resource.extend(resource.basic.usermetadata.classifications)
            for fieldmetadata in resource.basic.fieldmetadata:
                field_id = resource.generate_field_id(fieldmetadata.field)
                for annotationparagraph in fieldmetadata.paragraphs:
                    userdefinedparagraphclass[annotationparagraph.key] = annotationparagraph

    for (type_id, field_id), field in fields.items():
        fieldid = FieldID(field_type=type_id, field=field_id)
        field_key = resource.generate_field_id(fieldid)
        fm = await field.get_field_metadata()
        extracted_text = None
        vo = None
        text = None

        if enabled_metadata.vector:
            # XXX: Given that nobody requested any particular vectorset, we'll
            # return any
            vectorset_id = None
            async with datamanagers.with_ro_transaction() as txn:
                async for vectorset_id, vs in datamanagers.vectorsets.iter(
                    txn=txn, kbid=resource.kb.kbid
                ):
                    break
            assert vectorset_id is not None, "All KBs must have at least a vectorset"
            vo = await field.get_vectors(vectorset_id, vs.storage_key_kind)

        extracted_text = await field.get_extracted_text()

        if fm is None:
            continue

        field_metadatas: list[tuple[Optional[str], FieldMetadata]] = [(None, fm.metadata)]
        for subfield_metadata, splitted_metadata in fm.split_metadata.items():
            field_metadatas.append((subfield_metadata, splitted_metadata))

        for subfield, field_metadata in field_metadatas:
            if enabled_metadata.labels:
                metadata.labels.ClearField("field")
                metadata.labels.field.extend(field_metadata.classifications)

            entities: dict[str, str] = {}
            if enabled_metadata.entities:
                _update_entities_dict(entities, field_metadata)

            precomputed_vectors = {}
            if vo is not None:
                if subfield is not None:
                    vectors = vo.split_vectors[subfield]
                    base_vector_key = f"{resource.uuid}/{field_key}/{subfield}"
                else:
                    vectors = vo.vectors
                    base_vector_key = f"{resource.uuid}/{field_key}"
                for index, vector in enumerate(vectors.vectors):
                    vector_key = f"{base_vector_key}/{index}/{vector.start}-{vector.end}"
                    precomputed_vectors[vector_key] = vector.vector

            if extracted_text is not None:
                if subfield is not None:
                    text = extracted_text.split_text[subfield]
                else:
                    text = extracted_text.text

            for paragraph in field_metadata.paragraphs:
                if subfield is not None:
                    paragraph_key = (
                        f"{resource.uuid}/{field_key}/{subfield}/{paragraph.start}-{paragraph.end}"
                    )
                else:
                    paragraph_key = f"{resource.uuid}/{field_key}/{paragraph.start}-{paragraph.end}"

                if enabled_metadata.labels:
                    metadata.labels.ClearField("field")
                    metadata.labels.paragraph.extend(paragraph.classifications)
                    if paragraph_key in userdefinedparagraphclass:
                        metadata.labels.paragraph.extend(
                            userdefinedparagraphclass[paragraph_key].classifications
                        )

                for index, sentence in enumerate(paragraph.sentences):
                    if subfield is not None:
                        sentence_key = f"{resource.uuid}/{field_key}/{subfield}/{index}/{sentence.start}-{sentence.end}"
                    else:
                        sentence_key = (
                            f"{resource.uuid}/{field_key}/{index}/{sentence.start}-{sentence.end}"
                        )

                    if vo is not None:
                        metadata.ClearField("vector")
                        vector_tmp = precomputed_vectors.get(sentence_key)
                        if vector_tmp:
                            metadata.vector.extend(vector_tmp)

                    if extracted_text is not None and text is not None:
                        metadata.text = text[sentence.start : sentence.end]

                    metadata.ClearField("entities")
                    metadata.ClearField("entity_positions")
                    if enabled_metadata.entities and text is not None:
                        local_text = text[sentence.start : sentence.end]
                        add_entities_to_metadata(entities, local_text, metadata)

                    pb_sentence = TrainSentence()
                    pb_sentence.uuid = resource.uuid
                    pb_sentence.field.CopyFrom(fieldid)
                    pb_sentence.paragraph = paragraph_key
                    pb_sentence.sentence = sentence_key
                    pb_sentence.metadata.CopyFrom(metadata)
                    yield pb_sentence


async def iterate_paragraphs(
    resource: Resource, enabled_metadata: EnabledMetadata
) -> AsyncIterator[TrainParagraph]:
    fields = await resource.get_fields(force=True)
    metadata = TrainMetadata()
    userdefinedparagraphclass: dict[str, ParagraphAnnotation] = {}
    if enabled_metadata.labels:
        if resource.basic is None:
            resource.basic = await resource.get_basic()
        if resource.basic is not None:
            metadata.labels.resource.extend(resource.basic.usermetadata.classifications)
            for fieldmetadata in resource.basic.fieldmetadata:
                field_id = resource.generate_field_id(fieldmetadata.field)
                for annotationparagraph in fieldmetadata.paragraphs:
                    userdefinedparagraphclass[annotationparagraph.key] = annotationparagraph

    for (type_id, field_id), field in fields.items():
        fieldid = FieldID(field_type=type_id, field=field_id)
        field_key = resource.generate_field_id(fieldid)
        fm = await field.get_field_metadata()
        extracted_text = None
        text = None

        extracted_text = await field.get_extracted_text()

        if fm is None:
            continue

        field_metadatas: list[tuple[Optional[str], FieldMetadata]] = [(None, fm.metadata)]
        for subfield_metadata, splitted_metadata in fm.split_metadata.items():
            field_metadatas.append((subfield_metadata, splitted_metadata))

        for subfield, field_metadata in field_metadatas:
            if enabled_metadata.labels:
                metadata.labels.ClearField("field")
                metadata.labels.field.extend(field_metadata.classifications)

            entities: dict[str, str] = {}
            if enabled_metadata.entities:
                _update_entities_dict(entities, field_metadata)

            if extracted_text is not None:
                if subfield is not None:
                    text = extracted_text.split_text[subfield]
                else:
                    text = extracted_text.text

            for paragraph in field_metadata.paragraphs:
                if subfield is not None:
                    paragraph_key = (
                        f"{resource.uuid}/{field_key}/{subfield}/{paragraph.start}-{paragraph.end}"
                    )
                else:
                    paragraph_key = f"{resource.uuid}/{field_key}/{paragraph.start}-{paragraph.end}"

                if enabled_metadata.labels:
                    metadata.labels.ClearField("paragraph")
                    metadata.labels.paragraph.extend(paragraph.classifications)

                    if extracted_text is not None and text is not None:
                        metadata.text = text[paragraph.start : paragraph.end]

                    metadata.ClearField("entities")
                    metadata.ClearField("entity_positions")
                    if enabled_metadata.entities and text is not None:
                        local_text = text[paragraph.start : paragraph.end]
                        add_entities_to_metadata(entities, local_text, metadata)

                    if paragraph_key in userdefinedparagraphclass:
                        metadata.labels.paragraph.extend(
                            userdefinedparagraphclass[paragraph_key].classifications
                        )

                    pb_paragraph = TrainParagraph()
                    pb_paragraph.uuid = resource.uuid
                    pb_paragraph.field.CopyFrom(fieldid)
                    pb_paragraph.paragraph = paragraph_key
                    pb_paragraph.metadata.CopyFrom(metadata)

                    yield pb_paragraph


async def iterate_fields(
    resource: Resource, enabled_metadata: EnabledMetadata
) -> AsyncIterator[TrainField]:
    fields = await resource.get_fields(force=True)
    metadata = TrainMetadata()
    if enabled_metadata.labels:
        if resource.basic is None:
            resource.basic = await resource.get_basic()
        if resource.basic is not None:
            metadata.labels.resource.extend(resource.basic.usermetadata.classifications)

    for (type_id, field_id), field in fields.items():
        fieldid = FieldID(field_type=type_id, field=field_id)
        fm = await field.get_field_metadata()
        extracted_text = None

        if enabled_metadata.text:
            extracted_text = await field.get_extracted_text()

        if fm is None:
            continue

        field_metadatas: list[tuple[Optional[str], FieldMetadata]] = [(None, fm.metadata)]
        for subfield_metadata, splitted_metadata in fm.split_metadata.items():
            field_metadatas.append((subfield_metadata, splitted_metadata))

        for subfield, splitted_metadata in field_metadatas:
            if enabled_metadata.labels:
                metadata.labels.ClearField("field")
                metadata.labels.field.extend(splitted_metadata.classifications)

            if extracted_text is not None:
                if subfield is not None:
                    metadata.text = extracted_text.split_text[subfield]
                else:
                    metadata.text = extracted_text.text

            if enabled_metadata.entities:
                metadata.ClearField("entities")
                _update_entities_dict(metadata.entities, splitted_metadata)

            pb_field = TrainField()
            pb_field.uuid = resource.uuid
            pb_field.field.CopyFrom(fieldid)
            pb_field.metadata.CopyFrom(metadata)
            yield pb_field


async def generate_train_resource(
    resource: Resource, enabled_metadata: EnabledMetadata
) -> TrainResource:
    fields = await resource.get_fields(force=True)
    metadata = TrainMetadata()
    if enabled_metadata.labels:
        if resource.basic is None:
            resource.basic = await resource.get_basic()
        if resource.basic is not None:
            metadata.labels.resource.extend(resource.basic.usermetadata.classifications)

    metadata.labels.ClearField("field")
    metadata.ClearField("entities")

    for (_, _), field in fields.items():
        extracted_text = None
        fm = await field.get_field_metadata()

        if enabled_metadata.text:
            extracted_text = await field.get_extracted_text()

        if extracted_text is not None:
            metadata.text += extracted_text.text
            for text in extracted_text.split_text.values():
                metadata.text += f" {text}"

        if fm is None:
            continue

        field_metadatas: list[tuple[Optional[str], FieldMetadata]] = [(None, fm.metadata)]
        for subfield_metadata, splitted_metadata in fm.split_metadata.items():
            field_metadatas.append((subfield_metadata, splitted_metadata))

        for _, splitted_metadata in field_metadatas:
            if enabled_metadata.labels:
                metadata.labels.field.extend(splitted_metadata.classifications)

            if enabled_metadata.entities:
                _update_entities_dict(metadata.entities, splitted_metadata)

    pb_resource = TrainResource()
    pb_resource.uuid = resource.uuid
    if resource.basic is not None:
        pb_resource.title = resource.basic.title
        pb_resource.icon = resource.basic.icon
        pb_resource.slug = resource.basic.slug
        pb_resource.modified.CopyFrom(resource.basic.modified)
        pb_resource.created.CopyFrom(resource.basic.created)
    pb_resource.metadata.CopyFrom(metadata)
    return pb_resource


def add_entities_to_metadata(entities: dict[str, str], local_text: str, metadata: TrainMetadata) -> None:
    for entity_key, entity_value in entities.items():
        if entity_key not in local_text:
            # Add the entity only if found in text
            continue
        metadata.entities[entity_key] = entity_value

        # Add positions for the entity relative to the local text
        poskey = f"{entity_value}/{entity_key}"
        metadata.entity_positions[poskey].entity = entity_key
        last_occurrence_end = 0
        for _ in range(local_text.count(entity_key)):
            start = local_text.index(entity_key, last_occurrence_end)
            end = start + len(entity_key)
            metadata.entity_positions[poskey].positions.append(TrainPosition(start=start, end=end))
            last_occurrence_end = end


def _update_entities_dict(target_entites_dict: MutableMapping[str, str], field_metadata: FieldMetadata):
    """
    Update the entities dict with the entities from the field metadata.
    Method created to ease the transition from legacy ner field to new entities field.
    """
    # Data Augmentation + Processor entities
    # This will overwrite entities detected from more than one data augmentation task
    # TODO: Change TrainMetadata proto to accept multiple entities with the same text
    entity_map = {
        entity.text: entity.label
        for data_augmentation_task_id, entities_wrapper in field_metadata.entities.items()
        for entity in entities_wrapper.entities
    }
    target_entites_dict.update(entity_map)

    # Legacy processor entities
    # TODO: Remove once processor doesn't use this anymore and remove the positions and ner fields from the message
    target_entites_dict.update(field_metadata.ner)
