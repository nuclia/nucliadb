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
import asyncio

from nucliadb.common.ids import FIELD_TYPE_STR_TO_PB, FieldId
from nucliadb.common.models_utils import from_proto
from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.orm.resource import Resource
from nucliadb.models.internal.augment import (
    ConversationAnswer,
    ConversationAttachments,
    FieldClassificationLabels,
    FieldEntities,
    FieldProp,
    FieldText,
    FieldValue,
)
from nucliadb.search import logger
from nucliadb.search.augmentor.models import (
    AugmentedConversationField,
    AugmentedField,
    AugmentedFileField,
    AugmentedGenericField,
    AugmentedLinkField,
    AugmentedTextField,
)
from nucliadb.search.augmentor.resources import get_basic
from nucliadb.search.augmentor.utils import limited_concurrency
from nucliadb.search.search import cache


async def augment_fields(
    kbid: str,
    given: list[FieldId],
    select: list[FieldProp],
    *,
    concurrency_control: asyncio.Semaphore | None = None,
) -> dict[FieldId, AugmentedField | None]:
    """Augment a list of fields following an augmentation"""

    ops = []
    for field_id in given:
        task = asyncio.create_task(
            limited_concurrency(
                augment_field(kbid, field_id, select),
                max_ops=concurrency_control,
            )
        )
        ops.append(task)
    results: list[AugmentedField | None] = await asyncio.gather(*ops)

    augmented = {}
    for field_id, augmentation in zip(given, results):
        augmented[field_id] = augmentation

    return augmented


async def augment_field(
    kbid: str,
    field_id: FieldId,
    select: list[FieldProp],
) -> AugmentedField | None:
    # TODO: make sure we don't repeat any select clause

    rid = field_id.rid
    resource = await cache.get_resource(kbid, rid)
    if resource is None:
        # skip resources that aren't in the DB
        return None

    field_type_pb = FIELD_TYPE_STR_TO_PB[field_id.type]
    # we must check if field exists or get_field will return an empty field
    # (behaviour thought for ingestion) that we don't want
    if not (await resource.field_exists(field_type_pb, field_id.key)):
        # skip a fields that aren't in the DB
        return None
    field = await resource.get_field(field_id.key, field_id.pb_type)

    return await db_augment_field(field, field_id, select)


async def db_augment_field(
    field: Field,
    field_id: FieldId,
    select: list[FieldProp],
) -> AugmentedField:
    db_augments_by_type = {
        "t": db_augment_text_field,
        "f": db_augment_file_field,
        "u": db_augment_link_field,
        "c": db_augment_conversation_field,
        "a": db_augment_generic_field,
    }
    return await db_augments_by_type[field_id.type](field, field_id, select)


async def db_augment_text_field(
    field: Field,
    field_id: FieldId,
    select: list[FieldProp],
) -> AugmentedTextField:
    augmented = AugmentedTextField(id=field.field_id)

    for prop in select:
        if isinstance(prop, FieldText):
            augmented.text = await get_field_extracted_text(field_id, field)

        elif isinstance(prop, FieldClassificationLabels):
            augmented.classification_labels = await classification_labels(field_id, field.resource)

        elif isinstance(prop, FieldEntities):
            augmented.entities = await field_entities(field_id, field)

        # text field props

        elif isinstance(prop, FieldValue):
            db_value = await field.get_value()
            augmented.value = from_proto.field_text(db_value)

    return augmented


async def db_augment_file_field(
    field: Field,
    field_id: FieldId,
    select: list[FieldProp],
) -> AugmentedFileField:
    augmented = AugmentedFileField(id=field.field_id)

    for prop in select:
        if isinstance(prop, FieldText):
            augmented.text = await get_field_extracted_text(field_id, field)

        elif isinstance(prop, FieldClassificationLabels):
            augmented.classification_labels = await classification_labels(field_id, field.resource)

        elif isinstance(prop, FieldEntities):
            augmented.entities = await field_entities(field_id, field)

        # file field props

        elif isinstance(prop, FieldValue):
            db_value = await field.get_value()
            augmented.value = from_proto.field_file(db_value)

    return augmented


async def db_augment_link_field(
    field: Field,
    field_id: FieldId,
    select: list[FieldProp],
) -> AugmentedLinkField:
    augmented = AugmentedLinkField(id=field.field_id)

    for prop in select:
        if isinstance(prop, FieldText):
            augmented.text = await get_field_extracted_text(field_id, field)

        elif isinstance(prop, FieldClassificationLabels):
            augmented.classification_labels = await classification_labels(field_id, field.resource)

        elif isinstance(prop, FieldEntities):
            augmented.entities = await field_entities(field_id, field)

        # link field props

        elif isinstance(prop, FieldValue):
            db_value = await field.get_value()
            augmented.value = from_proto.field_link(db_value)

    return augmented


async def db_augment_conversation_field(
    field: Field,
    field_id: FieldId,
    select: list[FieldProp],
) -> AugmentedConversationField:
    augmented = AugmentedConversationField(id=field.field_id)

    for prop in select:
        if isinstance(prop, FieldText):
            # TODO: which text should we augment here?
            raise NotImplementedError()

        elif isinstance(prop, FieldValue):
            db_value = await field.get_value()
            augmented.value = from_proto.field_conversation(db_value)

        elif isinstance(prop, FieldClassificationLabels):
            augmented.classification_labels = await classification_labels(field_id, field.resource)

        elif isinstance(prop, FieldEntities):
            augmented.entities = await field_entities(field_id, field)

        elif isinstance(prop, ConversationAnswer):
            raise NotImplementedError()

        elif isinstance(prop, ConversationAttachments):
            raise NotImplementedError()

        else:
            logger.warning(f"conversation field property not implemented: {prop}")

    return augmented


async def db_augment_generic_field(
    field: Field,
    field_id: FieldId,
    select: list[FieldProp],
) -> AugmentedGenericField:
    augmented = AugmentedGenericField(id=field.field_id)

    for prop in select:
        if isinstance(prop, FieldText):
            augmented.text = await get_field_extracted_text(field_id, field)

        elif isinstance(prop, FieldClassificationLabels):
            augmented.classification_labels = await classification_labels(field_id, field.resource)

        elif isinstance(prop, FieldEntities):
            augmented.entities = await field_entities(field_id, field)

        # generic field props

        elif isinstance(prop, FieldValue):
            db_value = await field.get_value()
            augmented.value = db_value

    return augmented


async def get_field_extracted_text(id: FieldId, field: Field) -> str | None:
    extracted_text_pb = await cache.get_field_extracted_text(field)
    if extracted_text_pb is None:  # pragma: no cover
        return None

    if id.subfield_id:
        return extracted_text_pb.split_text[id.subfield_id]
    else:
        return extracted_text_pb.text


async def classification_labels(id: FieldId, resource: Resource) -> list[tuple[str, str]] | None:
    basic = await get_basic(resource)
    if basic is None:
        return None

    labels = set()
    for fc in basic.computedmetadata.field_classifications:
        if fc.field.field == id.key and fc.field.field_type == id.pb_type:
            for classification in fc.classifications:
                if classification.cancelled_by_user:  # pragma: no cover
                    continue
                labels.add((classification.labelset, classification.label))
    return list(labels)


async def field_entities(id: FieldId, field: Field) -> dict[str, set[str]] | None:
    field_metadata = await field.get_field_metadata()
    if field_metadata is None:
        return None

    ners: dict[str, set[str]] = {}
    # Data Augmentation + Processor entities
    for (
        data_aumgentation_task_id,
        entities_wrapper,
    ) in field_metadata.metadata.entities.items():
        for entity in entities_wrapper.entities:
            ners.setdefault(entity.label, set()).add(entity.text)
    # Legacy processor entities
    # TODO: Remove once processor doesn't use this anymore and remove the positions and ner fields from the message
    for token, family in field_metadata.metadata.ner.items():
        ners.setdefault(family, set()).add(token)

    return ners
