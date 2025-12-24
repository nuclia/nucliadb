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
from collections import deque
from collections.abc import AsyncIterator, Sequence
from typing import Deque, cast

from typing_extensions import assert_never

from nucliadb.common.ids import FIELD_TYPE_STR_TO_PB, FieldId
from nucliadb.common.models_utils import from_proto
from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.fields.conversation import Conversation
from nucliadb.ingest.fields.file import File
from nucliadb.ingest.fields.generic import Generic
from nucliadb.ingest.fields.link import Link
from nucliadb.ingest.fields.text import Text
from nucliadb.ingest.orm.resource import Resource
from nucliadb.models.internal.augment import (
    AnswerSelector,
    AugmentedConversationField,
    AugmentedConversationMessage,
    AugmentedField,
    AugmentedFileField,
    AugmentedGenericField,
    AugmentedLinkField,
    AugmentedTextField,
    ConversationAttachments,
    ConversationProp,
    ConversationSelector,
    ConversationText,
    FieldClassificationLabels,
    FieldEntities,
    FieldProp,
    FieldText,
    FieldValue,
    FileProp,
    FileThumbnail,
    FullSelector,
    MessageSelector,
    NeighboursSelector,
    PageSelector,
    WindowSelector,
)
from nucliadb.search.augmentor.metrics import augmentor_observer
from nucliadb.search.augmentor.resources import get_basic
from nucliadb.search.augmentor.utils import limited_concurrency
from nucliadb.search.search import cache
from nucliadb_models.common import FieldTypeName
from nucliadb_protos import resources_pb2
from nucliadb_utils.storages.storage import STORAGE_FILE_EXTRACTED


async def augment_fields(
    kbid: str,
    given: list[FieldId],
    select: list[FieldProp | ConversationProp],
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


@augmentor_observer.wrap({"type": "field"})
async def augment_field(
    kbid: str,
    field_id: FieldId,
    select: Sequence[FieldProp | ConversationProp],
) -> AugmentedField | None:
    # TODO(decoupled-ask): make sure we don't repeat any select clause

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
    select: Sequence[FieldProp | FileProp | ConversationProp],
) -> AugmentedField:
    field_type = field_id.type

    # Note we cast `select` to the specific Union type required by the
    # db_augment_ function. This is safe even if there are props that are not
    # for a specific field, as they will be ignored

    if field_type == FieldTypeName.TEXT.abbreviation():
        field = cast(Text, field)
        select = cast(list[FieldProp], select)
        return await db_augment_text_field(field, field_id, select)

    elif field_type == FieldTypeName.FILE.abbreviation():
        field = cast(File, field)
        select = cast(list[FileProp], select)
        return await db_augment_file_field(field, field_id, select)

    elif field_type == FieldTypeName.LINK.abbreviation():
        field = cast(Link, field)
        select = cast(list[FieldProp], select)
        return await db_augment_link_field(field, field_id, select)

    elif field_type == FieldTypeName.CONVERSATION.abbreviation():
        field = cast(Conversation, field)
        select = cast(list[ConversationProp], select)
        return await db_augment_conversation_field(field, field_id, select)

    elif field_type == FieldTypeName.GENERIC.abbreviation():
        field = cast(Generic, field)
        select = cast(list[FieldProp], select)
        return await db_augment_generic_field(field, field_id, select)

    else:  # pragma: no cover
        assert False, f"unknown field type: {field_type}"


@augmentor_observer.wrap({"type": "db_text_field"})
async def db_augment_text_field(
    field: Text,
    field_id: FieldId,
    select: Sequence[FieldProp],
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
            if db_value is None:
                continue
            augmented.value = from_proto.field_text(db_value)

        else:  # pragma: no cover
            assert_never(prop)

    return augmented


@augmentor_observer.wrap({"type": "db_file_field"})
async def db_augment_file_field(
    field: File,
    field_id: FieldId,
    select: Sequence[FileProp],
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
            if db_value is None:
                continue
            augmented.value = from_proto.field_file(db_value)

        elif isinstance(prop, FileThumbnail):
            augmented.thumbnail_path = await get_file_thumbnail_path(field, field_id)

        else:  # pragma: no cover
            assert_never(prop)

    return augmented


@augmentor_observer.wrap({"type": "db_link_field"})
async def db_augment_link_field(
    field: Link,
    field_id: FieldId,
    select: Sequence[FieldProp],
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
            if db_value is None:
                continue
            augmented.value = from_proto.field_link(db_value)

        else:  # pragma: no cover
            assert_never(prop)

    return augmented


@augmentor_observer.wrap({"type": "db_conversation_field"})
async def db_augment_conversation_field(
    field: Conversation,
    field_id: FieldId,
    select: list[ConversationProp],
) -> AugmentedConversationField:
    augmented = AugmentedConversationField(id=field.field_id)
    # map (page, index) -> augmented message. The key uniquely identifies and
    # orders messages
    messages: dict[tuple[int, int], AugmentedConversationMessage] = {}

    for prop in select:
        if isinstance(prop, FieldText):
            if isinstance(prop, ConversationText):
                selector = prop.selector
            else:
                # when asking for the conversation text without details, we
                # choose the message if a split is provided in the id or the
                # full conversation otherwise
                if field_id.subfield_id is not None:
                    selector = MessageSelector()
                else:
                    selector = FullSelector()

            # gather the text from each message matching the selector
            extracted_text_pb = await cache.get_field_extracted_text(field)
            async for page, index, message in conversation_selector(field, field_id, selector):
                augmented_message = messages.setdefault(
                    (page, index), AugmentedConversationMessage(ident=message.ident)
                )
                if extracted_text_pb is not None and message.ident in extracted_text_pb.split_text:
                    augmented_message.text = extracted_text_pb.split_text[message.ident]
                else:
                    augmented_message.text = message.content.text

        elif isinstance(prop, FieldValue):
            db_value = await field.get_metadata()
            augmented.value = from_proto.field_conversation(db_value)

        elif isinstance(prop, FieldClassificationLabels):
            augmented.classification_labels = await classification_labels(field_id, field.resource)

        elif isinstance(prop, FieldEntities):
            augmented.entities = await field_entities(field_id, field)

        elif isinstance(prop, ConversationAttachments):
            # Each message on a conversation field can have attachments as
            # references to other fields in the same resource.
            #
            # Here, we iterate through all the messages matched by the selector
            # and collect all the attachment references
            async for page, index, message in conversation_selector(field, field_id, prop.selector):
                augmented_message = messages.setdefault(
                    (page, index), AugmentedConversationMessage(ident=message.ident)
                )
                augmented_message.attachments = []
                for ref in message.content.attachments_fields:
                    field_id = FieldId.from_pb(
                        field.uuid, ref.field_type, ref.field_id, ref.split or None
                    )
                    augmented_message.attachments.append(field_id)

        else:  # pragma: no cover
            assert_never(prop)

    if len(messages) > 0:
        augmented.messages = []
        for (_page, _index), m in sorted(messages.items()):
            augmented.messages.append(m)

    return augmented


@augmentor_observer.wrap({"type": "db_generic_field"})
async def db_augment_generic_field(
    field: Generic,
    field_id: FieldId,
    select: Sequence[FieldProp],
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

        else:  # pragma: no cover
            assert_never(prop)

    return augmented


@augmentor_observer.wrap({"type": "field_text"})
async def get_field_extracted_text(id: FieldId, field: Field) -> str | None:
    extracted_text_pb = await cache.get_field_extracted_text(field)
    if extracted_text_pb is None:  # pragma: no cover
        return None

    if id.subfield_id:
        return extracted_text_pb.split_text[id.subfield_id]
    else:
        return extracted_text_pb.text


async def classification_labels(id: FieldId, resource: Resource) -> dict[str, set[str]] | None:
    basic = await get_basic(resource)
    if basic is None:
        return None

    labels: dict[str, set[str]] = {}
    for fc in basic.computedmetadata.field_classifications:
        if fc.field.field == id.key and fc.field.field_type == id.pb_type:
            for classification in fc.classifications:
                if classification.cancelled_by_user:  # pragma: no cover
                    continue
                labels.setdefault(classification.labelset, set()).add(classification.label)
    return labels


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
    # TODO(decoupled-ask): Remove once processor doesn't use this anymore and remove the positions and ner fields from the message
    for token, family in field_metadata.metadata.ner.items():
        ners.setdefault(family, set()).add(token)

    return ners


async def get_file_thumbnail_path(field: File, field_id: FieldId) -> str | None:
    thumbnail = await field.thumbnail()
    if thumbnail is None:
        return None

    # When ingesting file processed data, we move thumbnails to a owned
    # path. The thumbnail.key must then match this path so we can safely
    # return a path that can be used with the download API to get the
    # actual image
    _expected_prefix = STORAGE_FILE_EXTRACTED.format(
        kbid=field.kbid, uuid=field.uuid, field_type=field_id.type, field=field_id.key, key=""
    )
    assert thumbnail.key.startswith(_expected_prefix), (
        "we use a hardcoded path for file thumbnails and we assume is this"
    )
    thumbnail_path = thumbnail.key.removeprefix(_expected_prefix)

    return thumbnail_path


async def find_conversation_message(
    field: Conversation, ident: str
) -> tuple[int, int, resources_pb2.Message] | None:
    """Find a message in the conversation identified by `ident`."""
    conversation_metadata = await field.get_metadata()
    for page in range(1, conversation_metadata.pages + 1):
        conversation = await field.db_get_value(page)
        for idx, message in enumerate(conversation.messages):
            if message.ident == ident:
                return page, idx, message
    return None


async def iter_conversation_messages(
    field: Conversation,
    *,
    start_from: tuple[int, int] = (1, 0),  # (page, message)
) -> AsyncIterator[tuple[int, int, resources_pb2.Message]]:
    """Iterate through the conversation messages starting from an specific page
    and index.

    """
    start_page, start_index = start_from
    conversation_metadata = await field.get_metadata()
    for page in range(start_page, conversation_metadata.pages + 1):
        conversation = await field.db_get_value(page)
        for idx, message in enumerate(conversation.messages[start_index:]):
            yield (page, start_index + idx, message)
        # next iteration we want all messages
        start_index = 0


async def conversation_answer(
    field: Conversation,
    *,
    start_from: tuple[int, int] = (1, 0),  # (page, message)
) -> tuple[int, int, resources_pb2.Message] | None:
    """Find the next conversation message of type ANSWER starting from an
    specific page and index.

    """
    async for page, index, message in iter_conversation_messages(field, start_from=start_from):
        if message.type == resources_pb2.Message.MessageType.ANSWER:
            return page, index, message
    return None


async def conversation_messages_after(
    field: Conversation,
    *,
    start_from: tuple[int, int] = (1, 0),  # (page, index)
    limit: int | None = None,
) -> AsyncIterator[tuple[int, int, resources_pb2.Message]]:
    assert limit is None or limit > 0, "this function can't iterate backwards"
    async for page, index, message in iter_conversation_messages(field, start_from=start_from):
        yield page, index, message

        if limit is not None:
            limit -= 1
            if limit == 0:
                break


async def conversation_selector(
    field: Conversation,
    field_id: FieldId,
    selector: ConversationSelector,
) -> AsyncIterator[tuple[int, int, resources_pb2.Message]]:
    """Given a conversation, iterate through the messages matched by a
    selector.

    """
    split = field_id.subfield_id

    if isinstance(selector, MessageSelector):
        if selector.id is None and selector.index is None and split is None:
            return

        if selector.index is not None:
            metadata = await field.get_metadata()
            if metadata is None:
                # we can't know about pages/messages
                return

            if isinstance(selector.index, int):
                page = selector.index // metadata.size + 1
                index = selector.index % metadata.size

            elif isinstance(selector.index, str):
                if selector.index == "first":
                    page, index = (1, 0)
                elif selector.index == "last":
                    page = metadata.pages
                    index = metadata.total % metadata.size - 1
                else:  # pragma: no cover
                    assert_never(selector.index)

            else:  # pragma: no cover
                assert_never(selector.index)

            found = None
            async for found in iter_conversation_messages(field, start_from=(page, index)):
                break

            if found is None:
                return

            page, index, message = found
            yield page, index, message

        else:
            # selector.id takes priority over the field id, as it is more specific
            if selector.id is not None:
                split = selector.id
            assert split is not None

            found = await find_conversation_message(field, split)
            if found is None:
                return

            page, index, message = found
            yield page, index, message

    elif isinstance(selector, PageSelector):
        if split is None:
            return
        found = await find_conversation_message(field, split)
        if found is None:
            return
        page, _, _ = found

        conversation_page = await field.db_get_value(page)
        for index, message in enumerate(conversation_page.messages):
            yield page, index, message

    elif isinstance(selector, NeighboursSelector):
        selector = cast(NeighboursSelector, selector)
        if split is None:
            return
        found = await find_conversation_message(field, split)
        if found is None:
            return
        page, index, message = found
        yield page, index, message

        start_from = (page, index + 1)
        async for page, index, message in conversation_messages_after(
            field, start_from=start_from, limit=selector.after
        ):
            yield page, index, message

    elif isinstance(selector, WindowSelector):
        if split is None:
            return
        # Find the position of the `split` message and get the window
        # surrounding it. If there are not enough preceding/following messages,
        # the window won't be centered
        messages: Deque[tuple[int, int, resources_pb2.Message]] = deque(maxlen=selector.size)
        metadata = await field.get_metadata()
        pending = -1
        for page in range(1, metadata.pages + 1):
            conversation_page = await field.db_get_value(page)
            for index, message in enumerate(conversation_page.messages):
                messages.append((page, index, message))
                if pending > 0:
                    pending -= 1
                if message.ident == split:
                    pending = (selector.size - 1) // 2
                if pending == 0:
                    break
            if pending == 0:
                break

        for page, index, message in messages:
            yield page, index, message

    elif isinstance(selector, AnswerSelector):
        if split is None:
            return
        found = await find_conversation_message(field, split)
        if found is None:
            return
        page, index, message = found

        found = await conversation_answer(field, start_from=(page, index))
        if found is not None:
            page, index, answer = found
            yield page, index, answer

    elif isinstance(selector, FullSelector):
        async for page, index, message in iter_conversation_messages(field):
            yield page, index, message

    else:  # pragma: no cover
        assert_never(selector)
