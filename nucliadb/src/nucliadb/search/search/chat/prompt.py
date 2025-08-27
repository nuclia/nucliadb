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
import copy
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional, Sequence, Tuple, Union, cast

import yaml
from pydantic import BaseModel

from nucliadb.common.ids import FIELD_TYPE_STR_TO_PB, FieldId, ParagraphId
from nucliadb.common.maindb.utils import get_driver
from nucliadb.common.models_utils import from_proto
from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.fields.conversation import Conversation
from nucliadb.ingest.fields.file import File
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb.search import logger
from nucliadb.search.search import cache
from nucliadb.search.search.chat.images import (
    get_file_thumbnail_image,
    get_page_image,
    get_paragraph_image,
)
from nucliadb.search.search.hydrator import hydrate_field_text, hydrate_resource_text
from nucliadb.search.search.metrics import Metrics
from nucliadb.search.search.paragraphs import get_paragraph_text
from nucliadb_models.labels import translate_alias_to_system_label
from nucliadb_models.metadata import Extra, Origin
from nucliadb_models.search import (
    SCORE_TYPE,
    AugmentedContext,
    AugmentedTextBlock,
    ConversationalStrategy,
    FieldExtensionStrategy,
    FindParagraph,
    FullResourceStrategy,
    HierarchyResourceStrategy,
    Image,
    ImageRagStrategy,
    ImageRagStrategyName,
    MetadataExtensionStrategy,
    MetadataExtensionType,
    NeighbouringParagraphsStrategy,
    PageImageStrategy,
    ParagraphImageStrategy,
    PromptContext,
    PromptContextImages,
    PromptContextOrder,
    RagStrategy,
    RagStrategyName,
    TableImageStrategy,
    TextBlockAugmentationType,
    TextPosition,
)
from nucliadb_protos import resources_pb2
from nucliadb_protos.resources_pb2 import ExtractedText, FieldComputedMetadata
from nucliadb_utils.asyncio_utils import run_concurrently
from nucliadb_utils.utilities import get_storage

MAX_RESOURCE_TASKS = 5
MAX_RESOURCE_FIELD_TASKS = 4


# Number of messages to pull after a match in a message
# The hope here is it will be enough to get the answer to the question.
CONVERSATION_MESSAGE_CONTEXT_EXPANSION = 15

TextBlockId = Union[ParagraphId, FieldId]


class ParagraphIdNotFoundInExtractedMetadata(Exception):
    pass


class CappedPromptContext:
    """
    Class to keep track of the size (in number of characters) of the prompt context
    and automatically trim data that exceeds the limit when it's being set on the dictionary.
    """

    def __init__(self, max_size: Optional[int]):
        self.output: PromptContext = {}
        self.images: PromptContextImages = {}
        self.max_size = max_size

    def __setitem__(self, key: str, value: str) -> None:
        self.output.__setitem__(key, value)

    def __getitem__(self, key: str) -> str:
        return self.output.__getitem__(key)

    def __contains__(self, key: str) -> bool:
        return key in self.output

    def __delitem__(self, key: str) -> None:
        try:
            self.output.__delitem__(key)
        except KeyError:
            pass

    def text_block_ids(self) -> list[str]:
        return list(self.output.keys())

    @property
    def size(self) -> int:
        """
        Returns the total size of the context in characters.
        """
        return sum(len(text) for text in self.output.values())

    def cap(self) -> dict[str, str]:
        """
        This method will trim the context to the maximum size if it exceeds it.
        It will remove text from the most recent entries first, until the size is below the limit.
        """
        if self.max_size is None:
            return self.output

        if self.size <= self.max_size:
            return self.output

        logger.info("Removing text from context to fit within the max size limit")
        # Iterate the dictionary in reverse order of insertion
        for key in reversed(list(self.output.keys())):
            current_size = self.size
            if current_size <= self.max_size:
                break
            # Remove text from the value
            text = self.output[key]
            # If removing the whole text still keeps the total size above the limit, remove it
            if current_size - len(text) >= self.max_size:
                del self.output[key]
            else:
                # Otherwise, trim the text to fit within the limit
                excess_size = current_size - self.max_size
                if excess_size > 0:
                    trimmed_text = text[:-excess_size]
                    self.output[key] = trimmed_text
        return self.output


async def get_next_conversation_messages(
    *,
    field_obj: Conversation,
    page: int,
    start_idx: int,
    num_messages: int,
    message_type: Optional[resources_pb2.Message.MessageType.ValueType] = None,
    msg_to: Optional[str] = None,
) -> List[resources_pb2.Message]:
    output = []
    cmetadata = await field_obj.get_metadata()
    for current_page in range(page, cmetadata.pages + 1):
        conv = await field_obj.db_get_value(current_page)
        for message in conv.messages[start_idx:]:
            if message_type is not None and message.type != message_type:  # pragma: no cover
                continue
            if msg_to is not None and msg_to not in message.to:  # pragma: no cover
                continue
            output.append(message)
            if len(output) >= num_messages:
                return output
        start_idx = 0

    return output


async def find_conversation_message(
    field_obj: Conversation, mident: str
) -> tuple[Optional[resources_pb2.Message], int, int]:
    cmetadata = await field_obj.get_metadata()
    for page in range(1, cmetadata.pages + 1):
        conv = await field_obj.db_get_value(page)
        for idx, message in enumerate(conv.messages):
            if message.ident == mident:
                return message, page, idx
    return None, -1, -1


async def get_expanded_conversation_messages(
    *,
    kb: KnowledgeBoxORM,
    rid: str,
    field_id: str,
    mident: str,
    max_messages: int = CONVERSATION_MESSAGE_CONTEXT_EXPANSION,
) -> list[resources_pb2.Message]:
    resource = await kb.get(rid)
    if resource is None:  # pragma: no cover
        return []
    field_obj: Conversation = await resource.get_field(field_id, FIELD_TYPE_STR_TO_PB["c"], load=True)  # type: ignore
    found_message, found_page, found_idx = await find_conversation_message(
        field_obj=field_obj, mident=mident
    )
    if found_message is None:  # pragma: no cover
        return []
    elif found_message.type == resources_pb2.Message.MessageType.QUESTION:
        # only try to get answer if it was a question
        return await get_next_conversation_messages(
            field_obj=field_obj,
            page=found_page,
            start_idx=found_idx + 1,
            num_messages=1,
            message_type=resources_pb2.Message.MessageType.ANSWER,
        )
    else:
        return await get_next_conversation_messages(
            field_obj=field_obj,
            page=found_page,
            start_idx=found_idx + 1,
            num_messages=max_messages,
        )


async def default_prompt_context(
    context: CappedPromptContext,
    kbid: str,
    ordered_paragraphs: list[FindParagraph],
) -> None:
    """
    - Updates context (which is an ordered dict of text_block_id -> context_text).
    - text_block_id is typically the paragraph id, but has a special value for the
      user context. (USER_CONTEXT_0, USER_CONTEXT_1, ...)
    - Paragraphs are inserted in order of relevance, by increasing `order` field
      of the find result paragraphs.
    - User context is inserted first, in order of appearance.
    - Using an dict prevents from duplicates pulled in through conversation expansion.
    """
    # Sort retrieved paragraphs by decreasing order (most relevant first)
    async with get_driver().ro_transaction() as txn:
        storage = await get_storage()
        kb = KnowledgeBoxORM(txn, storage, kbid)
        for paragraph in ordered_paragraphs:
            context[paragraph.id] = _clean_paragraph_text(paragraph)

            # If the paragraph is a conversation and it matches semantically, we assume we
            # have matched with the question, therefore try to include the answer to the
            # context by pulling the next few messages of the conversation field
            rid, field_type, field_id, mident = paragraph.id.split("/")[:4]
            if field_type == "c" and paragraph.score_type in (
                SCORE_TYPE.VECTOR,
                SCORE_TYPE.BOTH,
            ):
                expanded_msgs = await get_expanded_conversation_messages(
                    kb=kb, rid=rid, field_id=field_id, mident=mident
                )
                for msg in expanded_msgs:
                    text = msg.content.text.strip()
                    pid = f"{rid}/{field_type}/{field_id}/{msg.ident}/0-{len(msg.content.text) + 1}"
                    context[pid] = text


async def full_resource_prompt_context(
    context: CappedPromptContext,
    kbid: str,
    ordered_paragraphs: list[FindParagraph],
    resource: Optional[str],
    strategy: FullResourceStrategy,
    metrics: Metrics,
    augmented_context: AugmentedContext,
) -> None:
    """
    Algorithm steps:
        - Collect the list of resources in the results (in order of relevance).
        - For each resource, collect the extracted text from all its fields and craft the context.
    Arguments:
        context: The context to be updated.
        kbid: The knowledge box id.
        ordered_paragraphs: The results of the retrieval (find) operation.
        resource: The resource to be included in the context. This is used only when chatting with a specific resource with no retrieval.
        strategy: strategy instance containing, for example, the number of full resources to include in the context.
    """  # noqa: E501
    if resource is not None:
        # The user has specified a resource to be included in the context.
        ordered_resources = [resource]
    else:
        # Collect the list of resources in the results (in order of relevance).
        ordered_resources = []
        for paragraph in ordered_paragraphs:
            resource_uuid = parse_text_block_id(paragraph.id).rid
            if resource_uuid not in ordered_resources:
                skip = False
                if strategy.apply_to is not None:
                    # decide whether the resource should be extended or not
                    for label in strategy.apply_to.exclude:
                        skip = skip or (
                            translate_alias_to_system_label(label) in (paragraph.labels or [])
                        )

                if not skip:
                    ordered_resources.append(resource_uuid)

    # For each resource, collect the extracted text from all its fields.
    resources_extracted_texts = await run_concurrently(
        [
            hydrate_resource_text(kbid, resource_uuid, max_concurrent_tasks=MAX_RESOURCE_FIELD_TASKS)
            for resource_uuid in ordered_resources[: strategy.count]
        ],
        max_concurrent=MAX_RESOURCE_TASKS,
    )
    added_fields = set()
    for resource_extracted_texts in resources_extracted_texts:
        if resource_extracted_texts is None:
            continue
        for field, extracted_text in resource_extracted_texts:
            # First off, remove the text block ids from paragraphs that belong to
            # the same field, as otherwise the context will be duplicated.
            for tb_id in context.text_block_ids():
                if tb_id.startswith(field.full()):
                    del context[tb_id]
            # Add the extracted text of each field to the context.
            context[field.full()] = extracted_text
            augmented_context.fields[field.full()] = AugmentedTextBlock(
                id=field.full(),
                text=extracted_text,
                augmentation_type=TextBlockAugmentationType.FULL_RESOURCE,
            )

            added_fields.add(field.full())

    metrics.set("full_resource_ops", len(added_fields))

    if strategy.include_remaining_text_blocks:
        for paragraph in ordered_paragraphs:
            pid = cast(ParagraphId, parse_text_block_id(paragraph.id))
            if pid.field_id.full() not in added_fields:
                context[paragraph.id] = _clean_paragraph_text(paragraph)


async def extend_prompt_context_with_metadata(
    context: CappedPromptContext,
    kbid: str,
    strategy: MetadataExtensionStrategy,
    metrics: Metrics,
    augmented_context: AugmentedContext,
) -> None:
    text_block_ids: list[TextBlockId] = []
    for text_block_id in context.text_block_ids():
        try:
            text_block_ids.append(parse_text_block_id(text_block_id))
        except ValueError:  # pragma: no cover
            # Some text block ids are not paragraphs nor fields, so they are skipped
            # (e.g. USER_CONTEXT_0, when the user provides extra context)
            continue
    if len(text_block_ids) == 0:  # pragma: no cover
        return

    ops = 0
    if MetadataExtensionType.ORIGIN in strategy.types:
        ops += 1
        await extend_prompt_context_with_origin_metadata(
            context, kbid, text_block_ids, augmented_context
        )

    if MetadataExtensionType.CLASSIFICATION_LABELS in strategy.types:
        ops += 1
        await extend_prompt_context_with_classification_labels(
            context, kbid, text_block_ids, augmented_context
        )

    if MetadataExtensionType.NERS in strategy.types:
        ops += 1
        await extend_prompt_context_with_ner(context, kbid, text_block_ids, augmented_context)

    if MetadataExtensionType.EXTRA_METADATA in strategy.types:
        ops += 1
        await extend_prompt_context_with_extra_metadata(context, kbid, text_block_ids, augmented_context)

    metrics.set("metadata_extension_ops", ops * len(text_block_ids))


def parse_text_block_id(text_block_id: str) -> TextBlockId:
    try:
        # Typically, the text block id is a paragraph id
        return ParagraphId.from_string(text_block_id)
    except ValueError:
        # When we're doing `full_resource` or `hierarchy` strategies,the text block id
        # is a field id
        return FieldId.from_string(text_block_id)


async def extend_prompt_context_with_origin_metadata(
    context: CappedPromptContext,
    kbid,
    text_block_ids: list[TextBlockId],
    augmented_context: AugmentedContext,
):
    async def _get_origin(kbid: str, rid: str) -> tuple[str, Optional[Origin]]:
        origin = None
        resource = await cache.get_resource(kbid, rid)
        if resource is not None:
            pb_origin = await resource.get_origin()
            if pb_origin is not None:
                origin = from_proto.origin(pb_origin)
        return rid, origin

    rids = {tb_id.rid for tb_id in text_block_ids}
    origins = await run_concurrently([_get_origin(kbid, rid) for rid in rids])
    rid_to_origin = {rid: origin for rid, origin in origins if origin is not None}
    for tb_id in text_block_ids:
        origin = rid_to_origin.get(tb_id.rid)
        if origin is not None and tb_id.full() in context:
            text = context.output.pop(tb_id.full())
            extended_text = text + f"\n\nDOCUMENT METADATA AT ORIGIN:\n{to_yaml(origin)}"
            context[tb_id.full()] = extended_text
            augmented_context.paragraphs[tb_id.full()] = AugmentedTextBlock(
                id=tb_id.full(),
                text=extended_text,
                parent=tb_id.full(),
                augmentation_type=TextBlockAugmentationType.METADATA_EXTENSION,
            )


async def extend_prompt_context_with_classification_labels(
    context: CappedPromptContext,
    kbid: str,
    text_block_ids: list[TextBlockId],
    augmented_context: AugmentedContext,
):
    async def _get_labels(kbid: str, _id: TextBlockId) -> tuple[TextBlockId, list[tuple[str, str]]]:
        fid = _id if isinstance(_id, FieldId) else _id.field_id
        labels = set()
        resource = await cache.get_resource(kbid, fid.rid)
        if resource is not None:
            pb_basic = await resource.get_basic()
            if pb_basic is not None:
                # Add the classification labels of the resource
                for classif in pb_basic.usermetadata.classifications:
                    labels.add((classif.labelset, classif.label))
                # Add the classifications labels of the field
                for fc in pb_basic.computedmetadata.field_classifications:
                    if fc.field.field == fid.key and fc.field.field_type == fid.pb_type:
                        for classif in fc.classifications:
                            if classif.cancelled_by_user:  # pragma: no cover
                                continue
                            labels.add((classif.labelset, classif.label))
        return _id, list(labels)

    classif_labels = await run_concurrently([_get_labels(kbid, tb_id) for tb_id in text_block_ids])
    tb_id_to_labels = {tb_id: labels for tb_id, labels in classif_labels if len(labels) > 0}
    for tb_id in text_block_ids:
        labels = tb_id_to_labels.get(tb_id)
        if labels is not None and tb_id.full() in context:
            text = context.output.pop(tb_id.full())

            labels_text = "DOCUMENT CLASSIFICATION LABELS:"
            for labelset, label in labels:
                labels_text += f"\n - {label} ({labelset})"
            extended_text = text + "\n\n" + labels_text

            context[tb_id.full()] = extended_text
            augmented_context.paragraphs[tb_id.full()] = AugmentedTextBlock(
                id=tb_id.full(),
                text=extended_text,
                parent=tb_id.full(),
                augmentation_type=TextBlockAugmentationType.METADATA_EXTENSION,
            )


async def extend_prompt_context_with_ner(
    context: CappedPromptContext,
    kbid: str,
    text_block_ids: list[TextBlockId],
    augmented_context: AugmentedContext,
):
    async def _get_ners(kbid: str, _id: TextBlockId) -> tuple[TextBlockId, dict[str, set[str]]]:
        fid = _id if isinstance(_id, FieldId) else _id.field_id
        ners: dict[str, set[str]] = {}
        resource = await cache.get_resource(kbid, fid.rid)
        if resource is not None:
            field = await resource.get_field(fid.key, fid.pb_type, load=False)
            fcm = await field.get_field_metadata()
            if fcm is not None:
                # Data Augmentation + Processor entities
                for (
                    data_aumgentation_task_id,
                    entities_wrapper,
                ) in fcm.metadata.entities.items():
                    for entity in entities_wrapper.entities:
                        ners.setdefault(entity.label, set()).add(entity.text)
                # Legacy processor entities
                # TODO: Remove once processor doesn't use this anymore and remove the positions and ner fields from the message
                for token, family in fcm.metadata.ner.items():
                    ners.setdefault(family, set()).add(token)
        return _id, ners

    nerss = await run_concurrently([_get_ners(kbid, tb_id) for tb_id in text_block_ids])
    tb_id_to_ners = {tb_id: ners for tb_id, ners in nerss if len(ners) > 0}
    for tb_id in text_block_ids:
        ners = tb_id_to_ners.get(tb_id)
        if ners is not None and tb_id.full() in context:
            text = context.output.pop(tb_id.full())

            ners_text = "DOCUMENT NAMED ENTITIES (NERs):"
            for family, tokens in ners.items():
                ners_text += f"\n - {family}:"
                for token in sorted(list(tokens)):
                    ners_text += f"\n   - {token}"

            extended_text = text + "\n\n" + ners_text

            context[tb_id.full()] = extended_text
            augmented_context.paragraphs[tb_id.full()] = AugmentedTextBlock(
                id=tb_id.full(),
                text=extended_text,
                parent=tb_id.full(),
                augmentation_type=TextBlockAugmentationType.METADATA_EXTENSION,
            )


async def extend_prompt_context_with_extra_metadata(
    context: CappedPromptContext,
    kbid: str,
    text_block_ids: list[TextBlockId],
    augmented_context: AugmentedContext,
):
    async def _get_extra(kbid: str, rid: str) -> tuple[str, Optional[Extra]]:
        extra = None
        resource = await cache.get_resource(kbid, rid)
        if resource is not None:
            pb_extra = await resource.get_extra()
            if pb_extra is not None:
                extra = from_proto.extra(pb_extra)
        return rid, extra

    rids = {tb_id.rid for tb_id in text_block_ids}
    extras = await run_concurrently([_get_extra(kbid, rid) for rid in rids])
    rid_to_extra = {rid: extra for rid, extra in extras if extra is not None}
    for tb_id in text_block_ids:
        extra = rid_to_extra.get(tb_id.rid)
        if extra is not None and tb_id.full() in context:
            text = context.output.pop(tb_id.full())
            extended_text = text + f"\n\nDOCUMENT EXTRA METADATA:\n{to_yaml(extra)}"
            context[tb_id.full()] = extended_text
            augmented_context.paragraphs[tb_id.full()] = AugmentedTextBlock(
                id=tb_id.full(),
                text=extended_text,
                parent=tb_id.full(),
                augmentation_type=TextBlockAugmentationType.METADATA_EXTENSION,
            )


def to_yaml(obj: BaseModel) -> str:
    return yaml.dump(
        obj.model_dump(exclude_none=True, exclude_defaults=True, exclude_unset=True),
        default_flow_style=False,
        indent=2,
        sort_keys=True,
    )


async def field_extension_prompt_context(
    context: CappedPromptContext,
    kbid: str,
    ordered_paragraphs: list[FindParagraph],
    strategy: FieldExtensionStrategy,
    metrics: Metrics,
    augmented_context: AugmentedContext,
) -> None:
    """
    Algorithm steps:
        - Collect the list of resources in the results (in order of relevance).
        - For each resource, collect the extracted text from all its fields.
        - Add the extracted text of each field to the beginning of the context.
        - Add the extracted text of each paragraph to the end of the context.
    """
    ordered_resources = []
    for paragraph in ordered_paragraphs:
        resource_uuid = ParagraphId.from_string(paragraph.id).rid
        if resource_uuid not in ordered_resources:
            ordered_resources.append(resource_uuid)

    # Fetch the extracted texts of the specified fields for each resource
    extend_fields = strategy.fields
    extend_field_ids = []
    for resource_uuid in ordered_resources:
        for field_id in extend_fields:
            try:
                fid = FieldId.from_string(f"{resource_uuid}/{field_id.strip('/')}")
                extend_field_ids.append(fid)
            except ValueError:  # pragma: no cover
                # Invalid field id, skiping
                continue

    tasks = [hydrate_field_text(kbid, fid) for fid in extend_field_ids]
    field_extracted_texts = await run_concurrently(tasks)

    metrics.set("field_extension_ops", len(field_extracted_texts))

    for result in field_extracted_texts:
        if result is None:  # pragma: no cover
            continue
        field, extracted_text = result
        # First off, remove the text block ids from paragraphs that belong to
        # the same field, as otherwise the context will be duplicated.
        for tb_id in context.text_block_ids():
            if tb_id.startswith(field.full()):
                del context[tb_id]
        # Add the extracted text of each field to the beginning of the context.
        if field.full() not in context:
            context[field.full()] = extracted_text
            augmented_context.fields[field.full()] = AugmentedTextBlock(
                id=field.full(),
                text=extracted_text,
                augmentation_type=TextBlockAugmentationType.FIELD_EXTENSION,
            )

    # Add the extracted text of each paragraph to the end of the context.
    for paragraph in ordered_paragraphs:
        if paragraph.id not in context:
            context[paragraph.id] = _clean_paragraph_text(paragraph)


async def get_orm_field(kbid: str, field_id: FieldId) -> Optional[Field]:
    resource = await cache.get_resource(kbid, field_id.rid)
    if resource is None:  # pragma: no cover
        return None
    return await resource.get_field(key=field_id.key, type=field_id.pb_type, load=False)


async def neighbouring_paragraphs_prompt_context(
    context: CappedPromptContext,
    kbid: str,
    ordered_text_blocks: list[FindParagraph],
    strategy: NeighbouringParagraphsStrategy,
    metrics: Metrics,
    augmented_context: AugmentedContext,
) -> None:
    """
    This function will get the paragraph texts and then craft a context with the neighbouring paragraphs of the
    paragraphs in the ordered_paragraphs list.
    """
    retrieved_paragraphs_ids = [
        ParagraphId.from_string(text_block.id) for text_block in ordered_text_blocks
    ]
    unique_field_ids = list({pid.field_id for pid in retrieved_paragraphs_ids})

    # Get extracted texts and metadatas for all fields
    fm_ops = []
    et_ops = []
    for field_id in unique_field_ids:
        field = await get_orm_field(kbid, field_id)
        if field is None:
            continue
        fm_ops.append(asyncio.create_task(field.get_field_metadata()))
        et_ops.append(asyncio.create_task(field.get_extracted_text()))

    field_metadatas: dict[FieldId, FieldComputedMetadata] = {
        fid: fm for fid, fm in zip(unique_field_ids, await asyncio.gather(*fm_ops)) if fm is not None
    }
    extracted_texts: dict[FieldId, ExtractedText] = {
        fid: et for fid, et in zip(unique_field_ids, await asyncio.gather(*et_ops)) if et is not None
    }

    def _get_paragraph_text(extracted_text: ExtractedText, pid: ParagraphId) -> str:
        if pid.field_id.subfield_id:
            text = extracted_text.split_text.get(pid.field_id.subfield_id) or ""
        else:
            text = extracted_text.text
        return text[pid.paragraph_start : pid.paragraph_end]

    for pid in retrieved_paragraphs_ids:
        # Add the retrieved paragraph first
        field_extracted_text = extracted_texts.get(pid.field_id, None)
        if field_extracted_text is None:
            continue
        ptext = _get_paragraph_text(field_extracted_text, pid)
        if ptext and pid.full() not in context:
            context[pid.full()] = ptext

        # Now add the neighbouring paragraphs
        field_extracted_metadata = field_metadatas.get(pid.field_id, None)
        if field_extracted_metadata is None:
            continue

        field_pids = [
            ParagraphId(
                field_id=pid.field_id,
                paragraph_start=p.start,
                paragraph_end=p.end,
            )
            for p in field_extracted_metadata.metadata.paragraphs
        ]
        try:
            index = field_pids.index(pid)
        except ValueError:
            continue

        for neighbour_index in get_neighbouring_indices(
            index=index,
            before=strategy.before,
            after=strategy.after,
            field_pids=field_pids,
        ):
            if neighbour_index == index:
                # Already handled above
                continue
            try:
                npid = field_pids[neighbour_index]
            except IndexError:
                continue
            if npid in retrieved_paragraphs_ids or npid.full() in context:
                # Already added
                continue
            ptext = _get_paragraph_text(field_extracted_text, npid)
            if not ptext:
                continue
            context[npid.full()] = ptext
            augmented_context.paragraphs[npid.full()] = AugmentedTextBlock(
                id=npid.full(),
                text=ptext,
                position=get_text_position(npid, neighbour_index, field_extracted_metadata),
                parent=pid.full(),
                augmentation_type=TextBlockAugmentationType.NEIGHBOURING_PARAGRAPHS,
            )

    metrics.set("neighbouring_paragraphs_ops", len(augmented_context.paragraphs))


def get_text_position(
    paragraph_id: ParagraphId, index: int, field_metadata: FieldComputedMetadata
) -> Optional[TextPosition]:
    if paragraph_id.field_id.subfield_id:
        metadata = field_metadata.split_metadata[paragraph_id.field_id.subfield_id]
    else:
        metadata = field_metadata.metadata
    try:
        pmetadata = metadata.paragraphs[index]
    except IndexError:
        return None
    page_number = None
    if pmetadata.HasField("page"):
        page_number = pmetadata.page.page
    return TextPosition(
        page_number=page_number,
        index=index,
        start=pmetadata.start,
        end=pmetadata.end,
        start_seconds=list(pmetadata.start_seconds),
        end_seconds=list(pmetadata.end_seconds),
    )


def get_neighbouring_indices(
    index: int, before: int, after: int, field_pids: list[ParagraphId]
) -> list[int]:
    lb_index = max(0, index - before)
    ub_index = min(len(field_pids), index + after + 1)
    return list(range(lb_index, index)) + list(range(index + 1, ub_index))


async def conversation_prompt_context(
    context: CappedPromptContext,
    kbid: str,
    ordered_paragraphs: list[FindParagraph],
    strategy: ConversationalStrategy,
    visual_llm: bool,
    metrics: Metrics,
    augmented_context: AugmentedContext,
):
    analyzed_fields: List[str] = []
    ops = 0
    async with get_driver().ro_transaction() as txn:
        storage = await get_storage()
        kb = KnowledgeBoxORM(txn, storage, kbid)
        for paragraph in ordered_paragraphs:
            if paragraph.id not in context:
                context[paragraph.id] = _clean_paragraph_text(paragraph)

            # If the paragraph is a conversation and it matches semantically, we assume we
            # have matched with the question, therefore try to include the answer to the
            # context by pulling the next few messages of the conversation field
            rid, field_type, field_id, mident = paragraph.id.split("/")[:4]
            if field_type == "c" and paragraph.score_type in (
                SCORE_TYPE.VECTOR,
                SCORE_TYPE.BOTH,
                SCORE_TYPE.BM25,
            ):
                field_unique_id = "-".join([rid, field_type, field_id])
                if field_unique_id in analyzed_fields:
                    continue
                resource = await kb.get(rid)
                if resource is None:  # pragma: no cover
                    continue

                field_obj: Conversation = await resource.get_field(
                    field_id, FIELD_TYPE_STR_TO_PB["c"], load=True
                )  # type: ignore
                cmetadata = await field_obj.get_metadata()

                attachments: List[resources_pb2.FieldRef] = []
                if strategy.full:
                    ops += 5
                    extracted_text = await field_obj.get_extracted_text()
                    for current_page in range(1, cmetadata.pages + 1):
                        conv = await field_obj.db_get_value(current_page)

                        for message in conv.messages:
                            ident = message.ident
                            if extracted_text is not None:
                                text = extracted_text.split_text.get(ident, message.content.text.strip())
                            else:
                                text = message.content.text.strip()
                            pid = f"{rid}/{field_type}/{field_id}/{ident}/0-{len(text) + 1}"
                            attachments.extend(message.content.attachments_fields)
                            if pid in context:
                                continue
                            context[pid] = text
                            augmented_context.paragraphs[pid] = AugmentedTextBlock(
                                id=pid,
                                text=text,
                                parent=paragraph.id,
                                augmentation_type=TextBlockAugmentationType.CONVERSATION,
                            )
                else:
                    # Add first message
                    extracted_text = await field_obj.get_extracted_text()
                    first_page = await field_obj.db_get_value()
                    if len(first_page.messages) > 0:
                        message = first_page.messages[0]
                        ident = message.ident
                        if extracted_text is not None:
                            text = extracted_text.split_text.get(ident, message.content.text.strip())
                        else:
                            text = message.content.text.strip()
                        attachments.extend(message.content.attachments_fields)
                        pid = f"{rid}/{field_type}/{field_id}/{ident}/0-{len(text) + 1}"
                        if pid in context:
                            continue
                        context[pid] = text
                        augmented_context.paragraphs[pid] = AugmentedTextBlock(
                            id=pid,
                            text=text,
                            parent=paragraph.id,
                            augmentation_type=TextBlockAugmentationType.CONVERSATION,
                        )

                    messages: Deque[resources_pb2.Message] = deque(maxlen=strategy.max_messages)

                    pending = -1
                    for page in range(1, cmetadata.pages + 1):
                        # Collect the messages with the window asked by the user arround the match paragraph
                        conv = await field_obj.db_get_value(page)
                        for message in conv.messages:
                            messages.append(message)
                            if pending > 0:
                                pending -= 1
                            if message.ident == mident:
                                pending = (strategy.max_messages - 1) // 2
                            if pending == 0:
                                break
                        if pending == 0:
                            break

                    for message in messages:
                        ops += 1
                        text = message.content.text.strip()
                        attachments.extend(message.content.attachments_fields)
                        pid = f"{rid}/{field_type}/{field_id}/{message.ident}/0-{len(message.content.text) + 1}"
                        if pid in context:
                            continue
                        context[pid] = text
                        augmented_context.paragraphs[pid] = AugmentedTextBlock(
                            id=pid,
                            text=text,
                            parent=paragraph.id,
                            augmentation_type=TextBlockAugmentationType.CONVERSATION,
                        )

                if strategy.attachments_text:
                    # add on the context the images if vlm enabled
                    for attachment in attachments:
                        ops += 1
                        field: File = await resource.get_field(
                            attachment.field_id, attachment.field_type, load=True
                        )  # type: ignore
                        extracted_text = await field.get_extracted_text()
                        if extracted_text is not None:
                            pid = f"{rid}/{field_type}/{attachment.field_id}/0-{len(extracted_text.text) + 1}"
                            if pid in context:
                                continue
                            text = f"Attachment {attachment.field_id}: {extracted_text.text}\n\n"
                            context[pid] = text
                            augmented_context.paragraphs[pid] = AugmentedTextBlock(
                                id=pid,
                                text=text,
                                parent=paragraph.id,
                                augmentation_type=TextBlockAugmentationType.CONVERSATION,
                            )

                if strategy.attachments_images and visual_llm:
                    for attachment in attachments:
                        ops += 1
                        file_field: File = await resource.get_field(
                            attachment.field_id, attachment.field_type, load=True
                        )  # type: ignore
                        image = await get_file_thumbnail_image(file_field)
                        if image is not None:
                            pid = f"{rid}/f/{attachment.field_id}/0-0"
                            context.images[pid] = image

                analyzed_fields.append(field_unique_id)
    metrics.set("conversation_ops", ops)


async def hierarchy_prompt_context(
    context: CappedPromptContext,
    kbid: str,
    ordered_paragraphs: list[FindParagraph],
    strategy: HierarchyResourceStrategy,
    metrics: Metrics,
    augmented_context: AugmentedContext,
) -> None:
    """
    This function will get the paragraph texts (possibly with extra characters, if extra_characters > 0) and then
    craft a context with all paragraphs of the same resource grouped together. Moreover, on each group of paragraphs,
    it includes the resource title and summary so that the LLM can have a better understanding of the context.
    """
    paragraphs_extra_characters = max(strategy.count, 0)
    # Make a copy of the ordered paragraphs to avoid modifying the original list, which is returned
    # in the response to the user
    ordered_paragraphs_copy = copy.deepcopy(ordered_paragraphs)
    resources: Dict[str, ExtraCharsParagraph] = {}

    # Iterate paragraphs to get extended text
    for paragraph in ordered_paragraphs_copy:
        paragraph_id = ParagraphId.from_string(paragraph.id)
        extended_paragraph_text = paragraph.text
        if paragraphs_extra_characters > 0:
            extended_paragraph_text = await get_paragraph_text(
                kbid=kbid,
                paragraph_id=paragraph_id,
                log_on_missing_field=True,
            )
        rid = paragraph_id.rid
        if rid not in resources:
            # Get the title and the summary of the resource
            title_text = await get_paragraph_text(
                kbid=kbid,
                paragraph_id=ParagraphId(
                    field_id=FieldId(
                        rid=rid,
                        type="a",
                        key="title",
                    ),
                    paragraph_start=0,
                    paragraph_end=500,
                ),
                log_on_missing_field=False,
            )
            summary_text = await get_paragraph_text(
                kbid=kbid,
                paragraph_id=ParagraphId(
                    field_id=FieldId(
                        rid=rid,
                        type="a",
                        key="summary",
                    ),
                    paragraph_start=0,
                    paragraph_end=1000,
                ),
                log_on_missing_field=False,
            )
            resources[rid] = ExtraCharsParagraph(
                title=title_text,
                summary=summary_text,
                paragraphs=[(paragraph, extended_paragraph_text)],
            )
        else:
            resources[rid].paragraphs.append((paragraph, extended_paragraph_text))

    metrics.set("hierarchy_ops", len(resources))
    augmented_paragraphs = set()

    # Modify the first paragraph of each resource to include the title and summary of the resource, as well as the
    # extended paragraph text of all the paragraphs in the resource.
    for values in resources.values():
        title_text = values.title
        summary_text = values.summary
        first_paragraph = None
        text_with_hierarchy = ""
        for paragraph, extended_paragraph_text in values.paragraphs:
            if first_paragraph is None:
                first_paragraph = paragraph
            text_with_hierarchy += "\n EXTRACTED BLOCK: \n " + extended_paragraph_text + " \n\n "
            # All paragraphs of the resource are cleared except the first one, which will be the
            # one containing the whole hierarchy information
            paragraph.text = ""

        if first_paragraph is not None:
            # The first paragraph is the only one holding the hierarchy information
            first_paragraph.text = f"DOCUMENT: {title_text} \n SUMMARY: {summary_text} \n RESOURCE CONTENT: {text_with_hierarchy}"
            augmented_paragraphs.add(first_paragraph.id)

    # Now that the paragraphs have been modified, we can add them to the context
    for paragraph in ordered_paragraphs_copy:
        if paragraph.text == "":
            # Skip paragraphs that were cleared in the hierarchy expansion
            continue
        paragraph_text = _clean_paragraph_text(paragraph)
        context[paragraph.id] = paragraph_text
        if paragraph.id in augmented_paragraphs:
            pid = ParagraphId.from_string(paragraph.id)
            augmented_context.paragraphs[pid.full()] = AugmentedTextBlock(
                id=pid.full(), text=paragraph_text, augmentation_type=TextBlockAugmentationType.HIERARCHY
            )
    return


class PromptContextBuilder:
    """
    Builds the context for the LLM prompt.
    """

    def __init__(
        self,
        kbid: str,
        ordered_paragraphs: list[FindParagraph],
        resource: Optional[str] = None,
        user_context: Optional[list[str]] = None,
        user_image_context: Optional[list[Image]] = None,
        strategies: Optional[Sequence[RagStrategy]] = None,
        image_strategies: Optional[Sequence[ImageRagStrategy]] = None,
        max_context_characters: Optional[int] = None,
        visual_llm: bool = False,
        query_image: Optional[Image] = None,
        metrics: Metrics = Metrics("prompt_context_builder"),
    ):
        self.kbid = kbid
        self.ordered_paragraphs = ordered_paragraphs
        self.resource = resource
        self.user_context = user_context
        self.user_image_context = user_image_context
        self.strategies = strategies
        self.image_strategies = image_strategies
        self.max_context_characters = max_context_characters
        self.visual_llm = visual_llm
        self.metrics = metrics
        self.query_image = query_image
        self.augmented_context = AugmentedContext(paragraphs={}, fields={})

    def prepend_user_context(self, context: CappedPromptContext):
        # Chat extra context passed by the user is the most important, therefore
        # it is added first, followed by the found text blocks in order of relevance
        for i, text_block in enumerate(self.user_context or []):
            context[f"USER_CONTEXT_{i}"] = text_block
        # Add the query image as part of the image context
        if self.query_image is not None:
            context.images["QUERY_IMAGE"] = self.query_image
        else:
            for i, image in enumerate(self.user_image_context or []):
                context.images[f"USER_IMAGE_CONTEXT_{i}"] = image

    async def build(
        self,
    ) -> tuple[PromptContext, PromptContextOrder, PromptContextImages, AugmentedContext]:
        ccontext = CappedPromptContext(max_size=self.max_context_characters)
        self.prepend_user_context(ccontext)
        await self._build_context(ccontext)
        if self.visual_llm and not self.query_image:
            await self._build_context_images(ccontext)
        context = ccontext.cap()
        context_images = ccontext.images
        context_order = {text_block_id: order for order, text_block_id in enumerate(context.keys())}
        return context, context_order, context_images, self.augmented_context

    async def _build_context_images(self, context: CappedPromptContext) -> None:
        ops = 0
        if self.image_strategies is None or len(self.image_strategies) == 0:
            # Nothing to do
            return
        page_image_strategy: Optional[PageImageStrategy] = None
        max_page_images = 5
        table_image_strategy: Optional[TableImageStrategy] = None
        paragraph_image_strategy: Optional[ParagraphImageStrategy] = None
        for strategy in self.image_strategies:
            if strategy.name == ImageRagStrategyName.PAGE_IMAGE:
                if page_image_strategy is None:
                    page_image_strategy = cast(PageImageStrategy, strategy)
                    if page_image_strategy.count is not None:
                        max_page_images = page_image_strategy.count
            elif strategy.name == ImageRagStrategyName.TABLES:
                if table_image_strategy is None:
                    table_image_strategy = cast(TableImageStrategy, strategy)
            elif strategy.name == ImageRagStrategyName.PARAGRAPH_IMAGE:
                if paragraph_image_strategy is None:
                    paragraph_image_strategy = cast(ParagraphImageStrategy, strategy)
            else:  # pragma: no cover
                logger.warning(
                    "Unknown image strategy",
                    extra={"strategy": strategy.name, "kbid": self.kbid},
                )
        page_images_added = 0
        for paragraph in self.ordered_paragraphs:
            pid = ParagraphId.from_string(paragraph.id)
            paragraph_page_number = get_paragraph_page_number(paragraph)
            if (
                page_image_strategy is not None
                and page_images_added < max_page_images
                and paragraph_page_number is not None
            ):
                # page_image_id: rid/f/myfield/0
                page_image_id = "/".join([pid.field_id.full(), str(paragraph_page_number)])
                if page_image_id not in context.images:
                    image = await get_page_image(self.kbid, pid, paragraph_page_number)
                    if image is not None:
                        ops += 1
                        context.images[page_image_id] = image
                        page_images_added += 1
                    else:
                        logger.warning(
                            f"Could not retrieve image for paragraph from storage",
                            extra={
                                "kbid": self.kbid,
                                "paragraph": pid.full(),
                                "page_number": paragraph_page_number,
                            },
                        )

            add_table = table_image_strategy is not None and paragraph.is_a_table
            add_paragraph = paragraph_image_strategy is not None and not paragraph.is_a_table
            if (add_table or add_paragraph) and (
                paragraph.reference is not None and paragraph.reference != ""
            ):
                pimage = await get_paragraph_image(self.kbid, pid, paragraph.reference)
                if pimage is not None:
                    ops += 1
                    context.images[paragraph.id] = pimage
                else:
                    logger.warning(
                        f"Could not retrieve image for paragraph from storage",
                        extra={
                            "kbid": self.kbid,
                            "paragraph": pid.full(),
                            "reference": paragraph.reference,
                        },
                    )
        self.metrics.set("image_ops", ops)

    async def _build_context(self, context: CappedPromptContext) -> None:
        if self.strategies is None or len(self.strategies) == 0:
            # When no strategy is specified, use the default one
            await default_prompt_context(context, self.kbid, self.ordered_paragraphs)
            return
        else:
            # Add the paragraphs to the context and then apply the strategies
            for paragraph in self.ordered_paragraphs:
                context[paragraph.id] = _clean_paragraph_text(paragraph)

        strategies_not_handled_here = [
            RagStrategyName.PREQUERIES,
            RagStrategyName.GRAPH,
        ]

        full_resource: Optional[FullResourceStrategy] = None
        hierarchy: Optional[HierarchyResourceStrategy] = None
        neighbouring_paragraphs: Optional[NeighbouringParagraphsStrategy] = None
        field_extension: Optional[FieldExtensionStrategy] = None
        metadata_extension: Optional[MetadataExtensionStrategy] = None
        conversational_strategy: Optional[ConversationalStrategy] = None
        for strategy in self.strategies:
            if strategy.name == RagStrategyName.FIELD_EXTENSION:
                field_extension = cast(FieldExtensionStrategy, strategy)
            elif strategy.name == RagStrategyName.CONVERSATION:
                conversational_strategy = cast(ConversationalStrategy, strategy)
            elif strategy.name == RagStrategyName.FULL_RESOURCE:
                full_resource = cast(FullResourceStrategy, strategy)
                if self.resource:  # pragma: no cover
                    # When the retrieval is scoped to a specific resource
                    # the full resource strategy only includes that resource
                    full_resource.count = 1
            elif strategy.name == RagStrategyName.HIERARCHY:
                hierarchy = cast(HierarchyResourceStrategy, strategy)
            elif strategy.name == RagStrategyName.NEIGHBOURING_PARAGRAPHS:
                neighbouring_paragraphs = cast(NeighbouringParagraphsStrategy, strategy)
            elif strategy.name == RagStrategyName.METADATA_EXTENSION:
                metadata_extension = cast(MetadataExtensionStrategy, strategy)
            elif strategy.name not in strategies_not_handled_here:  # pragma: no cover
                # Prequeries and graph are not handled here
                logger.warning(
                    "Unknown rag strategy",
                    extra={"strategy": strategy.name, "kbid": self.kbid},
                )

        if full_resource:
            # When full resoure is enabled, only metadata extension is allowed.
            await full_resource_prompt_context(
                context,
                self.kbid,
                self.ordered_paragraphs,
                self.resource,
                full_resource,
                self.metrics,
                self.augmented_context,
            )
            if metadata_extension:
                await extend_prompt_context_with_metadata(
                    context,
                    self.kbid,
                    metadata_extension,
                    self.metrics,
                    self.augmented_context,
                )
            return

        if hierarchy:
            await hierarchy_prompt_context(
                context,
                self.kbid,
                self.ordered_paragraphs,
                hierarchy,
                self.metrics,
                self.augmented_context,
            )
        if neighbouring_paragraphs:
            await neighbouring_paragraphs_prompt_context(
                context,
                self.kbid,
                self.ordered_paragraphs,
                neighbouring_paragraphs,
                self.metrics,
                self.augmented_context,
            )
        if field_extension:
            await field_extension_prompt_context(
                context,
                self.kbid,
                self.ordered_paragraphs,
                field_extension,
                self.metrics,
                self.augmented_context,
            )
        if conversational_strategy:
            await conversation_prompt_context(
                context,
                self.kbid,
                self.ordered_paragraphs,
                conversational_strategy,
                self.visual_llm,
                self.metrics,
                self.augmented_context,
            )
        if metadata_extension:
            await extend_prompt_context_with_metadata(
                context,
                self.kbid,
                metadata_extension,
                self.metrics,
                self.augmented_context,
            )


def get_paragraph_page_number(paragraph: FindParagraph) -> Optional[int]:
    if not paragraph.page_with_visual:
        return None
    if paragraph.position is None:
        return None
    return paragraph.position.page_number


@dataclass
class ExtraCharsParagraph:
    title: str
    summary: str
    paragraphs: List[Tuple[FindParagraph, str]]


def _clean_paragraph_text(paragraph: FindParagraph) -> str:
    text = paragraph.text.strip()
    # Do not send highlight marks on prompt context
    text = text.replace("<mark>", "").replace("</mark>", "")
    return text
