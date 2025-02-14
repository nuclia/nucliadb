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
from nucliadb.search.search.paragraphs import get_paragraph_text
from nucliadb_models.labels import translate_alias_to_system_label
from nucliadb_models.metadata import Extra, Origin
from nucliadb_models.search import (
    SCORE_TYPE,
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
)
from nucliadb_protos import resources_pb2
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
    and raise an exception if it exceeds the configured limit.

    This class will automatically trim data that exceeds the limit when it's being
    set on the dictionary.
    """

    def __init__(self, max_size: Optional[int]):
        self.output: PromptContext = {}
        self.images: PromptContextImages = {}
        self.max_size = max_size
        self._size = 0

    def __setitem__(self, key: str, value: str) -> None:
        prev_value_len = len(self.output.get(key, ""))
        if self.max_size is None:
            # Unbounded size context
            to_add = value
        else:
            # Make sure we don't exceed the max size
            size_available = max(self.max_size - self._size + prev_value_len, 0)
            to_add = value[:size_available]
        self.output[key] = to_add
        self._size = self._size - prev_value_len + len(to_add)

    def __getitem__(self, key: str) -> str:
        return self.output.__getitem__(key)

    def __delitem__(self, key: str) -> None:
        value = self.output.pop(key, "")
        self._size -= len(value)

    def text_block_ids(self) -> list[str]:
        return list(self.output.keys())

    @property
    def size(self) -> int:
        return self._size


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
    async with get_driver().transaction(read_only=True) as txn:
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
            added_fields.add(field.full())

    if strategy.include_remaining_text_blocks:
        for paragraph in ordered_paragraphs:
            pid = cast(ParagraphId, parse_text_block_id(paragraph.id))
            if pid.field_id.full() not in added_fields:
                context[paragraph.id] = _clean_paragraph_text(paragraph)


async def extend_prompt_context_with_metadata(
    context: CappedPromptContext,
    kbid: str,
    strategy: MetadataExtensionStrategy,
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

    if MetadataExtensionType.ORIGIN in strategy.types:
        await extend_prompt_context_with_origin_metadata(context, kbid, text_block_ids)

    if MetadataExtensionType.CLASSIFICATION_LABELS in strategy.types:
        await extend_prompt_context_with_classification_labels(context, kbid, text_block_ids)

    if MetadataExtensionType.NERS in strategy.types:
        await extend_prompt_context_with_ner(context, kbid, text_block_ids)

    if MetadataExtensionType.EXTRA_METADATA in strategy.types:
        await extend_prompt_context_with_extra_metadata(context, kbid, text_block_ids)


def parse_text_block_id(text_block_id: str) -> TextBlockId:
    try:
        # Typically, the text block id is a paragraph id
        return ParagraphId.from_string(text_block_id)
    except ValueError:
        # When we're doing `full_resource` or `hierarchy` strategies,the text block id
        # is a field id
        return FieldId.from_string(text_block_id)


async def extend_prompt_context_with_origin_metadata(context, kbid, text_block_ids: list[TextBlockId]):
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
        if origin is not None and tb_id.full() in context.output:
            context[tb_id.full()] += f"\n\nDOCUMENT METADATA AT ORIGIN:\n{to_yaml(origin)}"


async def extend_prompt_context_with_classification_labels(
    context, kbid, text_block_ids: list[TextBlockId]
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
        if labels is not None and tb_id.full() in context.output:
            labels_text = "DOCUMENT CLASSIFICATION LABELS:"
            for labelset, label in labels:
                labels_text += f"\n - {label} ({labelset})"
            context[tb_id.full()] += "\n\n" + labels_text


async def extend_prompt_context_with_ner(context, kbid, text_block_ids: list[TextBlockId]):
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
        if ners is not None and tb_id.full() in context.output:
            ners_text = "DOCUMENT NAMED ENTITIES (NERs):"
            for family, tokens in ners.items():
                ners_text += f"\n - {family}:"
                for token in sorted(list(tokens)):
                    ners_text += f"\n   - {token}"
            context[tb_id.full()] += "\n\n" + ners_text


async def extend_prompt_context_with_extra_metadata(context, kbid, text_block_ids: list[TextBlockId]):
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
        if extra is not None and tb_id.full() in context.output:
            context[tb_id.full()] += f"\n\nDOCUMENT EXTRA METADATA:\n{to_yaml(extra)}"


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
        context[field.full()] = extracted_text

    # Add the extracted text of each paragraph to the end of the context.
    for paragraph in ordered_paragraphs:
        context[paragraph.id] = _clean_paragraph_text(paragraph)


async def get_paragraph_text_with_neighbours(
    kbid: str,
    pid: ParagraphId,
    field_paragraphs: list[ParagraphId],
    before: int = 0,
    after: int = 0,
) -> tuple[ParagraphId, str]:
    """
    This function will get the paragraph text of the paragraph with the neighbouring paragraphs included.
    Parameters:
        kbid: The knowledge box id.
        pid: The matching paragraph id.
        field_paragraphs: The list of paragraph ids of the field.
        before: The number of paragraphs to include before the matching paragraph.
        after: The number of paragraphs to include after the matching paragraph.
    """

    async def _get_paragraph_text(
        kbid: str,
        pid: ParagraphId,
    ) -> tuple[ParagraphId, str]:
        return pid, await get_paragraph_text(
            kbid=kbid,
            paragraph_id=pid,
            log_on_missing_field=True,
        )

    ops = []
    try:
        for paragraph_index in get_neighbouring_paragraph_indexes(
            field_paragraphs=field_paragraphs,
            matching_paragraph=pid,
            before=before,
            after=after,
        ):
            neighbour_pid = field_paragraphs[paragraph_index]
            ops.append(
                asyncio.create_task(
                    _get_paragraph_text(
                        kbid=kbid,
                        pid=neighbour_pid,
                    )
                )
            )
    except ParagraphIdNotFoundInExtractedMetadata:
        logger.warning(
            "Could not find matching paragraph in extracted metadata. This is odd and needs to be investigated.",
            extra={
                "kbid": kbid,
                "matching_paragraph": pid.full(),
                "field_paragraphs": [p.full() for p in field_paragraphs],
            },
        )
        # If we could not find the matching paragraph in the extracted metadata, we can't retrieve
        # the neighbouring paragraphs and we simply fetch the text of the matching paragraph.
        ops.append(
            asyncio.create_task(
                _get_paragraph_text(
                    kbid=kbid,
                    pid=pid,
                )
            )
        )

    results = []
    if len(ops) > 0:
        results = await asyncio.gather(*ops)

    # Sort the results by the paragraph start
    results.sort(key=lambda x: x[0].paragraph_start)
    paragraph_texts = []
    for _, text in results:
        if text != "":
            paragraph_texts.append(text)
    return pid, "\n\n".join(paragraph_texts)


async def get_field_paragraphs_list(
    kbid: str,
    field: FieldId,
    paragraphs: list[ParagraphId],
) -> None:
    """
    Modifies the paragraphs list by adding the paragraph ids of the field, sorted by position.
    """
    resource = await cache.get_resource(kbid, field.rid)
    if resource is None:  # pragma: no cover
        return
    field_obj: Field = await resource.get_field(key=field.key, type=field.pb_type, load=False)
    field_metadata: Optional[resources_pb2.FieldComputedMetadata] = await field_obj.get_field_metadata(
        force=True
    )
    if field_metadata is None:  # pragma: no cover
        return
    for paragraph in field_metadata.metadata.paragraphs:
        paragraphs.append(
            ParagraphId(
                field_id=field,
                paragraph_start=paragraph.start,
                paragraph_end=paragraph.end,
            )
        )


async def neighbouring_paragraphs_prompt_context(
    context: CappedPromptContext,
    kbid: str,
    ordered_text_blocks: list[FindParagraph],
    strategy: NeighbouringParagraphsStrategy,
) -> None:
    """
    This function will get the paragraph texts and then craft a context with the neighbouring paragraphs of the
    paragraphs in the ordered_paragraphs list. The number of paragraphs to include before and after each paragraph
    """
    # First, get the sorted list of paragraphs for each matching field
    # so we can know the indexes of the neighbouring paragraphs
    unique_fields = {
        ParagraphId.from_string(text_block.id).field_id for text_block in ordered_text_blocks
    }
    paragraphs_by_field: dict[FieldId, list[ParagraphId]] = {}
    field_ops = []
    for field_id in unique_fields:
        plist = paragraphs_by_field.setdefault(field_id, [])
        field_ops.append(
            asyncio.create_task(get_field_paragraphs_list(kbid=kbid, field=field_id, paragraphs=plist))
        )
    if field_ops:
        await asyncio.gather(*field_ops)

    # Now, get the paragraph texts with the neighbouring paragraphs
    paragraph_ops = []
    for text_block in ordered_text_blocks:
        pid = ParagraphId.from_string(text_block.id)
        paragraph_ops.append(
            asyncio.create_task(
                get_paragraph_text_with_neighbours(
                    kbid=kbid,
                    pid=pid,
                    before=strategy.before,
                    after=strategy.after,
                    field_paragraphs=paragraphs_by_field.get(pid.field_id, []),
                )
            )
        )
    if not paragraph_ops:  # pragma: no cover
        return

    results: list[tuple[ParagraphId, str]] = await asyncio.gather(*paragraph_ops)
    # Add the paragraph texts to the context
    for pid, text in results:
        if text != "":
            context[pid.full()] = text


async def conversation_prompt_context(
    context: CappedPromptContext,
    kbid: str,
    ordered_paragraphs: list[FindParagraph],
    conversational_strategy: ConversationalStrategy,
    visual_llm: bool,
):
    analyzed_fields: List[str] = []
    async with get_driver().transaction(read_only=True) as txn:
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
                if conversational_strategy.full:
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
                            context[pid] = text
                            attachments.extend(message.content.attachments_fields)
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
                        pid = f"{rid}/{field_type}/{field_id}/{ident}/0-{len(text) + 1}"
                        context[pid] = text
                        attachments.extend(message.content.attachments_fields)

                    messages: Deque[resources_pb2.Message] = deque(
                        maxlen=conversational_strategy.max_messages
                    )

                    pending = -1
                    for page in range(1, cmetadata.pages + 1):
                        # Collect the messages with the window asked by the user arround the match paragraph
                        conv = await field_obj.db_get_value(page)
                        for message in conv.messages:
                            messages.append(message)
                            if pending > 0:
                                pending -= 1
                            if message.ident == mident:
                                pending = (conversational_strategy.max_messages - 1) // 2
                            if pending == 0:
                                break
                        if pending == 0:
                            break

                    for message in messages:
                        text = message.content.text.strip()
                        pid = f"{rid}/{field_type}/{field_id}/{message.ident}/0-{len(message.content.text) + 1}"
                        context[pid] = text
                        attachments.extend(message.content.attachments_fields)

                if conversational_strategy.attachments_text:
                    # add on the context the images if vlm enabled
                    for attachment in attachments:
                        field: File = await resource.get_field(
                            attachment.field_id, attachment.field_type, load=True
                        )  # type: ignore
                        extracted_text = await field.get_extracted_text()
                        if extracted_text is not None:
                            pid = f"{rid}/{field_type}/{attachment.field_id}/0-{len(extracted_text.text) + 1}"
                            context[pid] = f"Attachment {attachment.field_id}: {extracted_text.text}\n\n"

                if conversational_strategy.attachments_images and visual_llm:
                    for attachment in attachments:
                        file_field: File = await resource.get_field(
                            attachment.field_id, attachment.field_type, load=True
                        )  # type: ignore
                        image = await get_file_thumbnail_image(file_field)
                        if image is not None:
                            pid = f"{rid}/f/{attachment.field_id}/0-0"
                            context.images[pid] = image

                analyzed_fields.append(field_unique_id)


async def hierarchy_prompt_context(
    context: CappedPromptContext,
    kbid: str,
    ordered_paragraphs: list[FindParagraph],
    strategy: HierarchyResourceStrategy,
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

    # Now that the paragraphs have been modified, we can add them to the context
    for paragraph in ordered_paragraphs_copy:
        if paragraph.text == "":
            # Skip paragraphs that were cleared in the hierarchy expansion
            continue
        context[paragraph.id] = _clean_paragraph_text(paragraph)
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

    def prepend_user_context(self, context: CappedPromptContext):
        # Chat extra context passed by the user is the most important, therefore
        # it is added first, followed by the found text blocks in order of relevance
        for i, text_block in enumerate(self.user_context or []):
            context[f"USER_CONTEXT_{i}"] = text_block
        for i, image in enumerate(self.user_image_context or []):
            context.images[f"USER_IMAGE_CONTEXT_{i}"] = image

    async def build(
        self,
    ) -> tuple[PromptContext, PromptContextOrder, PromptContextImages]:
        ccontext = CappedPromptContext(max_size=self.max_context_characters)
        self.prepend_user_context(ccontext)
        await self._build_context(ccontext)
        if self.visual_llm:
            await self._build_context_images(ccontext)

        context = ccontext.output
        context_images = ccontext.images
        context_order = {text_block_id: order for order, text_block_id in enumerate(context.keys())}
        return context, context_order, context_images

    async def _build_context_images(self, context: CappedPromptContext) -> None:
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

    async def _build_context(self, context: CappedPromptContext) -> None:
        if self.strategies is None or len(self.strategies) == 0:
            # When no strategy is specified, use the default one
            await default_prompt_context(context, self.kbid, self.ordered_paragraphs)
            return
        else:
            # Add the paragraphs to the context and then apply the strategies
            for paragraph in self.ordered_paragraphs:
                context[paragraph.id] = _clean_paragraph_text(paragraph)

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
            elif (
                strategy.name != RagStrategyName.PREQUERIES and strategy.name != RagStrategyName.GRAPH
            ):  # pragma: no cover
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
            )
            if metadata_extension:
                await extend_prompt_context_with_metadata(context, self.kbid, metadata_extension)
            return

        if hierarchy:
            await hierarchy_prompt_context(
                context,
                self.kbid,
                self.ordered_paragraphs,
                hierarchy,
            )
        if neighbouring_paragraphs:
            await neighbouring_paragraphs_prompt_context(
                context,
                self.kbid,
                self.ordered_paragraphs,
                neighbouring_paragraphs,
            )
        if field_extension:
            await field_extension_prompt_context(
                context,
                self.kbid,
                self.ordered_paragraphs,
                field_extension,
            )
        if conversational_strategy:
            await conversation_prompt_context(
                context,
                self.kbid,
                self.ordered_paragraphs,
                conversational_strategy,
                self.visual_llm,
            )
        if metadata_extension:
            await extend_prompt_context_with_metadata(context, self.kbid, metadata_extension)


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


def get_neighbouring_paragraph_indexes(
    field_paragraphs: list[ParagraphId],
    matching_paragraph: ParagraphId,
    before: int,
    after: int,
) -> list[int]:
    """
    Returns the indexes of the neighbouring paragraphs to fetch (including the matching paragraph).
    """
    assert before >= 0
    assert after >= 0
    try:
        matching_index = field_paragraphs.index(matching_paragraph)
    except ValueError:
        raise ParagraphIdNotFoundInExtractedMetadata(
            f"Matching paragraph {matching_paragraph.full()} not found in extracted metadata"
        )
    start_index = max(0, matching_index - before)
    end_index = min(len(field_paragraphs), matching_index + after + 1)
    return list(range(start_index, end_index))
