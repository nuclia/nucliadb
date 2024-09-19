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
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple, Union, cast

import yaml
from pydantic import BaseModel

from nucliadb.common.ids import FieldId, ParagraphId
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.fields.conversation import Conversation
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb.ingest.orm.resource import FIELD_TYPE_STR_TO_PB
from nucliadb.search.search import cache
from nucliadb.search.search.chat.images import get_page_image, get_paragraph_image
from nucliadb.search.search.paragraphs import get_paragraph_text
from nucliadb_models.metadata import Extra, Origin
from nucliadb_models.search import (
    SCORE_TYPE,
    FieldExtensionStrategy,
    FindParagraph,
    FullResourceStrategy,
    HierarchyResourceStrategy,
    ImageRagStrategy,
    ImageRagStrategyName,
    KnowledgeboxFindResults,
    MetadataExtensionStrategy,
    MetadataExtensionType,
    NeighbouringParagraphsStrategy,
    PageImageStrategy,
    PreQueryResult,
    PromptContext,
    PromptContextImages,
    PromptContextOrder,
    RagStrategy,
    RagStrategyName,
    TableImageStrategy,
)
from nucliadb_protos import resources_pb2
from nucliadb_utils.asyncio_utils import ConcurrentRunner, run_concurrently
from nucliadb_utils.utilities import get_storage

MAX_RESOURCE_TASKS = 5
MAX_RESOURCE_FIELD_TASKS = 4


# Number of messages to pull after a match in a message
# The hope here is it will be enough to get the answer to the question.
CONVERSATION_MESSAGE_CONTEXT_EXPANSION = 15

TextBlockId = Union[ParagraphId, FieldId]


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
):
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
    *, kb: KnowledgeBoxORM, rid: str, field_id: str, mident: str
) -> list[resources_pb2.Message]:
    resource = await kb.get(rid)
    if resource is None:  # pragma: no cover
        return []
    field_obj = await resource.get_field(field_id, FIELD_TYPE_STR_TO_PB["c"], load=True)
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
            num_messages=CONVERSATION_MESSAGE_CONTEXT_EXPANSION,
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


async def get_field_extracted_text(kbid: str, field_id: FieldId) -> Optional[tuple[FieldId, str]]:
    extracted_text_pb = await cache.get_extracted_text_from_field_id(kbid, field_id)
    if extracted_text_pb is None:  # pragma: no cover
        return None
    return field_id, extracted_text_pb.text


async def get_resource_extracted_texts(
    kbid: str,
    resource_uuid: str,
) -> list[tuple[FieldId, str]]:
    resource = await cache.get_resource(kbid, resource_uuid)
    if resource is None:  # pragma: no cover
        return []

    # Schedule the extraction of the text of each field in the resource
    async with get_driver().transaction(read_only=True) as txn:
        resource.txn = txn
        runner = ConcurrentRunner(max_tasks=MAX_RESOURCE_FIELD_TASKS)
        for field_type, field_key in await resource.get_fields(force=True):
            field_id = FieldId.from_pb(resource_uuid, field_type, field_key)
            runner.schedule(get_field_extracted_text(kbid, field_id))
        # Include the summary aswell
        runner.schedule(
            get_field_extracted_text(kbid, FieldId(rid=resource_uuid, type="a", key="summary"))
        )

        # Wait for the results
        results = await runner.wait()

    return [result for result in results if result is not None]


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
        results: The results of the retrieval (find) operation.
        resource: The resource to be included in the context. This is used only when chatting with a specific resource with no retrieval.
        number_of_full_resources: The number of full resources to include in the context.
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
                ordered_resources.append(resource_uuid)

    # For each resource, collect the extracted text from all its fields.
    resources_extracted_texts = await run_concurrently(
        [
            get_resource_extracted_texts(kbid, resource_uuid)
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
        except ValueError:
            # Some text block ids are not paragraphs nor fields, so they are skipped
            # (e.g. USER_CONTEXT_0, when the user provides extra context)
            continue
    if len(text_block_ids) == 0:
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
                origin = Origin.from_message(pb_origin)
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
                            if classif.cancelled_by_user:
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
                extra = Extra.from_message(pb_extra)
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
            except ValueError:
                # Invalid field id, skiping
                continue

    tasks = [get_field_extracted_text(kbid, fid) for fid in extend_field_ids]
    field_extracted_texts = await run_concurrently(tasks)

    for result in field_extracted_texts:
        if result is None:
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
    if resource is None:
        return
    field_obj: Field = await resource.get_field(key=field.key, type=field.pb_type, load=False)
    field_metadata: Optional[resources_pb2.FieldComputedMetadata] = await field_obj.get_field_metadata(
        force=True
    )
    if field_metadata is None:
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
    if not paragraph_ops:
        return

    results: list[tuple[ParagraphId, str]] = await asyncio.gather(*paragraph_ops)
    # Add the paragraph texts to the context
    for pid, text in results:
        if text != "":
            context[pid.full()] = text


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
        main_results: KnowledgeboxFindResults,
        prequeries_results: Optional[list[PreQueryResult]] = None,
        main_query_weight: float = 1.0,
        resource: Optional[str] = None,
        user_context: Optional[list[str]] = None,
        strategies: Optional[Sequence[RagStrategy]] = None,
        image_strategies: Optional[Sequence[ImageRagStrategy]] = None,
        max_context_characters: Optional[int] = None,
        visual_llm: bool = False,
    ):
        self.kbid = kbid
        self.ordered_paragraphs = get_ordered_paragraphs(
            main_results, prequeries_results, main_query_weight
        )
        self.resource = resource
        self.user_context = user_context
        self.strategies = strategies
        self.image_strategies = image_strategies
        self.max_context_characters = max_context_characters
        self.visual_llm = visual_llm

    def prepend_user_context(self, context: CappedPromptContext):
        # Chat extra context passed by the user is the most important, therefore
        # it is added first, followed by the found text blocks in order of relevance
        for i, text_block in enumerate(self.user_context or []):
            context[f"USER_CONTEXT_{i}"] = text_block

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
        page_count = 5
        gather_pages = False
        gather_tables = False
        if self.image_strategies is not None:
            for strategy in self.image_strategies:
                if strategy.name == ImageRagStrategyName.PAGE_IMAGE:
                    strategy = cast(PageImageStrategy, strategy)
                    gather_pages = True
                    if strategy.count is not None and strategy.count > 0:
                        page_count = strategy.count
                elif strategy.name == ImageRagStrategyName.TABLES:
                    strategy = cast(TableImageStrategy, strategy)
                    gather_tables = True

        for paragraph in self.ordered_paragraphs:
            if paragraph.page_with_visual and paragraph.position:
                if gather_pages and paragraph.position.page_number and len(context.images) < page_count:
                    field = "/".join(paragraph.id.split("/")[:3])
                    page = paragraph.position.page_number
                    page_id = f"{field}/{page}"
                    if page_id not in context.images:
                        context.images[page_id] = await get_page_image(self.kbid, paragraph.id, page)
            # Only send tables if enabled by strategy, by default, send paragraph images
            send_images = (gather_tables and paragraph.is_a_table) or not paragraph.is_a_table
            if send_images and paragraph.reference and paragraph.reference != "":
                image = paragraph.reference
                context.images[paragraph.id] = await get_paragraph_image(self.kbid, paragraph.id, image)

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
        for strategy in self.strategies:
            if strategy.name == RagStrategyName.FIELD_EXTENSION:
                field_extension = cast(FieldExtensionStrategy, strategy)
            elif strategy.name == RagStrategyName.FULL_RESOURCE:
                full_resource = cast(FullResourceStrategy, strategy)
                if self.resource:
                    # When the retrieval is scoped to a specific resource
                    # the full resource strategy only includes that resource
                    full_resource.count = 1
            elif strategy.name == RagStrategyName.HIERARCHY:
                hierarchy = cast(HierarchyResourceStrategy, strategy)
            elif strategy.name == RagStrategyName.NEIGHBOURING_PARAGRAPHS:
                neighbouring_paragraphs = cast(NeighbouringParagraphsStrategy, strategy)
            elif strategy.name == RagStrategyName.METADATA_EXTENSION:
                metadata_extension = cast(MetadataExtensionStrategy, strategy)

        if full_resource:
            # When full resoure is enabled, only metadata extension is allowed.
            await full_resource_prompt_context(
                context, self.kbid, self.ordered_paragraphs, self.resource, full_resource
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
        if metadata_extension:
            await extend_prompt_context_with_metadata(context, self.kbid, metadata_extension)


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


def get_ordered_paragraphs(
    main_results: KnowledgeboxFindResults,
    prequeries_results: Optional[list[PreQueryResult]] = None,
    main_query_weight: float = 1.0,
) -> list[FindParagraph]:
    """
    Returns the list of paragraphs in the results, ordered by relevance (descending score).

    If prequeries_results is provided, the paragraphs of the prequeries are weighted according to the
    normalized weight of the prequery. The paragraph score is not modified, but it is used to determine the order in which they
    are presented in the LLM prompt context.

    If a paragraph is matched in various prequeries, the final weighted score is the sum of the weighted scores for each prequery.

    `main_query_weight` is the weight given to the paragraphs matching the main query when calculating the final score.
    """

    def iter_paragraphs(results: KnowledgeboxFindResults):
        for resource in results.resources.values():
            for field in resource.fields.values():
                for paragraph in field.paragraphs.values():
                    yield paragraph

    total_weights = main_query_weight + sum(prequery.weight for prequery, _ in prequeries_results or [])

    paragraph_id_to_paragraph = {}
    paragraph_id_to_score = {}
    for paragraph in iter_paragraphs(main_results):
        paragraph_id_to_paragraph[paragraph.id] = paragraph
        weighted_score = paragraph.score * (main_query_weight / total_weights) * 100
        paragraph_id_to_score[paragraph.id] = weighted_score

    for prequery, prequery_results in prequeries_results or []:
        for paragraph in iter_paragraphs(prequery_results):
            normalize_weight = (prequery.weight / total_weights) * 100
            weighted_score = paragraph.score * normalize_weight
            if paragraph.id in paragraph_id_to_score:
                # If a paragraph is matched in various prequeries, the final score is the
                # sum of the weighted scores
                paragraph_id_to_paragraph[paragraph.id] = paragraph
                paragraph_id_to_score[paragraph.id] += weighted_score
            else:
                paragraph_id_to_paragraph[paragraph.id] = paragraph
                paragraph_id_to_score[paragraph.id] = weighted_score

    ids_ordered_by_score = sorted(
        paragraph_id_to_paragraph.keys(), key=lambda pid: paragraph_id_to_score[pid], reverse=True
    )
    return [paragraph_id_to_paragraph[pid] for pid in ids_ordered_by_score]


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
    matching_index = field_paragraphs.index(matching_paragraph)
    start_index = max(0, matching_index - before)
    end_index = min(len(field_paragraphs), matching_index + after + 1)
    return list(range(start_index, end_index))
