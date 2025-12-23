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
import copy
from collections.abc import Sequence
from dataclasses import dataclass
from typing import cast

import yaml
from pydantic import BaseModel

import nucliadb_models
from nucliadb.common.ids import (
    FIELD_TYPE_STR_TO_NAME,
    FIELD_TYPE_STR_TO_PB,
    FieldId,
    ParagraphId,
)
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.fields.conversation import Conversation
from nucliadb.ingest.fields.file import File
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb.search import logger
from nucliadb.search.augmentor.fields import (
    conversation_answer,
    conversation_messages_after,
    find_conversation_message,
)
from nucliadb.search.search.chat import rpc
from nucliadb.search.search.chat.images import (
    get_file_thumbnail_image,
)
from nucliadb.search.search.metrics import Metrics
from nucliadb_models.augment import (
    AugmentedConversationField,
    AugmentedField,
    AugmentFields,
    AugmentParagraph,
    AugmentParagraphs,
    AugmentRequest,
    AugmentResourceFields,
    AugmentResources,
    ResourceProp,
)
from nucliadb_models.labels import translate_alias_to_system_label
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
from nucliadb_protos.resources_pb2 import FieldComputedMetadata
from nucliadb_utils.utilities import get_storage

# Number of messages to pull after a match in a message
# The hope here is it will be enough to get the answer to the question.
CONVERSATION_MESSAGE_CONTEXT_EXPANSION = 15

TextBlockId = ParagraphId | FieldId


class CappedPromptContext:
    """
    Class to keep track of the size (in number of characters) of the prompt context
    and automatically trim data that exceeds the limit when it's being set on the dictionary.
    """

    def __init__(self, max_size: int | None):
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
    message_type: resources_pb2.Message.MessageType.ValueType | None = None,
    msg_to: str | None = None,
) -> list[resources_pb2.Message]:
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
    found_message = await find_conversation_message(field_obj, mident)
    if found_message is None:  # pragma: no cover
        return []
    page, index, message = found_message

    if message.type == resources_pb2.Message.MessageType.QUESTION:
        # only try to get answer if it was a question
        found = await conversation_answer(field_obj, start_from=(page, index + 1))
        if found is None:
            return []
        else:
            _, _, answer = found
            return [answer]

    else:
        messages_after = []
        async for _, _, message in conversation_messages_after(
            field_obj, start_from=(page, index + 1), limit=max_messages
        ):
            messages_after.append(message)
        return messages_after


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
                    pid = f"{rid}/{field_type}/{field_id}/{msg.ident}/0-{len(msg.content.text)}"
                    context[pid] = text


async def full_resource_prompt_context(
    context: CappedPromptContext,
    kbid: str,
    ordered_paragraphs: list[FindParagraph],
    rid: str | None,
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
    """
    if rid is not None:
        # The user has specified a resource to be included in the context.
        ordered_resources = [rid]
    else:
        # Collect the list of resources in the results (in order of relevance).
        ordered_resources = []
        for paragraph in ordered_paragraphs:
            rid = parse_text_block_id(paragraph.id).rid
            if rid not in ordered_resources:
                skip = False
                if strategy.apply_to is not None:
                    # decide whether the resource should be extended or not
                    for label in strategy.apply_to.exclude:
                        skip = skip or (
                            translate_alias_to_system_label(label) in (paragraph.labels or [])
                        )

                if not skip:
                    ordered_resources.append(rid)
                    # skip when we have enough resource ids
                    if strategy.count is not None and len(ordered_resources) > strategy.count:
                        break

    ordered_resources = ordered_resources[: strategy.count]

    # For each resource, collect the extracted text from all its fields and
    # include the title and summary as well
    augmented = await rpc.augment(
        kbid,
        AugmentRequest(
            resources=AugmentResources(
                given=ordered_resources,
                select=[
                    ResourceProp.TITLE,
                    ResourceProp.SUMMARY,
                ],
                fields=AugmentResourceFields(
                    text=True,
                    filters=[],
                ),
            )
        ),
    )

    extracted_texts = {}
    for rid, resource in augmented.resources.items():
        if resource.title is not None:
            field_id = FieldId(rid=rid, type="a", key="title").full()
            extracted_texts[field_id] = resource.title
        if resource.summary is not None:
            field_id = FieldId(rid=rid, type="a", key="summary").full()
            extracted_texts[field_id] = resource.summary

    for field_id, field in augmented.fields.items():
        field = cast(AugmentedField, field)
        if field.text is not None:
            extracted_texts[field_id] = field.text

    added_fields = set()
    for field_id, extracted_text in extracted_texts.items():
        # First off, remove the text block ids from paragraphs that belong to
        # the same field, as otherwise the context will be duplicated.
        for tb_id in context.text_block_ids():
            if tb_id.startswith(field_id):
                del context[tb_id]
        # Add the extracted text of each field to the context.
        context[field_id] = extracted_text
        augmented_context.fields[field_id] = AugmentedTextBlock(
            id=field_id,
            text=extracted_text,
            augmentation_type=TextBlockAugmentationType.FULL_RESOURCE,
        )

        added_fields.add(field_id)

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
    rids: list[str] = []
    field_ids: list[str] = []
    text_block_ids: list[TextBlockId] = []
    for text_block_id in context.text_block_ids():
        try:
            tb_id = parse_text_block_id(text_block_id)
        except ValueError:  # pragma: no cover
            # Some text block ids are not paragraphs nor fields, so they are skipped
            # (e.g. USER_CONTEXT_0, when the user provides extra context)
            continue

        field_id = tb_id if isinstance(tb_id, FieldId) else tb_id.field_id

        text_block_ids.append(tb_id)
        field_ids.append(field_id.full())
        rids.append(tb_id.rid)

    if len(text_block_ids) == 0:  # pragma: no cover
        return

    resource_select = []
    field_classification_labels = False
    field_entities = False

    ops = 0
    if MetadataExtensionType.ORIGIN in strategy.types:
        ops += 1
        resource_select.append(ResourceProp.ORIGIN)

    if MetadataExtensionType.CLASSIFICATION_LABELS in strategy.types:
        ops += 1
        resource_select.append(ResourceProp.CLASSIFICATION_LABELS)
        field_classification_labels = True

    if MetadataExtensionType.NERS in strategy.types:
        ops += 1
        field_entities = True

    if MetadataExtensionType.EXTRA_METADATA in strategy.types:
        ops += 1
        resource_select.append(ResourceProp.EXTRA)

    metrics.set("metadata_extension_ops", ops * len(text_block_ids))

    augment_req = AugmentRequest()
    if resource_select:
        augment_req.resources = AugmentResources(
            given=rids,
            select=resource_select,
        )
    if field_classification_labels or field_entities:
        augment_req.fields = AugmentFields(
            given=field_ids,
            classification_labels=field_classification_labels,
            entities=field_entities,
        )

    if augment_req.resources is None and augment_req.fields is None:
        # nothing to augment
        return

    augmented = await rpc.augment(kbid, augment_req)

    for tb_id in text_block_ids:
        field_id = tb_id if isinstance(tb_id, FieldId) else tb_id.field_id

        resource = augmented.resources.get(tb_id.rid)
        field = augmented.fields.get(field_id.full())

        if resource is not None:
            if resource.origin is not None:
                text = context.output.pop(tb_id.full())
                extended_text = text + f"\n\nDOCUMENT METADATA AT ORIGIN:\n{to_yaml(resource.origin)}"
                context[tb_id.full()] = extended_text
                augmented_context.paragraphs[tb_id.full()] = AugmentedTextBlock(
                    id=tb_id.full(),
                    text=extended_text,
                    parent=tb_id.full(),
                    augmentation_type=TextBlockAugmentationType.METADATA_EXTENSION,
                )

            if resource.extra is not None:
                text = context.output.pop(tb_id.full())
                extended_text = text + f"\n\nDOCUMENT EXTRA METADATA:\n{to_yaml(resource.extra)}"
                context[tb_id.full()] = extended_text
                augmented_context.paragraphs[tb_id.full()] = AugmentedTextBlock(
                    id=tb_id.full(),
                    text=extended_text,
                    parent=tb_id.full(),
                    augmentation_type=TextBlockAugmentationType.METADATA_EXTENSION,
                )

        if tb_id.full() in context:
            if (resource is not None and resource.classification_labels) or (
                field is not None and field.classification_labels
            ):
                text = context.output.pop(tb_id.full())

                labels_text = "DOCUMENT CLASSIFICATION LABELS:"
                if resource is not None and resource.classification_labels:
                    for labelset, labels in resource.classification_labels.items():
                        for label in labels:
                            labels_text += f"\n - {label} ({labelset})"

                if field is not None and field.classification_labels:
                    for labelset, labels in field.classification_labels.items():
                        for label in labels:
                            labels_text += f"\n - {label} ({labelset})"

                extended_text = text + "\n\n" + labels_text

                context[tb_id.full()] = extended_text
                augmented_context.paragraphs[tb_id.full()] = AugmentedTextBlock(
                    id=tb_id.full(),
                    text=extended_text,
                    parent=tb_id.full(),
                    augmentation_type=TextBlockAugmentationType.METADATA_EXTENSION,
                )

            if field is not None and field.entities:
                ners = field.entities

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


def parse_text_block_id(text_block_id: str) -> TextBlockId:
    try:
        # Typically, the text block id is a paragraph id
        return ParagraphId.from_string(text_block_id)
    except ValueError:
        # When we're doing `full_resource` or `hierarchy` strategies,the text block id
        # is a field id
        return FieldId.from_string(text_block_id)


def to_yaml(obj: BaseModel) -> str:
    # FIXME: this dumps enums REALLY poorly, e.g.,
    # `!!python/object/apply:nucliadb_models.metadata.Source\n- WEB` for
    # Source.WEB instead of `WEB`
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

    select = []
    filters: list[nucliadb_models.filters.Field | nucliadb_models.filters.Generated] = []
    # this strategy exposes a way to access resource title and summary using a
    # field id. However, as they are resource properties, we must request it as
    # that
    for name in strategy.fields:
        if name == "a/title":
            select.append(ResourceProp.TITLE)
        elif name == "a/summary":
            select.append(ResourceProp.SUMMARY)
        else:
            # model already enforces type/name format
            field_type, field_name = name.split("/")
            filters.append(
                nucliadb_models.filters.Field(
                    type=FIELD_TYPE_STR_TO_NAME[field_type], name=field_name or None
                )
            )

    for da_prefix in strategy.data_augmentation_field_prefixes:
        filters.append(nucliadb_models.filters.Generated(by="data-augmentation", da_task=da_prefix))

    augmented = await rpc.augment(
        kbid,
        AugmentRequest(
            resources=AugmentResources(
                given=ordered_resources,
                select=select,
                fields=AugmentResourceFields(
                    text=True,
                    filters=filters,
                ),
            )
        ),
    )

    # REVIEW(decoupled-ask): we don't have the field count anymore, is this good enough?
    metrics.set("field_extension_ops", len(ordered_resources))

    extracted_texts = {}
    # now we need to expose title and summary as fields again, so it gets
    # consistent with the view we are providing in the API
    for rid, augmented_resource in augmented.resources.items():
        if augmented_resource.title:
            extracted_texts[f"{rid}/a/title"] = augmented_resource.title
        if augmented_resource.summary:
            extracted_texts[f"{rid}/a/summary"] = augmented_resource.summary

    for fid, augmented_field in augmented.fields.items():
        if augmented_field is None or augmented_field.text is None:  # pragma: no cover
            continue
        extracted_texts[fid] = augmented_field.text

    for fid, extracted_text in extracted_texts.items():
        # First off, remove the text block ids from paragraphs that belong to
        # the same field, as otherwise the context will be duplicated.
        for tb_id in context.text_block_ids():
            if tb_id.startswith(fid):
                del context[tb_id]
        # Add the extracted text of each field to the beginning of the context.
        if fid not in context:
            context[fid] = extracted_text
            augmented_context.fields[fid] = AugmentedTextBlock(
                id=fid,
                text=extracted_text,
                augmentation_type=TextBlockAugmentationType.FIELD_EXTENSION,
            )

    # Add the extracted text of each paragraph to the end of the context.
    for paragraph in ordered_paragraphs:
        if paragraph.id not in context:
            context[paragraph.id] = _clean_paragraph_text(paragraph)


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

    augmented = await rpc.augment(
        kbid,
        AugmentRequest(
            paragraphs=AugmentParagraphs(
                given=[AugmentParagraph(id=pid.full()) for pid in retrieved_paragraphs_ids],
                text=True,
                neighbours_before=strategy.before,
                neighbours_after=strategy.after,
            )
        ),
    )

    for pid in retrieved_paragraphs_ids:
        paragraph = augmented.paragraphs.get(pid.full())
        if paragraph is None:
            continue

        ptext = paragraph.text or ""
        if ptext and pid.full() not in context:
            context[pid.full()] = ptext

        # Now add the neighbouring paragraphs
        neighbour_ids = [
            *(paragraph.neighbours_before or []),
            *(paragraph.neighbours_after or []),
        ]
        for npid in neighbour_ids:
            neighbour = augmented.paragraphs.get(npid)
            assert neighbour is not None, "augment should never return dangling paragraph references"

            if ParagraphId.from_string(npid) in retrieved_paragraphs_ids or npid in context:
                # already added
                continue

            ntext = neighbour.text
            if not ntext:
                continue

            context[npid] = ntext
            augmented_context.paragraphs[npid] = AugmentedTextBlock(
                id=npid,
                text=ntext,
                # TODO(decoupled-ask): implement neighbour positions
                position=None,
                parent=pid.full(),
                augmentation_type=TextBlockAugmentationType.NEIGHBOURING_PARAGRAPHS,
            )

    metrics.set("neighbouring_paragraphs_ops", len(augmented_context.paragraphs))


def get_text_position(
    paragraph_id: ParagraphId, index: int, field_metadata: FieldComputedMetadata
) -> TextPosition | None:
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
    analyzed_fields: list[str] = []
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

                attachments: list[FieldId] = []

                fid = ParagraphId.from_string(paragraph.id).field_id
                if strategy.full:
                    full_conversation = True
                    max_conversation_messages = None
                else:
                    full_conversation = False
                    max_conversation_messages = strategy.max_messages
                augment = AugmentRequest(
                    fields=AugmentFields(
                        given=[fid.full()],
                        full_conversation=full_conversation,
                        max_conversation_messages=max_conversation_messages,
                        conversation_text_attachments=strategy.attachments_text,
                        conversation_image_attachments=strategy.attachments_images,
                    )
                )

                augmented = await rpc.augment(kbid, augment)

                field = augmented.fields.get(fid.full_without_subfield())
                if field is not None:
                    field = cast(AugmentedConversationField, field)
                    for _message in field.messages or []:
                        ops += 1
                        if not _message.text:
                            continue

                        text = _message.text
                        pid = f"{rid}/{field_type}/{field_id}/{_message.ident}/0-{len(text)}"
                        if pid in context:
                            continue
                        context[pid] = text

                        attachments.extend(
                            [
                                FieldId.from_string(attachment_id)
                                for attachment_id in field.attachments or []
                            ]
                        )
                        augmented_context.paragraphs[pid] = AugmentedTextBlock(
                            id=pid,
                            text=text,
                            parent=paragraph.id,
                            augmentation_type=TextBlockAugmentationType.CONVERSATION,
                        )

                if strategy.attachments_text:
                    ops += len(attachments)

                    augmented = await rpc.augment(
                        kbid,
                        AugmentRequest(
                            fields=AugmentFields(
                                given=[id.full() for id in attachments],
                                text=True,
                            )
                        ),
                    )
                    for attachment_id_str, field in augmented.fields.items():
                        if not field.text:
                            continue

                        attachment_id = FieldId.from_string(attachment_id_str)
                        pid = f"{attachment_id.full_without_subfield()}/0-{len(field.text)}"
                        if pid in context:
                            continue
                        text = f"Attachment {attachment_id.key}: {field.text}\n\n"
                        context[pid] = text
                        augmented_context.paragraphs[pid] = AugmentedTextBlock(
                            id=pid,
                            text=text,
                            parent=paragraph.id,
                            augmentation_type=TextBlockAugmentationType.CONVERSATION,
                        )

                # TODO(decoupled-ask): call /augment with conversation_image_attachments=True
                if strategy.attachments_images and visual_llm:
                    ops += len(attachments)
                    for attachment in attachments:
                        file_field: File = await resource.get_field(
                            attachment.key, attachment.pb_type, load=True
                        )
                        image = await get_file_thumbnail_image(file_field)
                        if image is not None:
                            pid = f"{rid}/f/{attachment.key}/0-0"
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
    resources: dict[str, ExtraCharsParagraph] = {}

    # Iterate paragraphs to get extended text
    paragraphs_to_augment = []
    for paragraph in ordered_paragraphs_copy:
        paragraph_id = ParagraphId.from_string(paragraph.id)
        rid = paragraph_id.rid

        if paragraphs_extra_characters > 0:
            paragraph_id.paragraph_end += paragraphs_extra_characters

        paragraphs_to_augment.append(paragraph_id)

        if rid not in resources:
            # Get the title and the summary of the resource
            title_paragraph_id = ParagraphId(
                field_id=FieldId(
                    rid=rid,
                    type="a",
                    key="title",
                ),
                paragraph_start=0,
                paragraph_end=500,
            )
            summary_paragraph_id = ParagraphId(
                field_id=FieldId(
                    rid=rid,
                    type="a",
                    key="summary",
                ),
                paragraph_start=0,
                paragraph_end=1000,
            )
            paragraphs_to_augment.append(title_paragraph_id)
            paragraphs_to_augment.append(summary_paragraph_id)

            resources[rid] = ExtraCharsParagraph(
                title=title_paragraph_id,
                summary=summary_paragraph_id,
                paragraphs=[(paragraph, paragraph_id)],
            )
        else:
            resources[rid].paragraphs.append((paragraph, paragraph_id))

    metrics.set("hierarchy_ops", len(resources))

    augmented = await rpc.augment(
        kbid,
        AugmentRequest(
            paragraphs=AugmentParagraphs(
                given=[
                    AugmentParagraph(id=paragraph_id.full()) for paragraph_id in paragraphs_to_augment
                ],
                text=True,
            )
        ),
    )

    augmented_paragraphs = set()

    # Modify the first paragraph of each resource to include the title and summary of the resource, as well as the
    # extended paragraph text of all the paragraphs in the resource.
    for values in resources.values():
        augmented_title = augmented.paragraphs.get(values.title.full())
        if augmented_title:
            title_text = augmented_title.text or ""
        else:
            title_text = ""

        augmented_summary = augmented.paragraphs.get(values.summary.full())
        if augmented_summary:
            summary_text = augmented_summary.text or ""
        else:
            summary_text = ""

        first_paragraph = None
        text_with_hierarchy = ""
        for paragraph, paragraph_id in values.paragraphs:
            augmented_paragraph = augmented.paragraphs.get(paragraph_id.full())
            if augmented_paragraph:
                extended_paragraph_text = augmented_paragraph.text or ""
            else:
                extended_paragraph_text = ""

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
        resource: str | None = None,
        user_context: list[str] | None = None,
        user_image_context: list[Image] | None = None,
        strategies: Sequence[RagStrategy] | None = None,
        image_strategies: Sequence[ImageRagStrategy] | None = None,
        max_context_characters: int | None = None,
        visual_llm: bool = False,
        query_image: Image | None = None,
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
        page_image_strategy: PageImageStrategy | None = None
        max_page_images = 5
        table_image_strategy: TableImageStrategy | None = None
        paragraph_image_strategy: ParagraphImageStrategy | None = None
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
                    image = await rpc.download_image(
                        self.kbid,
                        pid.field_id,
                        f"generated/extracted_images_{paragraph_page_number}.png",
                        mime_type="image/png",
                    )
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
                pimage = await rpc.download_image(
                    self.kbid, pid.field_id, f"generated/{paragraph.reference}", mime_type="image/png"
                )
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

        full_resource: FullResourceStrategy | None = None
        hierarchy: HierarchyResourceStrategy | None = None
        neighbouring_paragraphs: NeighbouringParagraphsStrategy | None = None
        field_extension: FieldExtensionStrategy | None = None
        metadata_extension: MetadataExtensionStrategy | None = None
        conversational_strategy: ConversationalStrategy | None = None
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


def get_paragraph_page_number(paragraph: FindParagraph) -> int | None:
    if not paragraph.page_with_visual:
        return None
    if paragraph.position is None:
        return None
    return paragraph.position.page_number


@dataclass
class ExtraCharsParagraph:
    title: ParagraphId
    summary: ParagraphId
    paragraphs: list[tuple[FindParagraph, ParagraphId]]


def _clean_paragraph_text(paragraph: FindParagraph) -> str:
    text = paragraph.text.strip()
    # Do not send highlight marks on prompt context
    text = text.replace("<mark>", "").replace("</mark>", "")
    return text
