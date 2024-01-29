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
from typing import Optional, Sequence

from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.fields.conversation import Conversation
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb.ingest.orm.resource import KB_REVERSE
from nucliadb.ingest.orm.resource import Resource as ResourceORM
from nucliadb.middleware.transaction import get_read_only_transaction
from nucliadb.search import logger
from nucliadb_models.search import (
    SCORE_TYPE,
    FindParagraph,
    KnowledgeboxFindResults,
    PromptContext,
    PromptContextOrder,
    RagStrategy,
    RagStrategyName,
)
from nucliadb_protos import resources_pb2
from nucliadb_utils.asyncio_utils import ConcurrentRunner, run_concurrently
from nucliadb_utils.utilities import get_storage

MAX_RESOURCE_TASKS = 5
MAX_RESOURCE_FIELD_TASKS = 4


# Number of messages to pull after a match in a message
# The hope here is it will be enough to get the answer to the question.
CONVERSATION_MESSAGE_CONTEXT_EXPANSION = 15


class MaxContextSizeExceeded(Exception):
    pass


class CappedPromptContext:
    """
    Class to keep track of the size of the prompt context and raise an exception if it exceeds the configured limit.
    """

    def __init__(self, max_size: Optional[int]):
        self.output: PromptContext = {}
        self.max_size = max_size
        self._size = 0

    def _check_size(self, size_delta: int = 0):
        if self.max_size is None:
            # No limit on the size of the context
            return
        if self._size + size_delta > self.max_size:
            raise MaxContextSizeExceeded(
                f"Prompt context size exceeded: {self.max_size}"
            )

    def __setitem__(self, key, value):
        try:
            # Existing key
            size_delta = len(value) - len(self.output[key])
        except KeyError:
            # New key
            size_delta = len(value)
        self._check_size(size_delta)
        self._size += size_delta
        self.output[key] = value

    @property
    def size(self):
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
            if message_type is not None and message.type != message_type:
                continue
            if msg_to is not None and msg_to not in message.to:
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
    if resource is None:
        return []
    field_obj = await resource.get_field(field_id, KB_REVERSE["c"], load=True)
    found_message, found_page, found_idx = await find_conversation_message(
        field_obj=field_obj, mident=mident
    )
    if found_message is None:
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
    results: KnowledgeboxFindResults,
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
    ordered_paras = get_ordered_paragraphs(results)
    txn = await get_read_only_transaction()
    storage = await get_storage()
    kb = KnowledgeBoxORM(txn, storage, kbid)
    for paragraph in ordered_paras:
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


async def get_field_extracted_text(field: Field) -> Optional[tuple[Field, str]]:
    extracted_text_pb = await field.get_extracted_text(force=True)
    if extracted_text_pb is None:
        return None
    return field, extracted_text_pb.text


async def get_resource_field_extracted_text(
    kb_obj: KnowledgeBoxORM,
    resource_uuid,
    field_id: str,
) -> Optional[tuple[Field, str]]:
    resource = await kb_obj.get(resource_uuid)
    if resource is None:
        return None

    try:
        field_type, field_key = field_id.strip("/").split("/")
    except ValueError:
        logger.error(f"Invalid field id: {field_id}. Skipping getting extracted text.")
        return None
    field = await resource.get_field(field_key, KB_REVERSE[field_type], load=False)
    if field is None:
        return None
    result = await get_field_extracted_text(field)
    if result is None:
        return None
    _, extracted_text = result
    return field, extracted_text


async def get_resource_extracted_texts(
    kbid: str,
    resource_uuid: str,
) -> list[tuple[Field, str]]:
    txn = await get_read_only_transaction()
    storage = await get_storage()
    kb = KnowledgeBoxORM(txn, storage, kbid)
    resource = ResourceORM(
        txn=txn,
        storage=storage,
        kb=kb,
        uuid=resource_uuid,
    )

    # Schedule the extraction of the text of each field in the resource
    runner = ConcurrentRunner(max_tasks=MAX_RESOURCE_FIELD_TASKS)
    for field_type, field_key in await resource.get_fields(force=True):
        field = await resource.get_field(field_key, field_type, load=False)
        runner.schedule(get_field_extracted_text(field))

    # Wait for the results
    results = await runner.wait()
    return [result for result in results if result is not None]


async def full_resource_prompt_context(
    context: CappedPromptContext,
    kbid: str,
    results: KnowledgeboxFindResults,
) -> None:
    """
    Algorithm steps:
        - Collect the list of resources in the results (in order of relevance).
        - For each resource, collect the extracted text from all its fields and craft the context.
    """

    # Collect the list of resources in the results (in order of relevance).
    ordered_paras = get_ordered_paragraphs(results)
    ordered_resources = []
    for paragraph in ordered_paras:
        resource_uuid = paragraph.id.split("/")[0]
        if resource_uuid not in ordered_resources:
            ordered_resources.append(resource_uuid)

    # For each resource, collect the extracted text from all its fields.
    resource_extracted_texts = await run_concurrently(
        [
            get_resource_extracted_texts(kbid, resource_uuid)
            for resource_uuid in ordered_resources
        ],
        max_concurrent=MAX_RESOURCE_TASKS,
    )

    for extracted_texts in resource_extracted_texts:
        if extracted_texts is None:
            continue
        for field, extracted_text in extracted_texts:
            # Add the extracted text of each field to the context.
            context[field.resource_unique_id] = extracted_text


async def composed_prompt_context(
    context: CappedPromptContext,
    kbid: str,
    results: KnowledgeboxFindResults,
    extend_with_fields: list[str],
) -> None:
    """
    Algorithm steps:
        - Collect the list of resources in the results (in order of relevance).
        - For each resource, collect the extracted text from all its fields.
        - Add the extracted text of each field to the beginning of the context.
        - Add the extracted text of each paragraph to the end of the context.
    """
    # Collect the list of resources in the results (in order of relevance).
    ordered_paras = get_ordered_paragraphs(results)
    ordered_resources = []
    for paragraph in ordered_paras:
        resource_uuid = paragraph.id.split("/")[0]
        if resource_uuid not in ordered_resources:
            ordered_resources.append(resource_uuid)

    # Fetch the extracted texts of the specified fields for each resource
    txn = await get_read_only_transaction()
    kb_obj = KnowledgeBoxORM(txn, await get_storage(), kbid)

    tasks = [
        get_resource_field_extracted_text(kb_obj, resource_uuid, field_id)
        for resource_uuid in ordered_resources
        for field_id in extend_with_fields
    ]
    field_extracted_texts = await run_concurrently(tasks)

    for result in field_extracted_texts:
        if result is None:
            continue
        # Add the extracted text of each field to the beginning of the context.
        field, extracted_text = result
        context[field.resource_unique_id] = extracted_text

    # Add the extracted text of each paragraph to the end of the context.
    for paragraph in ordered_paras:
        context[paragraph.id] = _clean_paragraph_text(paragraph)


class PromptContextBuilder:
    """
    Builds the context for the LLM prompt.
    """

    def __init__(
        self,
        kbid: str,
        find_results: KnowledgeboxFindResults,
        user_context: Optional[list[str]] = None,
        strategies: Optional[Sequence[RagStrategy]] = None,
        max_context_size: Optional[int] = None,
    ):
        self.kbid = kbid
        self.find_results = find_results
        self.user_context = user_context
        self.strategies = strategies
        self.max_context_size = max_context_size

    def prepend_user_context(self, context: CappedPromptContext):
        # Chat extra context passed by the user is the most important, therefore
        # it is added first, followed by the found text blocks in order of relevance
        for i, text_block in enumerate(self.user_context or []):
            context[f"USER_CONTEXT_{i}"] = text_block

    async def build(self) -> tuple[PromptContext, PromptContextOrder]:
        ccontext = CappedPromptContext(max_size=self.max_context_size)
        try:
            self.prepend_user_context(ccontext)
            await self._build_context(ccontext)
        except MaxContextSizeExceeded:
            logger.warning(
                f"Prompt context size exceeded: {ccontext.size}."
                f"The context will be truncated to the maximum size: {self.max_context_size}."
            )
        context = ccontext.output
        context_order = {
            text_block_id: order for order, text_block_id in enumerate(context.keys())
        }
        return context, context_order

    async def _build_context(self, context: CappedPromptContext) -> None:
        if self.strategies is None or len(self.strategies) == 0:
            await default_prompt_context(context, self.kbid, self.find_results)
            return

        full_resource = False
        extend_with_fields = []
        for strategy in self.strategies:
            if strategy.name == RagStrategyName.FIELD_EXTENSION:
                extend_with_fields.extend(strategy.fields)  # type: ignore
            elif strategy.name == RagStrategyName.FULL_RESOURCE:
                full_resource = True

        if full_resource:
            await full_resource_prompt_context(context, self.kbid, self.find_results)
            return

        await composed_prompt_context(
            context,
            self.kbid,
            self.find_results,
            extend_with_fields=extend_with_fields,
        )
        return


def _clean_paragraph_text(paragraph: FindParagraph) -> str:
    text = paragraph.text.strip()
    # Do not send highlight marks on prompt context
    text = text.replace("<mark>", "").replace("</mark>", "")
    return text


def get_ordered_paragraphs(results: KnowledgeboxFindResults) -> list[FindParagraph]:
    """
    Returns the list of paragraphs in the results, ordered by relevance.
    """
    return sorted(
        [
            paragraph
            for resource in results.resources.values()
            for field in resource.fields.values()
            for paragraph in field.paragraphs.values()
        ],
        key=lambda paragraph: paragraph.order,
        reverse=False,
    )
