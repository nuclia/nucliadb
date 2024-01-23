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
from typing import Optional

from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.fields.conversation import Conversation
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb.ingest.orm.resource import KB_REVERSE, Resource
from nucliadb.middleware.transaction import get_read_only_transaction
from nucliadb.search import logger
from nucliadb_models.search import (
    SCORE_TYPE,
    ContextStrategy,
    FindParagraph,
    KnowledgeboxFindResults,
    RAGOptions,
)
from nucliadb_protos import resources_pb2
from nucliadb_utils.utilities import get_storage

Context = dict[str, str]
ContextOrder = dict[str, int]
MAX_EXTRACTED_TEXTS_TASKS = 10


# Number of messages to pull after a match in a message
# The hope here is it will be enough to get the answer to the question.
CONVERSATION_MESSAGE_CONTEXT_EXPANSION = 15


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
    kbid: str,
    results: KnowledgeboxFindResults,
) -> Context:
    """
    - Returns an ordered dict of context_id -> context_text.
    - context_id is typically the paragraph id, but has a special value for the
      user context. (USER_CONTEXT_0, USER_CONTEXT_1, ...)
    - Paragraphs are inserted in order of relevance, by increasing `order` field
      of the find result paragraphs.
    - User context is inserted first, in order of appearance.
    - Using an dict prevents from duplicates pulled in through conversation expansion.
    """
    output = {}

    # Sort retrieved paragraphs by decreasing order (most relevant first)
    ordered_paras = []
    for result in results.resources.values():
        for field_path, field in result.fields.items():
            for paragraph in field.paragraphs.values():
                ordered_paras.append((field_path, paragraph))
    ordered_paras.sort(key=lambda x: x[1].order, reverse=False)

    driver = get_driver()
    storage = await get_storage()
    async with driver.transaction() as txn:
        kb = KnowledgeBoxORM(txn, storage, kbid)
        for field_path, paragraph in ordered_paras:
            output[paragraph.id] = _clean_paragraph_text(paragraph)

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
                    output[pid] = text

    return output


async def get_field_extracted_text(
    field: Field, max_concurrent_tasks: asyncio.Semaphore
) -> Optional[tuple[Field, str]]:
    async with max_concurrent_tasks:
        extracted_text_pb = await field.get_extracted_text(force=True)
        if extracted_text_pb is None:
            return None
        return field, extracted_text_pb.text


async def get_resource_field_extracted_text(
    kb_obj: KnowledgeBoxORM,
    resource_uuid,
    field_id: str,
    max_concurrent_tasks: asyncio.Semaphore,
) -> Optional[tuple[Resource, Field, str]]:
    resource = await kb_obj.get(resource_uuid)
    if resource is None:
        return None
    field_key, field_type = field_id.split("/")
    field = await resource.get_field(field_key, KB_REVERSE[field_type], load=False)
    if field is None:
        return None
    result = await get_field_extracted_text(field, max_concurrent_tasks)
    if result is None:
        return None
    _, extracted_text = result
    return resource, field, extracted_text


async def get_resource_extracted_texts(
    kb_obj: KnowledgeBoxORM,
    resource_uuid: str,
    max_concurrent_tasks: asyncio.Semaphore,
) -> Optional[list[tuple[Field, str]]]:
    result: list[tuple[Field, str]] = []
    resource = await kb_obj.get(resource_uuid)
    if resource is None:
        return None

    field_tasks = []
    for field_type, field_key in await resource.get_fields(force=True):
        field = await resource.get_field(field_key, field_type, load=False)
        task = asyncio.create_task(
            get_field_extracted_text(field, max_concurrent_tasks)
        )
        field_tasks.append(task)

    done, _ = await asyncio.wait(field_tasks)
    done_task: asyncio.Task
    for done_task in done:
        if done_task.exception() is not None:
            logger.warning(f"Error getting extracted texts: {done_task.exception()}")
            continue
        task_result = done_task.result()
        if task_result is None:
            continue
        result.append(task_result)

    return result


async def full_resource_prompt_context(
    kbid: str,
    results: KnowledgeboxFindResults,
) -> Context:
    """
    Algorithm steps:
        - Collect the list of resources in the results (in order of relevance).
        - For each resource, collect the extracted text from all its fields and craft the context.
    """
    output: Context = {}

    # Collect the list of resources in the results (in order of relevance).
    ordered_paras = get_ordered_paragraphs(results)
    ordered_resources = []
    for paragraph in ordered_paras:
        resource_uuid = paragraph.id.split("/")[0]
        if resource_uuid not in ordered_resources:
            ordered_resources.append(resource_uuid)

    # For each resource, collect the extracted text from all its fields.
    txn = await get_read_only_transaction()
    kb_obj = KnowledgeBoxORM(txn, await get_storage(), kbid)
    max_concurrent_tasks = asyncio.Semaphore(MAX_EXTRACTED_TEXTS_TASKS)
    tasks = []
    for resource_uuid in ordered_resources:
        task = asyncio.create_task(
            get_resource_extracted_texts(kb_obj, resource_uuid, max_concurrent_tasks)
        )
        tasks.append(task)

    done, _ = await asyncio.wait(tasks)
    done_task: asyncio.Task
    for index, done_task in enumerate(done):
        if done_task.exception() is not None:
            logger.warning(f"Error getting extracted texts: {done_task.exception()}")
            continue
        extracted_texts = done_task.result()
        if extracted_texts is None:
            continue
        resource_uuid = ordered_resources[index]
        for field, extracted_text in extracted_texts:
            if extracted_text is None:
                continue
            # Add the extracted text of each field to the beginning of the context.
            output[field.resource_unique_id] = extracted_text
    return output


async def composed_prompt_context(
    kbid: str,
    results: KnowledgeboxFindResults,
    extend_with_fields: list[str],
) -> Context:
    """
    Algorithm steps:
        - Collect the list of resources in the results (in order of relevance).
        - For each resource, collect the extracted text from all its fields.
        - Add the extracted text of each field to the beginning of the context.
        - Add the extracted text of each paragraph to the end of the context.
    """
    output: Context = {}

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
    max_concurrent_tasks = asyncio.Semaphore(MAX_EXTRACTED_TEXTS_TASKS)
    tasks = []
    for resource_uuid in ordered_resources:
        for field_id in extend_with_fields:
            task = asyncio.create_task(
                get_resource_field_extracted_text(
                    kb_obj, resource_uuid, field_id, max_concurrent_tasks
                )
            )
            tasks.append(task)
    done, _ = await asyncio.wait(tasks)
    done_task: asyncio.Task
    for done_task in done:
        if done_task.exception() is not None:
            logger.warning(f"Error getting extracted texts: {done_task.exception()}")
            continue
        task_result = done_task.result()
        if task_result is None:
            continue
        # Add the extracted text of each field to the beginning of the context.
        _, field, extracted_text = task_result
        output[field.resource_unique_id] = extracted_text

    # Add the extracted text of each paragraph to the end of the context.
    for paragraph in ordered_paras:
        output[paragraph.id] = _clean_paragraph_text(paragraph)
    return output


class PromptContextBuilder:
    def __init__(
        self,
        kbid: str,
        find_results: KnowledgeboxFindResults,
        user_context: Optional[list[str]] = None,
        options: Optional[RAGOptions] = None,
    ):
        self.kbid = kbid
        self.find_results = find_results
        self.user_context = user_context
        self.options = options

    def prepend_user_context(self, context: Context) -> Context:
        # Chat extra context passed by the user is the most important, therefore
        # it is added first, followed by the found text blocks in order of relevance
        extended = {
            f"USER_CONTEXT_{i}": text_block
            for i, text_block in enumerate(self.user_context or [])
        }
        extended.update(context)
        return extended

    async def build(self) -> tuple[Context, ContextOrder]:
        context = await self._build_context()
        context = self.prepend_user_context(context)
        context_order = {
            text_block_id: order for order, text_block_id in enumerate(context.keys())
        }
        return context, context_order

    async def _build_context(self):
        if self.options is None or self.options.context_strategies is None:
            return await default_prompt_context(self.kbid, self.find_results)

        chosen_strategies = self.options.context_strategies
        if ContextStrategy.FULL_RESOURCE in chosen_strategies:
            return await full_resource_prompt_context(
                self.kbid, self.find_results, user_context=self.user_context
            )

        return await composed_prompt_context(
            self.kbid,
            self.find_results,
            extend_with_fields=self.options.extend_with_fields or [],
        )


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
