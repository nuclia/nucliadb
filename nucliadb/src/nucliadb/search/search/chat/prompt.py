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
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple, cast

from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.fields.conversation import Conversation
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb.ingest.orm.resource import KB_REVERSE
from nucliadb.ingest.orm.resource import Resource as ResourceORM
from nucliadb.middleware.transaction import get_read_only_transaction
from nucliadb.search import logger
from nucliadb.search.search import paragraphs
from nucliadb.search.search.chat.images import get_page_image, get_paragraph_image
from nucliadb_models.search import (
    SCORE_TYPE,
    FieldExtensionStrategy,
    FindParagraph,
    FullResourceStrategy,
    HierarchyResourceStrategy,
    ImageRagStrategy,
    ImageRagStrategyName,
    KnowledgeboxFindResults,
    PageImageStrategy,
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
        if self.max_size is None:
            # Unbounded size
            self.output[key] = value
        else:
            existing_len = len(self.output.get(key, ""))
            self._size -= existing_len
            size_available = self.max_size - self._size
            if size_available > 0:
                self.output[key] = value[:size_available]
                self._size += len(self.output[key])

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
    txn = await get_read_only_transaction()
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
    ordered_paragraphs: list[FindParagraph],
    resource: Optional[str],
    number_of_full_resources: Optional[int] = None,
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
            resource_uuid = paragraph.id.split("/")[0]
            if resource_uuid not in ordered_resources:
                ordered_resources.append(resource_uuid)

    # For each resource, collect the extracted text from all its fields.
    resource_extracted_texts = await run_concurrently(
        [
            get_resource_extracted_texts(kbid, resource_uuid)
            for resource_uuid in ordered_resources[:number_of_full_resources]
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
    ordered_paragraphs: list[FindParagraph],
    extend_with_fields: list[str],
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
    for paragraph in ordered_paragraphs:
        context[paragraph.id] = _clean_paragraph_text(paragraph)


async def hierarchy_prompt_context(
    context: CappedPromptContext,
    kbid: str,
    ordered_paragraphs: list[FindParagraph],
    paragraphs_extra_characters: int = 0,
) -> None:
    """
    This function will get the paragraph texts (possibly with extra characters, if extra_characters > 0) and then
    craft a context with all paragraphs of the same resource grouped together. Moreover, on each group of paragraphs,
    it includes the resource title and summary so that the LLM can have a better understanding of the context.
    """
    paragraphs_extra_characters = max(paragraphs_extra_characters, 0)
    # Make a copy of the ordered paragraphs to avoid modifying the original list, which is returned
    # in the response to the user
    ordered_paragraphs_copy = copy.deepcopy(ordered_paragraphs)
    etcache = paragraphs.ExtractedTextCache()
    resources: Dict[str, ExtraCharsParagraph] = {}

    # Iterate paragraphs to get extended text
    for paragraph in ordered_paragraphs_copy:
        rid, field_type, field = paragraph.id.split("/")[:3]
        field_path = "/".join([rid, field_type, field])
        position = paragraph.id.split("/")[-1]
        start, end = position.split("-")
        int_start = int(start)
        int_end = int(end) + paragraphs_extra_characters
        extended_paragraph_text = paragraph.text
        if paragraphs_extra_characters > 0:
            extended_paragraph_text = await paragraphs.get_paragraph_text(
                kbid=kbid,
                rid=rid,
                field=field_path,
                start=int_start,
                end=int_end,
                extracted_text_cache=etcache,
            )
        if rid not in resources:
            # Get the title and the summary of the resource
            title_text = await paragraphs.get_paragraph_text(
                kbid=kbid,
                rid=rid,
                field="/a/title",
                start=0,
                end=500,
                extracted_text_cache=etcache,
            )
            summary_text = await paragraphs.get_paragraph_text(
                kbid=kbid,
                rid=rid,
                field="/a/summary",
                start=0,
                end=1000,
                extracted_text_cache=etcache,
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
        find_results: KnowledgeboxFindResults,
        resource: Optional[str] = None,
        user_context: Optional[list[str]] = None,
        strategies: Optional[Sequence[RagStrategy]] = None,
        image_strategies: Optional[Sequence[ImageRagStrategy]] = None,
        max_context_characters: Optional[int] = None,
        visual_llm: bool = False,
    ):
        self.kbid = kbid
        self.ordered_paragraphs = get_ordered_paragraphs(find_results)
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
            await default_prompt_context(context, self.kbid, self.ordered_paragraphs)
            return

        full_resource_strategy = False
        number_of_full_resources = len(self.ordered_paragraphs)
        hierarchy_strategy = False
        hierarchy_paragraphs_extended_characters = 0
        extend_with_fields: list[str] = []
        for strategy in self.strategies:
            if strategy.name == RagStrategyName.FIELD_EXTENSION:
                strategy = cast(FieldExtensionStrategy, strategy)
                extend_with_fields.extend(strategy.fields)
            elif strategy.name == RagStrategyName.FULL_RESOURCE:
                strategy = cast(FullResourceStrategy, strategy)
                full_resource_strategy = True
                if self.resource:
                    number_of_full_resources = 1
                elif strategy.count is not None and strategy.count > 0:
                    number_of_full_resources = strategy.count
            elif strategy.name == RagStrategyName.HIERARCHY:
                strategy = cast(HierarchyResourceStrategy, strategy)
                hierarchy_strategy = True
                if strategy.count is not None and strategy.count > 0:
                    hierarchy_paragraphs_extended_characters = strategy.count

        if full_resource_strategy:
            await full_resource_prompt_context(
                context,
                self.kbid,
                self.ordered_paragraphs,
                self.resource,
                number_of_full_resources,
            )
            return

        if hierarchy_strategy:
            await hierarchy_prompt_context(
                context,
                self.kbid,
                self.ordered_paragraphs,
                hierarchy_paragraphs_extended_characters,
            )
            return

        await composed_prompt_context(
            context,
            self.kbid,
            self.ordered_paragraphs,
            extend_with_fields=extend_with_fields,
        )
        return


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
