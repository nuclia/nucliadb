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
from typing import Optional

from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.fields.conversation import Conversation
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb.ingest.orm.resource import KB_REVERSE
from nucliadb_models.search import SCORE_TYPE, KnowledgeboxFindResults
from nucliadb_protos import resources_pb2
from nucliadb_utils.utilities import get_storage

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


async def get_chat_prompt_context(
    kbid: str, results: KnowledgeboxFindResults
) -> list[str]:
    ordered_paras = []
    for result in results.resources.values():
        for field_path, field in result.fields.items():
            for paragraph in field.paragraphs.values():
                ordered_paras.append((field_path, paragraph))

    ordered_paras.sort(key=lambda x: x[1].order, reverse=False)

    driver = get_driver()
    storage = await get_storage()
    # ordered dict that prevents duplicates pulled in through conversation expansion
    output = {}
    async with driver.transaction() as txn:
        kb = KnowledgeBoxORM(txn, storage, kbid)
        for field_path, paragraph in ordered_paras:
            text = paragraph.text.strip()
            # Do not send highlight marks on prompt context
            text = text.replace("<mark>", "").replace("</mark>", "")
            output[paragraph.id] = text

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

    return list(output.values())
