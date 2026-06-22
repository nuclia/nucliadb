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
import uuid
from collections import defaultdict
from typing import Any, Iterable

from nucliadb.common import datamanagers
from nucliadb.ingest import logger
from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.fields.exceptions import FieldAuthorNotFound
from nucliadb_protos.resources_pb2 import CloudFile, FieldAuthor, FieldConversation, SplitsMetadata
from nucliadb_protos.resources_pb2 import Conversation as PBConversation
from nucliadb_utils.storages.storage import StorageField

MAX_CONVERSATION_MESSAGES = None  # No limit

PAGE_SIZE = 200


class PageNotFound(Exception):
    pass


class Conversation(Field[PBConversation]):
    pbklass = PBConversation
    type: str = "c"
    value: dict[int, PBConversation]
    metadata: FieldConversation | None

    _created: bool = False

    def __init__(self, id: str, resource: Any):
        super().__init__(id, resource)
        self.value = {}
        self._splits_metadata: SplitsMetadata | None = None
        self.metadata = None

    async def delete_value(self):
        await datamanagers.conversations.delete_field(
            self.resource.txn,
            kbid=self.kbid,
            rid=self.rid,
            field_type=self.type,
            field_id=self.id,
        )
        self._splits_metadata = None
        self.metadata = None
        self.value.clear()

    async def set_value(self, payload: PBConversation):
        if payload.replace_field:
            # As we need to overwrite the value of the conversation, first delete any previous data.
            await self.delete_value()

        metadata = await self.get_metadata()
        metadata.extract_strategy = payload.extract_strategy
        metadata.split_strategy = payload.split_strategy
        metadata.generated_by.CopyFrom(payload.generated_by)

        # Get the last page if it exists
        last_page: PBConversation | None = None
        if self._created is False and metadata.pages > 0:
            try:
                last_page = await self.db_get_value(page=metadata.pages)
            except PageNotFound:
                pass
        if last_page is None:
            last_page = PBConversation()
            metadata.pages += 1

        self._splits_metadata = await self.get_splits_metadata()

        # Make sure message attachment files are on our region. This is needed
        # to support the hybrid-onprem deployment as the attachments must be stored
        # at the storage services of the client's premises.
        for message in payload.messages:
            self._splits_metadata.metadata.get_or_create(message.ident)
            new_message_files = []
            for idx, file in enumerate(message.content.attachments):
                if self.storage.needs_move(file, self.kbid):
                    if message.ident == "":
                        message_ident = uuid.uuid4().hex
                    else:
                        message_ident = message.ident
                    sf: StorageField = self.storage.conversation_field_attachment(
                        self.kbid, self.rid, self.id, message_ident, attachment_index=idx
                    )
                    cf: CloudFile = await self.storage.normalize_binary(file, sf)
                    new_message_files.append(cf)
                else:
                    new_message_files.append(file)

            # Replace the attachments in the message with the new ones
            message.content.ClearField("attachments")
            for message_file in new_message_files:
                message.content.attachments.append(message_file)

        # Increment the metadata total with the number of messages
        messages = list(payload.messages)
        metadata.total += len(messages)

        # Store the messages in pages of PAGE_SIZE messages
        while True:
            # Fit the messages in the last page
            available_space = metadata.size - len(last_page.messages)
            page_messages = messages[:available_space]

            # Add the page to the split metadata, so we can track in which page each message is for efficient deletion later.
            for message in page_messages:
                split_meta = self._splits_metadata.metadata.get_or_create(message.ident)
                split_meta.page = metadata.pages

            last_page.messages.extend(page_messages)

            # Save the last page
            await self.db_set_value(last_page, metadata.pages)

            # If there are still messages, create a new page
            messages = messages[available_space:]
            if len(messages) > 0:
                metadata.pages += 1
                last_page = PBConversation()
            else:
                break

        # Finally, set the metadata
        await self.db_set_metadata(metadata)
        await self.set_splits_metadata(self._splits_metadata)

    async def get_value(self, page: int | None = None) -> PBConversation | None:
        # If no page was requested, force fetch of metadata
        # and set the page to the last page
        if page is None and self.metadata is None:
            await self.get_metadata()

        try:
            if page is not None and page > 0:
                pageobj = await self.db_get_value(page)
            else:
                pageobj = None
            return pageobj
        except PageNotFound:
            return None

    async def get_full_conversation(self) -> PBConversation:
        """
        Messages of a conversations may be stored across several pages.
        This method fetches them all and returns a single complete conversation.
        """
        full_conv = PBConversation()
        n_page = 1
        while True:
            page = await self.get_value(page=n_page)
            if page is None:
                break
            full_conv.messages.extend(page.messages)
            n_page += 1
        return full_conv

    async def get_metadata(self) -> FieldConversation:
        if self.metadata is None:
            self.metadata = await datamanagers.conversations.get_metadata(
                self.resource.txn,
                kbid=self.kbid,
                rid=self.rid,
                field_type=self.type,
                field_id=self.id,
            )
            if self.metadata is None:
                self.metadata = FieldConversation()
                self.metadata.size = PAGE_SIZE
                self.metadata.pages = 0
                self.metadata.total = 0
                self._created = True
        return self.metadata

    async def db_get_value(self, page: int = 1):
        if page == 0:
            raise ValueError("Conversation pages start at index 1")

        if self.value.get(page) is None:
            pb = await datamanagers.conversations.get_page(
                self.resource.txn,
                kbid=self.kbid,
                rid=self.rid,
                field_type=self.type,
                field_id=self.id,
                page=page,
            )
            if pb is None:
                raise PageNotFound()
            self.value[page] = pb
        return self.value[page]

    async def db_set_value(self, payload: PBConversation, page: int = 0):
        await datamanagers.conversations.set_page(
            self.resource.txn,
            kbid=self.kbid,
            rid=self.rid,
            field_type=self.type,
            field_id=self.id,
            page=page,
            value=payload,
        )
        self.value[page] = payload
        self.resource.modified = True

    async def db_set_metadata(self, payload: FieldConversation):
        await datamanagers.conversations.set_metadata(
            self.resource.txn,
            kbid=self.kbid,
            rid=self.rid,
            field_type=self.type,
            field_id=self.id,
            metadata=payload,
        )
        self.metadata = payload
        self.resource.modified = True
        self._created = False

    async def get_splits_metadata(self) -> SplitsMetadata:
        if self._splits_metadata is None:
            pb = await datamanagers.conversations.get_splits_metadata(
                self.resource.txn,
                kbid=self.kbid,
                rid=self.rid,
                field_type=self.type,
                field_id=self.id,
            )
            if pb is None:
                return SplitsMetadata()
            self._splits_metadata = pb
        return self._splits_metadata

    async def set_splits_metadata(self, payload: SplitsMetadata) -> None:
        await datamanagers.conversations.set_splits_metadata(
            self.resource.txn,
            kbid=self.kbid,
            rid=self.rid,
            field_type=self.type,
            field_id=self.id,
            splits_metadata=payload,
        )
        self._splits_metadata = payload
        self.resource.modified = True

    async def delete_messages(self, message_idents: Iterable[str]) -> int:
        """Delete messages by their ident from conversation pages.

        Messages are removed in-place from their pages without restructuring
        pages, so that page numbering stays stable.

        Returns the number of messages actually deleted.

        Processed metadata for the deleted messages (e.g: text, vectors, etc) are not deleted for now.
        """
        total_deleted = 0

        splits_metadata = await self.get_splits_metadata()
        idents_to_delete = set(message_idents) - set(splits_metadata.deleted_splits)
        idents_to_delete = idents_to_delete & set(splits_metadata.metadata.keys())

        page_idents: dict[int, set[str]] = defaultdict(set)
        for ident in idents_to_delete:
            split_meta = splits_metadata.metadata.get(ident)
            if split_meta is not None:
                page_idents[split_meta.page].add(ident)

        for page, idents in page_idents.items():
            some_deleted_in_page = False
            page_obj = await self.db_get_value(page)
            for message in page_obj.messages:
                if message.ident in idents and message.ident not in splits_metadata.deleted_splits:
                    message.content.text = ""
                    total_deleted += 1
                    some_deleted_in_page = True
                    splits_metadata.deleted_splits.append(message.ident)
                    idents_to_delete.remove(message.ident)
            if some_deleted_in_page:
                # If we deleted a message in this page, we need to update it in the database.
                await self.db_set_value(page_obj, page)

        if len(idents_to_delete) > 0:
            logger.warning(
                "Some message idents were not found in the conversation and could not be deleted",
                extra={
                    "kbid": self.kbid,
                    "rid": self.rid,
                    "idents": list(idents_to_delete),
                },
            )

        if total_deleted > 0:
            await self.set_splits_metadata(splits_metadata)

        return total_deleted

    async def generated_by(self) -> FieldAuthor:
        value = await self.get_metadata()
        if value is None:
            raise FieldAuthorNotFound("Field has no value, can't know who generated it")
        return value.generated_by
