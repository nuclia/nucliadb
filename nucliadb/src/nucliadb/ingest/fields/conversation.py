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
from typing import Any, Optional

from nucliadb.ingest.fields.base import Field, FieldTypes
from nucliadb_protos.resources_pb2 import (
    CloudFile,
    ExtractedTextWrapper,
    FieldConversation,
)
from nucliadb_protos.resources_pb2 import Conversation as PBConversation
from nucliadb_protos.utils_pb2 import ExtractedText
from nucliadb_utils.storages.storage import StorageField

PAGE_SIZE = 200

CONVERSATION_PAGE_VALUE = "/kbs/{kbid}/r/{uuid}/f/{type}/{field}/{page}"
CONVERSATION_METADATA = "/kbs/{kbid}/r/{uuid}/f/{type}/{field}"


class PageNotFound(Exception):
    pass


class Conversation(Field[PBConversation]):
    pbklass = PBConversation
    type: str = "c"
    value: dict[int, PBConversation]
    metadata: Optional[FieldConversation]

    _created: bool = False

    def __init__(
        self,
        id: str,
        resource: Any,
        pb: Optional[Any] = None,
        value: Optional[dict[int, PBConversation]] = None,
    ):
        super(Conversation, self).__init__(id, resource, pb, value)
        self.value = {}
        self.metadata = None

    async def set_value(self, payload: PBConversation):
        metadata = await self.get_metadata()
        metadata.extract_strategy = payload.extract_strategy
        metadata.split_strategy = payload.split_strategy

        # Get the last page if it exists
        last_page: Optional[PBConversation] = None
        if self._created is False and metadata.pages > 0:
            try:
                last_page = await self.db_get_value(page=metadata.pages)
            except PageNotFound:
                pass
        if last_page is None:
            last_page = PBConversation()
            metadata.pages += 1

        # Make sure message attachment files are on our region. This is needed
        # to support the hybrid-onprem deployment as the attachments must be stored
        # at the storage services of the client's premises.
        for message in payload.messages:
            new_message_files = []
            for idx, file in enumerate(message.content.attachments):
                if self.storage.needs_move(file, self.kbid):
                    if message.ident == "":
                        message_ident = uuid.uuid4().hex
                    else:
                        message_ident = message.ident
                    sf: StorageField = self.storage.conversation_field_attachment(
                        self.kbid, self.uuid, self.id, message_ident, attachment_index=idx
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
            last_page.messages.extend(messages[:available_space])

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

    async def get_value(self, page: Optional[int] = None) -> Optional[PBConversation]:
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

    async def get_full_conversation(self) -> Optional[PBConversation]:
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
            payload = await self.resource.txn.get(
                CONVERSATION_METADATA.format(
                    kbid=self.kbid, uuid=self.uuid, type=self.type, field=self.id
                )
            )
            self.metadata = FieldConversation()
            if payload:
                self.metadata.ParseFromString(payload)
            else:
                self.metadata.size = PAGE_SIZE
                self.metadata.pages = 0
                self.metadata.total = 0
                self._created = True
        return self.metadata

    async def db_get_value(self, page: int = 1):
        if page == 0:
            raise ValueError(f"Conversation pages start at index 1")

        if self.value.get(page) is None:
            field_key = CONVERSATION_PAGE_VALUE.format(
                kbid=self.kbid,
                uuid=self.uuid,
                type=self.type,
                field=self.id,
                page=page,
            )
            payload = await self.resource.txn.get(field_key)
            if payload is None:
                raise PageNotFound()
            self.value[page] = PBConversation()
            self.value[page].ParseFromString(payload)
        return self.value[page]

    async def db_set_value(self, payload: PBConversation, page: int = 0):
        field_key = CONVERSATION_PAGE_VALUE.format(
            kbid=self.kbid, uuid=self.uuid, type=self.type, field=self.id, page=page
        )
        await self.resource.txn.set(
            field_key,
            payload.SerializeToString(),
        )
        self.value[page] = payload
        self.resource.modified = True

    async def db_set_metadata(self, payload: FieldConversation):
        await self.resource.txn.set(
            CONVERSATION_METADATA.format(kbid=self.kbid, uuid=self.uuid, type=self.type, field=self.id),
            payload.SerializeToString(),
        )
        self.metadata = payload
        self.resource.modified = True
        self._created = False

    async def set_extracted_text(self, payload: ExtractedTextWrapper) -> None:
        metadata = await self.get_metadata()
        current_page = metadata.pages
        page_size = metadata.size
        total_messages = metadata.total
        try:
            actual_payload = await self.get_extracted_text(force=True)
        except KeyError:
            # No previous extracted text found
            actual_payload = None

        remaining = len(payload.)

        # Make sure to handle pagination correctly. 
        sf = self.get_storage_field_split(FieldTypes.FIELD_TEXT, page=current_page)
        if actual_payload is None:
            # No previous extracted text, this is the first time we set it so we can simply upload it to storage
            if payload.HasField("file"):
                # Normalize the storage key if the payload is a reference to a file in storage.
                # This is typically the case when the text is too large and we store it in a
                # cloud file. Normalization is needed to ensure that the hybrid-onprem deployment stores
                # the file in the correct bucket of its storage.
                await self.storage.normalize_binary(payload.file, sf)
            else:
                # Directly upload the ExtractedText protobuf to storage
                await self.storage.upload_pb(sf, payload.body)
                self.extracted_text = payload.body
        else:
            if payload.HasField("file"):
                # The extracted text coming from processing has a reference to another storage key.
                # Download it and copy it to its ExtractedText.body field. This is typically for cases
                # when the text is too large.
                raw_payload = await self.storage.downloadbytescf(payload.file)
                pb = ExtractedText()
                pb.ParseFromString(raw_payload.read())
                raw_payload.flush()
                payload.body.CopyFrom(pb)

            # Update or set the extracted text text for each split coming from the processing payload
            for key, value in payload.body.split_text.items():
                actual_payload.split_text[key] = value

            # Apply any split deletions that may come in the processing payload
            for key in payload.body.deleted_splits:
                if key in actual_payload.split_text:
                    del actual_payload.split_text[key]

            # Finally, handle the main text body (for the cases where the text is not split)
            if payload.body.text != "":
                actual_payload.text = payload.body.text

            # Upload the updated ExtractedText to storage
            await self.storage.upload_pb(sf, actual_payload)
            self.extracted_text = actual_payload

    async def get_extracted_text(self, force=False) -> Optional[ExtractedText]:
        if self.extracted_text is None or force:
            async with self.locks["extracted_text"]:
                # Value could have been fetched while waiting for the lock
                if self.extracted_text is None or force:
                    sf = self.get_storage_field(FieldTypes.FIELD_TEXT)
                    payload = await self.storage.download_pb(sf, ExtractedText)
                    if payload is not None:
                        self.extracted_text = payload
        return self.extracted_text
    
    async def get_extracted_texts_of_page(self, page: int) -> Optional[ExtractedText]:
        pass

    async def set_extracted_texts_of_page(self, payload: ExtractedText, page: int) -> None:
        pass