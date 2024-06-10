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

from nucliadb.ingest.fields.base import Field
from nucliadb_protos.resources_pb2 import CloudFile, FieldConversation
from nucliadb_protos.resources_pb2 import Conversation as PBConversation
from nucliadb_utils.storages.storage import StorageField

PAGE_SIZE = 200
KB_RESOURCE_FIELD = "/kbs/{kbid}/r/{uuid}/f/{type}/{field}/{page}"
KB_RESOURCE_FIELD_METADATA = "/kbs/{kbid}/r/{uuid}/f/{type}/{field}"


class PageNotFound(Exception):
    pass


class Conversation(Field):
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
        last_page: Optional[PBConversation] = None
        metadata = await self.get_metadata()
        if self._created is False and metadata.pages > 0:
            try:
                last_page = await self.db_get_value(page=metadata.pages)
            except PageNotFound:
                pass

        # Make sure message attachment files are on our region
        for message in payload.messages:
            new_message_files = []
            for count, file in enumerate(message.content.attachments):
                if self.storage.needs_move(file, self.kbid):
                    if message.ident == "":
                        message_ident = uuid.uuid4().hex
                    else:
                        message_ident = message.ident
                    sf: StorageField = self.storage.conversation_field(
                        self.kbid, self.uuid, self.id, message_ident, count
                    )
                    cf: CloudFile = await self.storage.normalize_binary(file, sf)
                    new_message_files.append(cf)
                else:
                    new_message_files.append(file)

            # Can be cleaned a list of PB
            message.content.ClearField("attachments")
            for message_file in new_message_files:
                message.content.attachments.append(message_file)

        if last_page is None:
            last_page = PBConversation()
            metadata.pages += 1

        # Merge on last page
        messages = list(payload.messages)
        metadata.total += len(messages)
        while len(messages) > 0:
            count = metadata.size - len(last_page.messages)
            last_page.messages.extend(messages[:count])
            await self.db_set_value(last_page, metadata.pages)

            messages = messages[count:]
            if len(messages) > 0:
                metadata.pages += 1
                last_page = PBConversation()

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
                KB_RESOURCE_FIELD_METADATA.format(
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
            field_key = KB_RESOURCE_FIELD.format(
                kbid=self.kbid,
                uuid=self.uuid,
                type=self.type,
                field=self.id,
                page=page,
            )
            payload = await self.resource.txn.get(field_key)
            if payload:
                self.value[page] = PBConversation()
                self.value[page].ParseFromString(payload)
            else:
                raise PageNotFound()
        return self.value[page]

    async def db_set_value(self, payload: PBConversation, page: int = 0):
        field_key = KB_RESOURCE_FIELD.format(
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
            KB_RESOURCE_FIELD_METADATA.format(
                kbid=self.kbid, uuid=self.uuid, type=self.type, field=self.id
            ),
            payload.SerializeToString(),
        )
        self.metadata = payload
        self.resource.modified = True
        self._created = False
