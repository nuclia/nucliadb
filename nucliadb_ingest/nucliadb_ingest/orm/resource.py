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
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Type

from nucliadb_protos.resources_pb2 import Basic as PBBasic
from nucliadb_protos.resources_pb2 import Conversation as PBConversation
from nucliadb_protos.resources_pb2 import (
    ExtractedTextWrapper,
    ExtractedVectorsWrapper,
    FieldComputedMetadataWrapper,
    FieldID,
    FieldType,
)
from nucliadb_protos.resources_pb2 import Metadata as PBMetadata
from nucliadb_protos.resources_pb2 import Origin as PBOrigin
from nucliadb_protos.resources_pb2 import Relations as PBRelations
from nucliadb_protos.utils_pb2 import Relation as PBRelation
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb_ingest.fields.base import Field
from nucliadb_ingest.fields.conversation import Conversation
from nucliadb_ingest.fields.date import Datetime
from nucliadb_ingest.fields.file import File
from nucliadb_ingest.fields.generic import VALID_GLOBAL, Generic
from nucliadb_ingest.fields.keywordset import Keywordset
from nucliadb_ingest.fields.layout import Layout
from nucliadb_ingest.fields.link import Link
from nucliadb_ingest.fields.text import Text
from nucliadb_ingest.maindb.driver import Transaction
from nucliadb_ingest.orm.brain import ResourceBrain
from nucliadb_ingest.orm.utils import get_basic, set_basic
from nucliadb_models.common import CloudLink
from nucliadb_utils.storages.storage import Storage

if TYPE_CHECKING:
    from nucliadb_ingest.orm.knowledgebox import KnowledgeBox

KB_RESOURCE_ORIGIN = "/kbs/{kbid}/r/{uuid}/origin"
KB_RESOURCE_METADATA = "/kbs/{kbid}/r/{uuid}/metadata"
KB_RESOURCE_RELATIONS = "/kbs/{kbid}/r/{uuid}/relations"
KB_RESOURCE_FIELD = "/kbs/{kbid}/r/{uuid}/f/{type}/{field}"
KB_RESOURCE_FIELDS = "/kbs/{kbid}/r/{uuid}/f/"
KB_RESOURCE_SLUG_BASE = "/kbs/{kbid}/s/"
KB_RESOURCE_SLUG = f"{KB_RESOURCE_SLUG_BASE}{{slug}}"
KB_RESOURCE_CONVERSATION = "/kbs/{kbid}/r/{uuid}/c/{page}"
GLOBAL_FIELD = "a"
KB_FIELDS: Dict[int, Type] = {
    FieldType.LAYOUT: Layout,
    FieldType.TEXT: Text,
    FieldType.FILE: File,
    FieldType.LINK: Link,
    FieldType.DATETIME: Datetime,
    FieldType.KEYWORDSET: Keywordset,
    FieldType.GENERIC: Generic,
    FieldType.CONVERSATION: Conversation,
}

KB_REVERSE: Dict[str, int] = {
    "l": FieldType.LAYOUT,
    "t": FieldType.TEXT,
    "f": FieldType.FILE,
    "u": FieldType.LINK,
    "d": FieldType.DATETIME,
    "k": FieldType.KEYWORDSET,
    "a": FieldType.GENERIC,
    "c": FieldType.CONVERSATION,
}

KB_REVERSE_REVERSE = {v: k for k, v in KB_REVERSE.items()}


class Resource:
    def __init__(
        self,
        txn: Transaction,
        storage: Storage,
        kb: KnowledgeBox,
        uuid: str,
        basic: PBBasic = None,
        disable_vectors: bool = True,
    ):
        self.fields: Dict[Tuple[int, str], Field] = {}
        self.conversations: Dict[int, PBConversation] = {}
        self.relations: Optional[PBRelations] = None
        self.all_fields_keys: List[Tuple[int, str]] = []
        self.origin: Optional[PBOrigin] = None
        self.modified: bool = False
        self._indexer: Optional[ResourceBrain] = None
        self._modified_extracted_text: List[FieldID] = []

        self.txn = txn
        self.storage = storage
        self.kb = kb
        self.uuid = uuid
        self.basic = basic
        self.disable_vectors = disable_vectors

    @property
    def indexer(self) -> ResourceBrain:
        if self._indexer is None:
            self._indexer = ResourceBrain(rid=self.uuid)
        return self._indexer

    async def set_slug(self):
        basic = await self.get_basic()
        new_key = KB_RESOURCE_SLUG.format(kbid=self.kb.kbid, slug=basic.slug)
        await self.txn.set(new_key, self.uuid.encode())

    @staticmethod
    def parse_basic(payload: bytes) -> PBBasic:
        pb = PBBasic()
        if payload is None:
            return None

        pb.ParseFromString(payload)
        return pb

    async def exists(self) -> bool:
        exists = True
        if self.basic is None:
            payload = await get_basic(self.txn, self.kb.kbid, self.uuid)
            if payload is not None:
                pb = PBBasic()
                pb.ParseFromString(payload)
                self.basic = pb
            else:
                exists = False
        return exists

    # Basic
    async def get_basic(self) -> Optional[PBBasic]:
        if self.basic is None:
            payload = await get_basic(self.txn, self.kb.kbid, self.uuid)
            self.basic = self.parse_basic(payload) if payload is not None else PBBasic()
        return self.basic

    async def set_basic(self, payload: PBBasic, slug: Optional[str] = None):
        await self.get_basic()
        if self.basic is not None and self.basic != payload:
            self.basic.MergeFrom(payload)
        else:
            self.basic = payload
        if slug is not None and slug != "":
            slug = await self.kb.get_unique_slug(self.uuid, slug)
            self.basic.slug = slug
        await set_basic(self.txn, self.kb.kbid, self.uuid, self.basic)
        self.modified = True

    # Origin
    async def get_origin(self) -> Optional[PBOrigin]:
        if self.origin is None:
            pb = PBOrigin()
            payload = await self.txn.get(
                KB_RESOURCE_ORIGIN.format(kbid=self.kb.kbid, uuid=self.uuid)
            )
            if payload is None:
                return None
            pb.ParseFromString(payload)
            self.origin = pb
        return self.origin

    async def set_origin(self, payload: PBOrigin):
        await self.txn.set(
            KB_RESOURCE_ORIGIN.format(kbid=self.kb.kbid, uuid=self.uuid),
            payload.SerializeToString(),
        )
        self.modified = True
        self.origin = payload

    # Relations
    async def get_relations(self) -> Optional[PBRelations]:
        if self.relations is None:
            pb = PBRelations()
            payload = await self.txn.get(
                KB_RESOURCE_RELATIONS.format(kbid=self.kb.kbid, uuid=self.uuid)
            )
            if payload is None:
                return None
            pb.ParseFromString(payload)
            self.relations = pb
        return self.relations

    async def set_relations(self, payload: List[PBRelation]):
        relations = PBRelations()
        for relation in payload:
            relations.relations.append(relation)
        await self.txn.set(
            KB_RESOURCE_RELATIONS.format(kbid=self.kb.kbid, uuid=self.uuid),
            relations.SerializeToString(),
        )
        self.modified = True
        self.relations = relations

    async def generate_index_message(self) -> ResourceBrain:
        brain = ResourceBrain(rid=self.uuid)
        origin = await self.get_origin()
        basic = await self.get_basic()
        if basic is not None:
            brain.set_global_tags(basic, self.uuid, origin)
        fields = await self.get_fields(force=True)
        for ((type_id, field_id), field) in fields.items():
            fieldid = FieldID(field_type=type_id, field=field_id)  # type: ignore
            await self.compute_global_text_field(fieldid, brain)

            field_metadata = await field.get_field_metadata()
            field_key = self.generate_field_id(fieldid)
            if field_metadata is not None:
                brain.apply_field_metadata(field_key, field_metadata, [], {})

            if self.disable_vectors is False:
                vo = await field.get_vectors()
                if vo is not None:
                    brain.apply_field_vectors(field_key, vo, [], {})
        return brain

    async def generate_broker_message(self) -> BrokerMessage:
        # Go for all fields and recreate brain
        bm = BrokerMessage()
        bm.kbid = self.kb.kbid
        bm.uuid = self.uuid
        basic = await self.get_basic()
        if basic is not None:
            bm.basic.CopyFrom(basic)
        bm.slug = bm.basic.slug
        origin = await self.get_origin()
        if origin is not None:
            bm.origin.CopyFrom(origin)
        relations = await self.get_relations()
        if relations is not None:
            for relation in relations.relations:
                bm.relations.append(relation)

        fields = await self.get_fields(force=True)
        for ((type_id, field_id), field) in fields.items():
            extracted_text_cf = await field.get_extracted_text_cf()
            if extracted_text_cf is not None:
                etw = ExtractedTextWrapper()
                etw.file.CopyFrom(extracted_text_cf)
                bm.extracted_text.append(etw)

            field_metadata = await field.get_field_metadata()
            if field_metadata is not None:
                fcmw = FieldComputedMetadataWrapper()
                fcmw.metadata.CopyFrom(field_metadata)
                fcmw.field.field = field_id
                fcmw.field.field_type = type_id  # type: ignore
                bm.extracted_text.append(etw)

            field_vectors = await field.get_vectors_cf()
            if field_vectors is not None:
                evw = ExtractedVectorsWrapper()
                evw.file.CopyFrom(field_vectors)
                evw.field.field = field_id
                evw.field.field_type = type_id  # type: ignore
                bm.extracted_text.append(etw)

            if type_id == FieldType.FILE and isinstance(field, File):
                field_extracted_data = await field.get_file_extracted_data()
                if field_extracted_data is not None:
                    bm.file_extracted_data.append(field_extracted_data)

            elif type_id == FieldType.LINK and isinstance(field, Link):
                link_extracted_data = await field.get_link_extracted_data()
                if link_extracted_data is not None:
                    bm.link_extracted_data.append(link_extracted_data)
        return bm

    # Fields
    async def get_fields(self, force: bool = False) -> Dict[Tuple[int, str], Field]:
        # Get all fields
        for type, field in await self.get_fields_ids(force=force):
            if (type, field) not in self.fields:
                self.fields[(type, field)] = await self.get_field(field, type)
        return self.fields

    async def get_fields_ids(self, force: bool = False) -> List[Tuple[int, str]]:
        # Get all fields
        if len(self.all_fields_keys) == 0 or force:
            result = []
            fields = KB_RESOURCE_FIELDS.format(kbid=self.kb.kbid, uuid=self.uuid)
            async for key in self.txn.keys(fields, count=-1):
                # The [6:8] `slicing purpose is to match exactly the two
                # splitted parts corresponding to type and field, and nothing else!
                type, field = key.split("/")[6:8]
                type_id = KB_REVERSE.get(type)
                if type_id is None:
                    raise AttributeError("Invalid field type")
                result.append((type_id, field))

            for generic in VALID_GLOBAL:
                result.append((FieldType.GENERIC, generic))

            self.all_fields_keys = result
        return self.all_fields_keys

    async def get_field(self, key: str, type: int, load: bool = True):
        field = (type, key)
        if field not in self.fields:
            field_obj: Field = KB_FIELDS[type](id=key, resource=self)
            if load:
                await field_obj.get_value()
            self.fields[field] = field_obj
        return self.fields[field]

    async def set_field(self, type: int, key: str, payload: Any):
        field = (type, key)
        if field not in self.fields:
            field_obj: Field = KB_FIELDS[type](id=key, resource=self)
            self.fields[field] = field_obj
        else:
            field_obj = self.fields[field]
        await field_obj.set_value(payload)
        self.all_fields_keys.append(field)

        self.modified = True
        return field_obj

    async def delete_field(self, type: int, key: str):
        field = (type, key)
        if field in self.fields:
            field_obj = self.fields[field]
            del self.fields[field]
        else:
            field_obj = KB_FIELDS[type](id=key, resource=self)

        field_key = self.generate_field_id(FieldID(field_type=type, field=key))  # type: ignore
        vo = await field_obj.get_vectors()
        if vo is not None:
            self.indexer.delete_vectors(field_key=field_key, vo=vo)

        metadata = await field_obj.get_field_metadata()
        if metadata is not None:
            self.indexer.delete_metadata(field_key=field_key, metadata=metadata)

        await field_obj.delete()

    async def apply_fields(self, message: BrokerMessage):
        for field, layout in message.layouts.items():
            await self.set_field(FieldType.LAYOUT, field, layout)

        for field, text in message.texts.items():
            await self.set_field(FieldType.TEXT, field, text)

        for field, keywordset in message.keywordsets.items():
            await self.set_field(FieldType.KEYWORDSET, field, keywordset)

        for field, datetimeobj in message.datetimes.items():
            await self.set_field(FieldType.DATETIME, field, datetimeobj)

        for field, link in message.links.items():
            await self.set_field(FieldType.LINK, field, link)

        for field, file in message.files.items():
            await self.set_field(FieldType.FILE, field, file)

        for field, conversation in message.conversations.items():
            await self.set_field(FieldType.CONVERSATION, field, conversation)

        for fieldid in message.delete_fields:
            await self.delete_field(fieldid.field_type, fieldid.field)

    async def apply_extracted(self, message: BrokerMessage):
        errors = False
        field_obj: Field
        basic_modified = False
        for error in message.errors:
            field_obj = await self.get_field(error.field, error.field_type, load=False)
            await field_obj.set_error(error)
            errors = True

        await self.get_basic()
        if self.basic is None:
            raise KeyError("Resource Not Found")

        if errors:
            self.basic.metadata.status = PBMetadata.Status.ERROR
            basic_modified = True
        elif errors is False and message.source is message.MessageSource.PROCESSOR:
            self.basic.metadata.status = PBMetadata.Status.PROCESSED
            basic_modified = True

        for extracted_text in message.extracted_text:
            field_obj = await self.get_field(
                extracted_text.field.field, extracted_text.field.field_type, load=False
            )
            await field_obj.set_extracted_text(extracted_text)
            self._modified_extracted_text.append(
                extracted_text.field,
            )

        for link_extracted_data in message.link_extracted_data:
            field_link: Link = await self.get_field(
                link_extracted_data.field,
                FieldType.LINK,
                load=False,
            )
            if link_extracted_data.link_thumbnail and self.basic.thumbnail == "":
                self.basic.thumbnail = CloudLink.format_reader_download_uri(
                    link_extracted_data.link_thumbnail.uri
                )
                basic_modified = True
            await field_link.set_link_extracted_data(link_extracted_data)

            if self.basic.icon == "":
                self.basic.icon = "application/stf-link"
                basic_modified = True
            if (
                self.basic.title.startswith("http") and link_extracted_data.title != ""
            ) or (self.basic.title == "" and link_extracted_data.title != ""):
                # If the title was http something or empty replace
                self.basic.title = link_extracted_data.title
                basic_modified = True
            if self.basic.summary == "" and link_extracted_data.description != "":
                self.basic.summary = link_extracted_data.description
                basic_modified = True

        for file_extracted_data in message.file_extracted_data:
            field_file: File = await self.get_field(
                file_extracted_data.field,
                FieldType.FILE,
                load=False,
            )
            if file_extracted_data.icon != "" and self.basic.icon == "":
                self.basic.icon = file_extracted_data.icon
                basic_modified = True
            if file_extracted_data.file_thumbnail and self.basic.thumbnail == "":
                self.basic.thumbnail = CloudLink.format_reader_download_uri(
                    file_extracted_data.file_thumbnail.uri
                )
                basic_modified = True
            await field_file.set_file_extracted_data(file_extracted_data)

        # Metadata should go first
        for field_metadata in message.field_metadata:
            field_obj = await self.get_field(
                field_metadata.field.field,
                field_metadata.field.field_type,
                load=False,
            )
            (
                metadata,
                replace_field,
                replace_splits,
            ) = await field_obj.set_field_metadata(field_metadata)
            field_key = self.generate_field_id(field_metadata.field)
            self.indexer.apply_field_metadata(
                field_key, metadata, replace_field, replace_splits
            )

            if (
                field_metadata.metadata.metadata.thumbnail
                and self.basic.thumbnail == ""
            ):
                self.basic.thumbnail = CloudLink.format_reader_download_uri(
                    field_metadata.metadata.metadata.thumbnail.uri
                )
                basic_modified = True

        # Upload to binary storage
        # Vector indexing
        if self.disable_vectors is False:
            for field_vectors in message.field_vectors:
                field_obj = await self.get_field(
                    field_vectors.field.field,
                    field_vectors.field.field_type,
                    load=False,
                )
                vo, replace_field, replace_splits = await field_obj.set_vectors(
                    field_vectors
                )
                field_key = self.generate_field_id(field_vectors.field)
                if vo is not None:
                    self.indexer.apply_field_vectors(
                        field_key, vo, replace_field, replace_splits
                    )
                else:
                    raise AttributeError("VO not found on set")

        # Only uploading to binary storage
        for field_large_metadata in message.field_large_metadata:
            field_obj = await self.get_field(
                field_large_metadata.field.field,
                field_large_metadata.field.field_type,
                load=False,
            )
            await field_obj.set_large_field_metadata(field_large_metadata)

        for relation in message.relations:
            self.indexer.brain.relations.append(relation)
        await self.set_relations(message.relations)  # type: ignore

        if basic_modified:
            await self.set_basic(self.basic)

    def generate_field_id(self, field: FieldID) -> str:
        return f"{KB_REVERSE_REVERSE[field.field_type]}/{field.field}"

    async def compute_global_tags(self, brain: ResourceBrain):
        origin = await self.get_origin()
        basic = await self.get_basic()
        if basic is None:
            raise KeyError("Resource not found")
        brain.set_global_tags(basic=basic, origin=origin, uuid=self.uuid)
        for type, field in await self.get_fields_ids():
            fieldobj = await self.get_field(field, type, load=False)
            fieldid = FieldID(field_type=type, field=field)  # type: ignore
            fieldkey = self.generate_field_id(fieldid)
            extracted_metadata = await fieldobj.get_field_metadata()
            if extracted_metadata is not None:
                brain.apply_field_tags_globally(fieldkey, extracted_metadata, self.uuid)
            if type == FieldType.KEYWORDSET:
                field_data = await fieldobj.db_get_value()
                brain.process_keywordset_fields(fieldkey, field_data)

    async def compute_global_text(self):
        # For each extracted
        for fieldid in self._modified_extracted_text:
            await self.compute_global_text_field(fieldid, self.indexer)

    async def compute_global_text_field(self, fieldid: FieldID, brain: ResourceBrain):
        fieldobj = await self.get_field(fieldid.field, fieldid.field_type, load=False)
        fieldkey = self.generate_field_id(fieldid)
        extracted_text = await fieldobj.get_extracted_text()
        if extracted_text is None:
            return
        field_text = extracted_text.text
        for _, split in extracted_text.split_text.items():
            field_text += f" {split} "
        brain.apply_field_text(fieldkey, field_text)

    async def get_all(self):
        if self.basic is None:
            self.basic = await self.get_basic()

        if self.origin is None:
            self.origin = await self.get_origin()

        if self.relations is None:
            self.relations = await self.get_relations()

    def clean(self):
        self._indexer = None
