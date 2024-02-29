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

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import TYPE_CHECKING, Any, AsyncIterator, Optional, Type

from nucliadb_protos.resources_pb2 import AllFieldIDs as PBAllFieldIDs
from nucliadb_protos.resources_pb2 import Basic
from nucliadb_protos.resources_pb2 import Basic as PBBasic
from nucliadb_protos.resources_pb2 import CloudFile
from nucliadb_protos.resources_pb2 import Conversation as PBConversation
from nucliadb_protos.resources_pb2 import Extra as PBExtra
from nucliadb_protos.resources_pb2 import (
    ExtractedTextWrapper,
    ExtractedVectorsWrapper,
    FieldClassifications,
    FieldComputedMetadataWrapper,
    FieldID,
    FieldMetadata,
    FieldQuestionAnswerWrapper,
    FieldText,
    FieldType,
    FileExtractedData,
    LargeComputedMetadataWrapper,
    LinkExtractedData,
)
from nucliadb_protos.resources_pb2 import Metadata
from nucliadb_protos.resources_pb2 import Metadata as PBMetadata
from nucliadb_protos.resources_pb2 import Origin as PBOrigin
from nucliadb_protos.resources_pb2 import Paragraph, ParagraphAnnotation
from nucliadb_protos.resources_pb2 import Relations as PBRelations
from nucliadb_protos.resources_pb2 import UserVectorsWrapper
from nucliadb_protos.train_pb2 import EnabledMetadata
from nucliadb_protos.train_pb2 import Position as TrainPosition
from nucliadb_protos.train_pb2 import (
    TrainField,
    TrainMetadata,
    TrainParagraph,
    TrainResource,
    TrainSentence,
)
from nucliadb_protos.utils_pb2 import Relation as PBRelation
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb.common.maindb.driver import Transaction
from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.fields.conversation import Conversation
from nucliadb.ingest.fields.date import Datetime
from nucliadb.ingest.fields.file import File
from nucliadb.ingest.fields.generic import VALID_GENERIC_FIELDS, Generic
from nucliadb.ingest.fields.keywordset import Keywordset
from nucliadb.ingest.fields.layout import Layout
from nucliadb.ingest.fields.link import Link
from nucliadb.ingest.fields.text import Text
from nucliadb.ingest.orm.brain import FilePagePositions, ResourceBrain
from nucliadb.ingest.orm.metrics import processor_observer
from nucliadb.ingest.orm.utils import get_basic, set_basic
from nucliadb_models.common import CloudLink
from nucliadb_models.writer import GENERIC_MIME_TYPE
from nucliadb_protos import utils_pb2, writer_pb2
from nucliadb_utils.storages.storage import Storage

if TYPE_CHECKING:  # pragma: no cover
    from nucliadb.ingest.orm.knowledgebox import KnowledgeBox

logger = logging.getLogger(__name__)

KB_RESOURCE_ORIGIN = "/kbs/{kbid}/r/{uuid}/origin"
KB_RESOURCE_EXTRA = "/kbs/{kbid}/r/{uuid}/extra"
KB_RESOURCE_SECURITY = "/kbs/{kbid}/r/{uuid}/security"
KB_RESOURCE_METADATA = "/kbs/{kbid}/r/{uuid}/metadata"
KB_RESOURCE_RELATIONS = "/kbs/{kbid}/r/{uuid}/relations"
KB_RESOURCE_FIELDS = "/kbs/{kbid}/r/{uuid}/f/"
KB_RESOURCE_ALL_FIELDS = "/kbs/{kbid}/r/{uuid}/allfields"
KB_RESOURCE_SLUG_BASE = "/kbs/{kbid}/s/"
KB_RESOURCE_SLUG = f"{KB_RESOURCE_SLUG_BASE}{{slug}}"
KB_RESOURCE_CONVERSATION = "/kbs/{kbid}/r/{uuid}/c/{page}"
GLOBAL_FIELD = "a"
KB_FIELDS: dict[int, Type] = {
    FieldType.LAYOUT: Layout,
    FieldType.TEXT: Text,
    FieldType.FILE: File,
    FieldType.LINK: Link,
    FieldType.DATETIME: Datetime,
    FieldType.KEYWORDSET: Keywordset,
    FieldType.GENERIC: Generic,
    FieldType.CONVERSATION: Conversation,
}

KB_REVERSE: dict[str, FieldType.ValueType] = {
    "l": FieldType.LAYOUT,
    "t": FieldType.TEXT,
    "f": FieldType.FILE,
    "u": FieldType.LINK,
    "d": FieldType.DATETIME,
    "k": FieldType.KEYWORDSET,
    "a": FieldType.GENERIC,
    "c": FieldType.CONVERSATION,
}

FIELD_TYPE_TO_ID = {v: k for k, v in KB_REVERSE.items()}

_executor = ThreadPoolExecutor(10)


PB_TEXT_FORMAT_TO_MIMETYPE = {
    FieldText.Format.PLAIN: "text/plain",
    FieldText.Format.HTML: "text/html",
    FieldText.Format.RST: "text/x-rst",
    FieldText.Format.MARKDOWN: "text/markdown",
    FieldText.Format.JSON: "application/json",
}

BASIC_IMMUTABLE_FIELDS = ("icon",)


class Resource:
    def __init__(
        self,
        txn: Transaction,
        storage: Storage,
        kb: KnowledgeBox,
        uuid: str,
        basic: Optional[PBBasic] = None,
        disable_vectors: bool = True,
    ):
        self.fields: dict[tuple[FieldType.ValueType, str], Field] = {}
        self.conversations: dict[int, PBConversation] = {}
        self.relations: Optional[PBRelations] = None
        self.all_fields_keys: Optional[list[tuple[FieldType.ValueType, str]]] = None
        self.origin: Optional[PBOrigin] = None
        self.extra: Optional[PBExtra] = None
        self.security: Optional[utils_pb2.Security] = None
        self.modified: bool = False
        self._indexer: Optional[ResourceBrain] = None
        self._modified_extracted_text: list[FieldID] = []

        self.txn = txn
        self.storage = storage
        self.kb = kb
        self.uuid = uuid
        self.basic = basic
        self.disable_vectors = disable_vectors
        self._previous_status: Optional[Metadata.Status.ValueType] = None

    @property
    def indexer(self) -> ResourceBrain:
        if self._indexer is None:
            self._indexer = ResourceBrain(rid=self.uuid)
        return self._indexer

    def replace_indexer(self, indexer: ResourceBrain) -> None:
        self._indexer = indexer

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

    def set_processing_status(self, current_basic: PBBasic, basic_in_payload: PBBasic):
        self._previous_status = current_basic.metadata.status
        if basic_in_payload.HasField("metadata") and basic_in_payload.metadata.useful:
            current_basic.metadata.status = basic_in_payload.metadata.status

    @processor_observer.wrap({"type": "set_basic"})
    async def set_basic(
        self,
        payload: PBBasic,
        deleted_fields: Optional[list[FieldID]] = None,
    ):
        await self.get_basic()

        if self.basic is None:
            self.basic = payload

        elif self.basic != payload:
            for field in BASIC_IMMUTABLE_FIELDS:
                # Immutable basic fields that are already set are cleared
                # from the payload so that they are not overwritten
                if getattr(self.basic, field, "") != "":
                    payload.ClearField(field)  # type: ignore

            self.basic.MergeFrom(payload)

            self.set_processing_status(self.basic, payload)

            # We force the usermetadata classification to be the one defined
            if payload.HasField("usermetadata"):
                self.basic.usermetadata.CopyFrom(payload.usermetadata)

            if len(payload.fieldmetadata):
                # keep only the last fieldmetadata item for each field. API
                # users are responsible to manage fieldmetadata properly
                fields = []
                positions = {}
                for i, fieldmetadata in enumerate(self.basic.fieldmetadata):
                    field_id = self.generate_field_id(fieldmetadata.field)
                    if field_id not in fields:
                        fields.append(field_id)
                    positions[field_id] = i

                updated = [
                    self.basic.fieldmetadata[positions[field]] for field in fields
                ]

                del self.basic.fieldmetadata[:]
                self.basic.fieldmetadata.extend(updated)

                # All modified field metadata should be indexed
                # TODO: could be improved to only index the diff
                for user_field_metadata in self.basic.fieldmetadata:
                    field_id = self.generate_field_id(fieldmetadata.field)
                    field_obj = await self.get_field(
                        fieldmetadata.field.field, fieldmetadata.field.field_type
                    )
                    field_metadata = await field_obj.get_field_metadata()
                    if field_metadata is not None:
                        page_positions: Optional[FilePagePositions] = None
                        if isinstance(field_obj, File):
                            page_positions = await get_file_page_positions(field_obj)

                        self.indexer.apply_field_metadata(
                            field_id,
                            field_metadata,
                            replace_field=[],
                            replace_splits={},
                            page_positions=page_positions,
                            extracted_text=await field_obj.get_extracted_text(),
                            basic_user_field_metadata=user_field_metadata,
                        )

        # Some basic fields are computed off field metadata.
        # This means we need to recompute upon field deletions.
        if deleted_fields is not None and len(deleted_fields) > 0:
            remove_field_classifications(self.basic, deleted_fields=deleted_fields)

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

    # Extra
    async def get_extra(self) -> Optional[PBExtra]:
        if self.extra is None:
            pb = PBExtra()
            payload = await self.txn.get(
                KB_RESOURCE_EXTRA.format(kbid=self.kb.kbid, uuid=self.uuid)
            )
            if payload is None:
                return None
            pb.ParseFromString(payload)
            self.extra = pb
        return self.extra

    async def set_extra(self, payload: PBExtra):
        key = KB_RESOURCE_EXTRA.format(kbid=self.kb.kbid, uuid=self.uuid)
        await self.txn.set(
            key,
            payload.SerializeToString(),
        )
        self.modified = True
        self.extra = payload

    # Security
    async def get_security(self) -> Optional[utils_pb2.Security]:
        if self.security is None:
            pb = utils_pb2.Security()
            key = KB_RESOURCE_SECURITY.format(kbid=self.kb.kbid, uuid=self.uuid)
            payload = await self.txn.get(key)
            if payload is None:
                return None
            pb.ParseFromString(payload)
            self.security = pb
        return self.security

    async def set_security(self, payload: utils_pb2.Security) -> None:
        key = KB_RESOURCE_SECURITY.format(kbid=self.kb.kbid, uuid=self.uuid)
        await self.txn.set(
            key,
            payload.SerializeToString(),
        )
        self.modified = True
        self.security = payload

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

    async def set_relations(self, payload: list[PBRelation]):
        relations = PBRelations()
        for relation in payload:
            relations.relations.append(relation)
        await self.txn.set(
            KB_RESOURCE_RELATIONS.format(kbid=self.kb.kbid, uuid=self.uuid),
            relations.SerializeToString(),
        )
        self.modified = True
        self.relations = relations

    @processor_observer.wrap({"type": "generate_index_message"})
    async def generate_index_message(self) -> ResourceBrain:
        brain = ResourceBrain(rid=self.uuid)
        origin = await self.get_origin()
        basic = await self.get_basic()
        if basic is not None:
            brain.set_resource_metadata(basic, origin)
        await self.compute_security(brain)
        await self.compute_global_tags(brain)
        fields = await self.get_fields(force=True)
        for (type_id, field_id), field in fields.items():
            fieldid = FieldID(field_type=type_id, field=field_id)  # type: ignore
            await self.compute_global_text_field(fieldid, brain)

            field_metadata = await field.get_field_metadata()
            field_key = self.generate_field_id(fieldid)
            if field_metadata is not None:
                page_positions: Optional[FilePagePositions] = None
                if type_id == FieldType.FILE and isinstance(field, File):
                    page_positions = await get_file_page_positions(field)

                user_field_metadata = None
                if basic is not None:
                    user_field_metadata = next(
                        (
                            fm
                            for fm in basic.fieldmetadata
                            if fm.field.field == field_id
                            and fm.field.field_type == type_id
                        ),
                        None,
                    )
                brain.apply_field_metadata(
                    field_key,
                    field_metadata,
                    replace_field=[],
                    replace_splits={},
                    page_positions=page_positions,
                    extracted_text=await field.get_extracted_text(),
                    basic_user_field_metadata=user_field_metadata,
                )

            if self.disable_vectors is False:
                vo = await field.get_vectors()
                if vo is not None:
                    brain.apply_field_vectors(field_key, vo, False, [])

                vu = await field.get_user_vectors()
                if vu is not None:
                    vectors_to_delete = {}  # type: ignore
                    brain.apply_user_vectors(field_key, vu, vectors_to_delete)  # type: ignore
        return brain

    async def generate_field_vectors(
        self,
        bm: BrokerMessage,
        type_id: FieldType.ValueType,
        field_id: str,
        field: Field,
    ):
        vo = await field.get_vectors()
        if vo is None:
            return
        evw = ExtractedVectorsWrapper()
        evw.field.field = field_id
        evw.field.field_type = type_id  # type: ignore
        evw.vectors.CopyFrom(vo)
        bm.field_vectors.append(evw)

    async def generate_user_vectors(
        self,
        bm: BrokerMessage,
        type_id: FieldType.ValueType,
        field_id: str,
        field: Field,
    ):
        uv = await field.get_user_vectors()
        if uv is None:
            return
        uvw = UserVectorsWrapper()
        uvw.field.field = field_id
        uvw.field.field_type = type_id  # type: ignore
        uvw.vectors.CopyFrom(uv)
        bm.user_vectors.append(uvw)

    async def generate_field_large_computed_metadata(
        self,
        bm: BrokerMessage,
        type_id: FieldType.ValueType,
        field_id: str,
        field: Field,
    ):
        lcm = await field.get_large_field_metadata()
        if lcm is None:
            return
        lcmw = LargeComputedMetadataWrapper()
        lcmw.field.field = field_id
        lcmw.field.field_type = type_id  # type: ignore
        lcmw.real.CopyFrom(lcm)
        bm.field_large_metadata.append(lcmw)

    async def generate_field_computed_metadata(
        self,
        bm: BrokerMessage,
        type_id: FieldType.ValueType,
        field_id: str,
        field: Field,
    ):
        fcmw = FieldComputedMetadataWrapper()
        fcmw.field.field = field_id
        fcmw.field.field_type = type_id  # type: ignore

        field_metadata = await field.get_field_metadata()
        if field_metadata is not None:
            fcmw.metadata.CopyFrom(field_metadata)
            fcmw.field.field = field_id
            fcmw.field.field_type = type_id  # type: ignore
            bm.field_metadata.append(fcmw)
            # Make sure cloud files are removed for exporting

    async def generate_extracted_text(
        self,
        bm: BrokerMessage,
        type_id: FieldType.ValueType,
        field_id: str,
        field: Field,
    ):
        etw = ExtractedTextWrapper()
        etw.field.field = field_id
        etw.field.field_type = type_id  # type: ignore
        extracted_text = await field.get_extracted_text()
        if extracted_text is not None:
            etw.body.CopyFrom(extracted_text)
            bm.extracted_text.append(etw)

    async def generate_field(
        self,
        bm: BrokerMessage,
        type_id: FieldType.ValueType,
        field_id: str,
        field: Field,
    ):
        # Used for exporting a field
        if type_id == FieldType.TEXT:
            value = await field.get_value()
            bm.texts[field_id].CopyFrom(value)
        elif type_id == FieldType.LINK:
            value = await field.get_value()
            bm.links[field_id].CopyFrom(value)
        elif type_id == FieldType.FILE:
            value = await field.get_value()
            bm.files[field_id].CopyFrom(value)
        elif type_id == FieldType.CONVERSATION:
            value = await self.get_full_conversation(field)  # type: ignore
            bm.conversations[field_id].CopyFrom(value)
        elif type_id == FieldType.KEYWORDSET:
            value = await field.get_value()
            bm.keywordsets[field_id].CopyFrom(value)
        elif type_id == FieldType.DATETIME:
            value = await field.get_value()
            bm.datetimes[field_id].CopyFrom(value)
        elif type_id == FieldType.LAYOUT:
            value = await field.get_value()
            bm.layouts[field_id].CopyFrom(value)

    async def get_full_conversation(
        self,
        conversation_field: Conversation,
    ) -> Optional[PBConversation]:
        """
        Messages of a conversations may be stored across several pages.
        This method fetches them all and returns a single complete conversation.
        """
        full_conv = PBConversation()
        n_page = 1
        while True:
            page = await conversation_field.get_value(page=n_page)
            if page is None:
                break
            full_conv.messages.extend(page.messages)
            n_page += 1
        return full_conv

    async def generate_broker_message(self) -> BrokerMessage:
        # full means downloading all the pointers
        # minuts the ones to external files that are not PB
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
        for (type_id, field_id), field in fields.items():
            # Value
            await self.generate_field(bm, type_id, field_id, field)

            # Extracted text
            await self.generate_extracted_text(bm, type_id, field_id, field)

            # Field Computed Metadata
            await self.generate_field_computed_metadata(bm, type_id, field_id, field)

            if type_id == FieldType.FILE and isinstance(field, File):
                field_extracted_data = await field.get_file_extracted_data()
                if field_extracted_data is not None:
                    bm.file_extracted_data.append(field_extracted_data)

            elif type_id == FieldType.LINK and isinstance(field, Link):
                link_extracted_data = await field.get_link_extracted_data()
                if link_extracted_data is not None:
                    bm.link_extracted_data.append(link_extracted_data)

            # Field vectors
            await self.generate_field_vectors(bm, type_id, field_id, field)

            # User vectors
            await self.generate_user_vectors(bm, type_id, field_id, field)

            # Large metadata
            await self.generate_field_large_computed_metadata(
                bm, type_id, field_id, field
            )

        return bm

    # Fields
    async def get_fields(
        self, force: bool = False
    ) -> dict[tuple[FieldType.ValueType, str], Field]:
        # Get all fields
        for type, field in await self.get_fields_ids(force=force):
            if (type, field) not in self.fields:
                self.fields[(type, field)] = await self.get_field(field, type)
        return self.fields

    async def _deprecated_scan_fields_ids(
        self,
    ) -> AsyncIterator[tuple[FieldType.ValueType, str]]:
        logger.warning("Scanning fields ids. This is not optimal.")
        prefix = KB_RESOURCE_FIELDS.format(kbid=self.kb.kbid, uuid=self.uuid)
        allfields = set()
        async for key in self.txn.keys(prefix, count=-1):
            # The [6:8] `slicing purpose is to match exactly the two
            # splitted parts corresponding to type and field, and nothing else!
            type, field = key.split("/")[6:8]
            type_id = KB_REVERSE.get(type)
            if type_id is None:
                raise AttributeError("Invalid field type")
            result = (type_id, field)
            if result not in allfields:
                # fields can have errors that are stored in a subkey:
                # - field key       -> kbs/kbid/r/ruuid/f/myfield
                # - field error key -> kbs/kbid/r/ruuid/f/myfield/errors
                # and that would return duplicates here.
                yield result
            allfields.add(result)

    async def _inner_get_fields_ids(self) -> list[tuple[FieldType.ValueType, str]]:
        # Use a set to make sure we don't have duplicate field ids
        result = set()
        all_fields = await self.get_all_field_ids()
        if all_fields is not None:
            for f in all_fields.fields:
                result.add((f.field_type, f.field))
        # We make sure that title and summary are set to be added
        basic = await self.get_basic()
        if basic is not None:
            for generic in VALID_GENERIC_FIELDS:
                append = True
                if generic == "title" and basic.title == "":
                    append = False
                elif generic == "summary" and basic.summary == "":
                    append = False
                if append:
                    result.add((FieldType.GENERIC, generic))
        return list(result)

    async def get_fields_ids(
        self, force: bool = False
    ) -> list[tuple[FieldType.ValueType, str]]:
        """
        Get all ids of the fields of the resource and cache them.
        """
        # Get all fields
        if self.all_fields_keys is None or force is True:
            self.all_fields_keys = await self._inner_get_fields_ids()
        return self.all_fields_keys

    async def get_field(self, key: str, type: FieldType.ValueType, load: bool = True):
        field = (type, key)
        if field not in self.fields:
            field_obj: Field = KB_FIELDS[type](id=key, resource=self)
            if load:
                await field_obj.get_value()
            self.fields[field] = field_obj
        return self.fields[field]

    async def set_field(self, type: FieldType.ValueType, key: str, payload: Any):
        field = (type, key)
        if field not in self.fields:
            field_obj: Field = KB_FIELDS[type](id=key, resource=self)
            self.fields[field] = field_obj
        else:
            field_obj = self.fields[field]
        await field_obj.set_value(payload)
        if self.all_fields_keys is None:
            self.all_fields_keys = []
        self.all_fields_keys.append(field)
        self.modified = True
        return field_obj

    async def delete_field(self, type: FieldType.ValueType, key: str):
        field = (type, key)
        if field in self.fields:
            field_obj = self.fields[field]
            del self.fields[field]
        else:
            field_obj = KB_FIELDS[type](id=key, resource=self)

        if self.all_fields_keys is not None:
            if field in self.all_fields_keys:
                self.all_fields_keys.remove(field)

        field_key = self.generate_field_id(FieldID(field_type=type, field=key))  # type: ignore
        vo = await field_obj.get_vectors()
        if vo is not None:
            self.indexer.delete_vectors(field_key=field_key, vo=vo)

        metadata = await field_obj.get_field_metadata()
        if metadata is not None:
            self.indexer.delete_metadata(field_key=field_key, metadata=metadata)

        await field_obj.delete()

    def has_field(self, type: FieldType.ValueType, field: str) -> bool:
        return (type, field) in self.fields

    async def get_all_field_ids(self) -> Optional[PBAllFieldIDs]:
        key = KB_RESOURCE_ALL_FIELDS.format(kbid=self.kb.kbid, uuid=self.uuid)
        payload = await self.txn.get(key)
        if payload is None:
            return None
        all_fields = PBAllFieldIDs()
        all_fields.ParseFromString(payload)
        return all_fields

    async def set_all_field_ids(self, all_fields: PBAllFieldIDs):
        key = KB_RESOURCE_ALL_FIELDS.format(kbid=self.kb.kbid, uuid=self.uuid)
        await self.txn.set(key, all_fields.SerializeToString())

    async def update_all_field_ids(
        self,
        *,
        updated: Optional[list[FieldID]] = None,
        deleted: Optional[list[FieldID]] = None,
        errors: Optional[list[writer_pb2.Error]] = None,
    ):
        needs_update = False
        all_fields = await self.get_all_field_ids()
        if all_fields is None:
            needs_update = True
            all_fields = PBAllFieldIDs()

        for field in updated or []:
            if field not in all_fields.fields:
                all_fields.fields.append(field)
                needs_update = True

        for error in errors or []:
            field_id = FieldID(field_type=error.field_type, field=error.field)
            if field_id not in all_fields.fields:
                all_fields.fields.append(field_id)
                needs_update = True

        for field in deleted or []:
            if field in all_fields.fields:
                all_fields.fields.remove(field)
                needs_update = True

        if needs_update:
            await self.set_all_field_ids(all_fields)

    @processor_observer.wrap({"type": "apply_fields"})
    async def apply_fields(self, message: BrokerMessage):
        message_updated_fields = []
        for field, layout in message.layouts.items():
            fid = FieldID(field_type=FieldType.LAYOUT, field=field)
            await self.set_field(fid.field_type, fid.field, layout)
            message_updated_fields.append(fid)

        for field, text in message.texts.items():
            fid = FieldID(field_type=FieldType.TEXT, field=field)
            await self.set_field(fid.field_type, fid.field, text)
            message_updated_fields.append(fid)

        for field, keywordset in message.keywordsets.items():
            fid = FieldID(field_type=FieldType.KEYWORDSET, field=field)
            await self.set_field(fid.field_type, fid.field, keywordset)
            message_updated_fields.append(fid)

        for field, datetimeobj in message.datetimes.items():
            fid = FieldID(field_type=FieldType.DATETIME, field=field)
            await self.set_field(fid.field_type, fid.field, datetimeobj)
            message_updated_fields.append(fid)

        for field, link in message.links.items():
            fid = FieldID(field_type=FieldType.LINK, field=field)
            await self.set_field(fid.field_type, fid.field, link)
            message_updated_fields.append(fid)

        for field, file in message.files.items():
            fid = FieldID(field_type=FieldType.FILE, field=field)
            await self.set_field(fid.field_type, fid.field, file)
            message_updated_fields.append(fid)

        for field, conversation in message.conversations.items():
            fid = FieldID(field_type=FieldType.CONVERSATION, field=field)
            await self.set_field(fid.field_type, fid.field, conversation)
            message_updated_fields.append(fid)

        for fieldid in message.delete_fields:
            await self.delete_field(fieldid.field_type, fieldid.field)

        if (
            len(message_updated_fields)
            or len(message.delete_fields)
            or len(message.errors)
        ):
            await self.update_all_field_ids(
                updated=message_updated_fields, deleted=message.delete_fields, errors=message.errors  # type: ignore
            )

    @processor_observer.wrap({"type": "apply_extracted"})
    async def apply_extracted(self, message: BrokerMessage):
        errors = False
        field_obj: Field
        for error in message.errors:
            field_obj = await self.get_field(error.field, error.field_type, load=False)
            await field_obj.set_error(error)
            errors = True

        await self.get_basic()
        if self.basic is None:
            raise KeyError("Resource Not Found")

        previous_basic = Basic()
        previous_basic.CopyFrom(self.basic)

        if errors:
            self.basic.metadata.status = PBMetadata.Status.ERROR
        elif errors is False and message.source is message.MessageSource.PROCESSOR:
            self.basic.metadata.status = PBMetadata.Status.PROCESSED

        maybe_update_basic_icon(self.basic, get_text_field_mimetype(message))

        for question_answers in message.question_answers:
            await self._apply_question_answers(question_answers)

        for extracted_text in message.extracted_text:
            await self._apply_extracted_text(extracted_text)

        extracted_languages = []

        for link_extracted_data in message.link_extracted_data:
            await self._apply_link_extracted_data(link_extracted_data)
            await self.maybe_update_title_metadata(link_extracted_data)
            extracted_languages.append(link_extracted_data.language)

        for file_extracted_data in message.file_extracted_data:
            await self._apply_file_extracted_data(file_extracted_data)
            extracted_languages.append(file_extracted_data.language)

        # Metadata should go first
        for field_metadata in message.field_metadata:
            await self._apply_field_computed_metadata(field_metadata)
            extracted_languages.extend(extract_field_metadata_languages(field_metadata))

        update_basic_languages(self.basic, extracted_languages)

        # Upload to binary storage
        # Vector indexing
        if self.disable_vectors is False:
            for field_vectors in message.field_vectors:
                await self._apply_extracted_vectors(field_vectors)
            for user_vectors in message.user_vectors:
                await self._apply_user_vectors(user_vectors)

        # Only uploading to binary storage
        for field_large_metadata in message.field_large_metadata:
            await self._apply_field_large_metadata(field_large_metadata)

        for relation in message.relations:
            self.indexer.brain.relations.append(relation)
        await self.set_relations(message.relations)  # type: ignore

        # Basic proto may have been modified in some apply functions but we only
        # want to set it once
        if self.basic != previous_basic:
            await self.set_basic(self.basic)

    async def _apply_extracted_text(self, extracted_text: ExtractedTextWrapper):
        field_obj = await self.get_field(
            extracted_text.field.field, extracted_text.field.field_type, load=False
        )
        await field_obj.set_extracted_text(extracted_text)
        self._modified_extracted_text.append(
            extracted_text.field,
        )

    async def _apply_question_answers(
        self, question_answers: FieldQuestionAnswerWrapper
    ):
        field = question_answers.field
        field_obj = await self.get_field(field.field, field.field_type, load=False)
        await field_obj.set_question_answers(question_answers)

    async def _apply_link_extracted_data(self, link_extracted_data: LinkExtractedData):
        assert self.basic is not None
        field_link: Link = await self.get_field(
            link_extracted_data.field,
            FieldType.LINK,
            load=False,
        )
        maybe_update_basic_thumbnail(self.basic, link_extracted_data.link_thumbnail)

        await field_link.set_link_extracted_data(link_extracted_data)

        maybe_update_basic_icon(self.basic, "application/stf-link")

        maybe_update_basic_summary(self.basic, link_extracted_data.description)

    async def maybe_update_title_metadata(self, link_extracted_data: LinkExtractedData):
        assert self.basic is not None
        if not link_extracted_data.title:
            return
        if not (self.basic.title.startswith("http") or self.basic.title == ""):
            return

        title = link_extracted_data.title
        self.basic.title = title
        # Extracted text
        field = await self.get_field("title", FieldType.GENERIC, load=False)
        etw = ExtractedTextWrapper()
        etw.body.text = title
        await field.set_extracted_text(etw)

        # Field computed metadata
        fcmw = FieldComputedMetadataWrapper()
        fcmw.field.field = "title"
        fcmw.field.field_type = FieldType.GENERIC

        # Merge with any existing field computed metadata
        fcm = await field.get_field_metadata(force=True)
        if fcm is not None:
            fcmw.metadata.CopyFrom(fcm)

        fcmw.metadata.metadata.ClearField("paragraphs")
        paragraph = Paragraph(
            start=0, end=len(title), kind=Paragraph.TypeParagraph.TITLE
        )
        fcmw.metadata.metadata.paragraphs.append(paragraph)

        await field.set_field_metadata(fcmw)

    async def _apply_file_extracted_data(self, file_extracted_data: FileExtractedData):
        assert self.basic is not None
        field_file: File = await self.get_field(
            file_extracted_data.field,
            FieldType.FILE,
            load=False,
        )
        # uri can change after extraction
        await field_file.set_file_extracted_data(file_extracted_data)
        maybe_update_basic_icon(self.basic, file_extracted_data.icon)
        maybe_update_basic_thumbnail(self.basic, file_extracted_data.file_thumbnail)

    async def _apply_field_computed_metadata(
        self, field_metadata: FieldComputedMetadataWrapper
    ):
        assert self.basic is not None
        maybe_update_basic_summary(self.basic, field_metadata.metadata.metadata.summary)

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

        page_positions: Optional[FilePagePositions] = None
        if field_metadata.field.field_type == FieldType.FILE and isinstance(
            field_obj, File
        ):
            page_positions = await get_file_page_positions(field_obj)

        user_field_metadata = next(
            (
                fm
                for fm in self.basic.fieldmetadata
                if fm.field.field == field_metadata.field.field
                and fm.field.field_type == field_metadata.field.field_type
            ),
            None,
        )

        extracted_text = await field_obj.get_extracted_text()
        apply_field_metadata = partial(
            self.indexer.apply_field_metadata,
            field_key,
            metadata,
            replace_field=replace_field,
            replace_splits=replace_splits,
            page_positions=page_positions,
            extracted_text=extracted_text,
            basic_user_field_metadata=user_field_metadata,
        )
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(_executor, apply_field_metadata)

        maybe_update_basic_thumbnail(
            self.basic, field_metadata.metadata.metadata.thumbnail
        )

        add_field_classifications(self.basic, field_metadata)

    async def _apply_extracted_vectors(self, field_vectors: ExtractedVectorsWrapper):
        if not self.has_field(
            field_vectors.field.field_type, field_vectors.field.field
        ):
            # skipping because field does not exist
            logger.warning(
                f'Field "{field_vectors.field.field}" does not exist, skipping vectors'
            )
            return

        field_obj = await self.get_field(
            field_vectors.field.field,
            field_vectors.field.field_type,
            load=False,
        )
        (
            vo,
            replace_field_sentences,
            replace_splits_sentences,
        ) = await field_obj.set_vectors(field_vectors)
        field_key = self.generate_field_id(field_vectors.field)
        if vo is not None:
            apply_field_vectors = partial(
                self.indexer.apply_field_vectors,
                field_key,
                vo,
                replace_field_sentences,
                replace_splits_sentences,
            )
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(_executor, apply_field_vectors)
        else:
            raise AttributeError("VO not found on set")

    async def _apply_user_vectors(self, user_vectors: UserVectorsWrapper):
        field_obj = await self.get_field(
            user_vectors.field.field,
            user_vectors.field.field_type,
            load=False,
        )
        uv, vectors_to_delete = await field_obj.set_user_vectors(user_vectors)
        field_key = self.generate_field_id(user_vectors.field)
        if uv is not None:
            # We need to make sure that the vectors replaced are not on the new vectors
            # So we extend the vectors to delete with the one replaced by the update
            for vectorset, vectors in vectors_to_delete.items():
                for vector in vectors.vectors:
                    if vector not in user_vectors.vectors_to_delete[vectorset].vectors:
                        user_vectors.vectors_to_delete[vectorset].vectors.append(vector)
            self.indexer.apply_user_vectors(
                field_key, uv, user_vectors.vectors_to_delete
            )
        else:
            raise AttributeError("User Vectors not found on set")

    async def _apply_field_large_metadata(
        self, field_large_metadata: LargeComputedMetadataWrapper
    ):
        field_obj = await self.get_field(
            field_large_metadata.field.field,
            field_large_metadata.field.field_type,
            load=False,
        )
        await field_obj.set_large_field_metadata(field_large_metadata)

    def generate_field_id(self, field: FieldID) -> str:
        return f"{FIELD_TYPE_TO_ID[field.field_type]}/{field.field}"

    async def compute_security(self, brain: ResourceBrain):
        security = await self.get_security()
        if security is None:
            return
        brain.set_security(security)

    @processor_observer.wrap({"type": "compute_global_tags"})
    async def compute_global_tags(self, brain: ResourceBrain):
        origin = await self.get_origin()
        basic = await self.get_basic()
        if basic is None:
            raise KeyError("Resource not found")

        brain.set_processing_status(basic=basic, previous_status=self._previous_status)
        brain.set_resource_metadata(basic=basic, origin=origin)
        for type, field in await self.get_fields_ids(force=True):
            fieldobj = await self.get_field(field, type, load=False)
            fieldid = FieldID(field_type=type, field=field)  # type: ignore
            fieldkey = self.generate_field_id(fieldid)
            extracted_metadata = await fieldobj.get_field_metadata()
            valid_user_field_metadata = None
            for user_field_metadata in basic.fieldmetadata:
                if (
                    user_field_metadata.field.field == field
                    and user_field_metadata.field.field_type == type
                ):
                    valid_user_field_metadata = user_field_metadata
                    break
            brain.apply_field_labels(
                fieldkey,
                extracted_metadata,
                self.uuid,
                basic.usermetadata,
                valid_user_field_metadata,
            )
            if type == FieldType.KEYWORDSET:
                field_data = await fieldobj.db_get_value()
                brain.process_keywordset_fields(fieldkey, field_data)

    @processor_observer.wrap({"type": "compute_global_text"})
    async def compute_global_text(self):
        for type, field in await self.get_fields_ids(force=True):
            fieldid = FieldID(field_type=type, field=field)
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

    def clean(self):
        self._indexer = None
        self.txn = None

    async def iterate_sentences(
        self, enabled_metadata: EnabledMetadata
    ) -> AsyncIterator[TrainSentence]:  # pragma: no cover
        fields = await self.get_fields(force=True)
        metadata = TrainMetadata()
        userdefinedparagraphclass: dict[str, ParagraphAnnotation] = {}
        if enabled_metadata.labels:
            if self.basic is None:
                self.basic = await self.get_basic()
            if self.basic is not None:
                metadata.labels.resource.extend(self.basic.usermetadata.classifications)
                for fieldmetadata in self.basic.fieldmetadata:
                    field_id = self.generate_field_id(fieldmetadata.field)
                    for annotationparagraph in fieldmetadata.paragraphs:
                        userdefinedparagraphclass[
                            annotationparagraph.key
                        ] = annotationparagraph

        for (type_id, field_id), field in fields.items():
            fieldid = FieldID(field_type=type_id, field=field_id)  # type: ignore
            field_key = self.generate_field_id(fieldid)
            fm = await field.get_field_metadata()
            extracted_text = None
            vo = None
            text = None

            if enabled_metadata.vector:
                vo = await field.get_vectors()

            extracted_text = await field.get_extracted_text()

            if fm is None:
                continue

            field_metadatas: list[tuple[Optional[str], FieldMetadata]] = [
                (None, fm.metadata)
            ]
            for subfield_metadata, splitted_metadata in fm.split_metadata.items():
                field_metadatas.append((subfield_metadata, splitted_metadata))

            for subfield, field_metadata in field_metadatas:
                if enabled_metadata.labels:
                    metadata.labels.ClearField("field")
                    metadata.labels.field.extend(field_metadata.classifications)

                entities: dict[str, str] = {}
                if enabled_metadata.entities:
                    entities.update(field_metadata.ner)

                precomputed_vectors = {}
                if vo is not None:
                    if subfield is not None:
                        vectors = vo.split_vectors[subfield]
                        base_vector_key = f"{self.uuid}/{field_key}/{subfield}"
                    else:
                        vectors = vo.vectors
                        base_vector_key = f"{self.uuid}/{field_key}"
                    for index, vector in enumerate(vectors.vectors):
                        vector_key = (
                            f"{base_vector_key}/{index}/{vector.start}-{vector.end}"
                        )
                        precomputed_vectors[vector_key] = vector.vector

                if extracted_text is not None:
                    if subfield is not None:
                        text = extracted_text.split_text[subfield]
                    else:
                        text = extracted_text.text

                for paragraph in field_metadata.paragraphs:
                    if subfield is not None:
                        paragraph_key = f"{self.uuid}/{field_key}/{subfield}/{paragraph.start}-{paragraph.end}"
                    else:
                        paragraph_key = (
                            f"{self.uuid}/{field_key}/{paragraph.start}-{paragraph.end}"
                        )

                    if enabled_metadata.labels:
                        metadata.labels.ClearField("field")
                        metadata.labels.paragraph.extend(paragraph.classifications)
                        if paragraph_key in userdefinedparagraphclass:
                            metadata.labels.paragraph.extend(
                                userdefinedparagraphclass[paragraph_key].classifications
                            )

                    for index, sentence in enumerate(paragraph.sentences):
                        if subfield is not None:
                            sentence_key = f"{self.uuid}/{field_key}/{subfield}/{index}/{sentence.start}-{sentence.end}"
                        else:
                            sentence_key = f"{self.uuid}/{field_key}/{index}/{sentence.start}-{sentence.end}"

                        if vo is not None:
                            metadata.ClearField("vector")
                            vector_tmp = precomputed_vectors.get(sentence_key)
                            if vector_tmp:
                                metadata.vector.extend(vector_tmp)

                        if extracted_text is not None and text is not None:
                            metadata.text = text[sentence.start : sentence.end]

                        metadata.ClearField("entities")
                        metadata.ClearField("entity_positions")
                        if enabled_metadata.entities and text is not None:
                            local_text = text[sentence.start : sentence.end]
                            add_entities_to_metadata(entities, local_text, metadata)

                        pb_sentence = TrainSentence()
                        pb_sentence.uuid = self.uuid
                        pb_sentence.field.CopyFrom(fieldid)
                        pb_sentence.paragraph = paragraph_key
                        pb_sentence.sentence = sentence_key
                        pb_sentence.metadata.CopyFrom(metadata)
                        yield pb_sentence

    async def iterate_paragraphs(
        self, enabled_metadata: EnabledMetadata
    ) -> AsyncIterator[TrainParagraph]:
        fields = await self.get_fields(force=True)
        metadata = TrainMetadata()
        userdefinedparagraphclass: dict[str, ParagraphAnnotation] = {}
        if enabled_metadata.labels:
            if self.basic is None:
                self.basic = await self.get_basic()
            if self.basic is not None:
                metadata.labels.resource.extend(self.basic.usermetadata.classifications)
                for fieldmetadata in self.basic.fieldmetadata:
                    field_id = self.generate_field_id(fieldmetadata.field)
                    for annotationparagraph in fieldmetadata.paragraphs:
                        userdefinedparagraphclass[
                            annotationparagraph.key
                        ] = annotationparagraph

        for (type_id, field_id), field in fields.items():
            fieldid = FieldID(field_type=type_id, field=field_id)  # type: ignore
            field_key = self.generate_field_id(fieldid)
            fm = await field.get_field_metadata()
            extracted_text = None
            text = None

            extracted_text = await field.get_extracted_text()

            if fm is None:
                continue

            field_metadatas: list[tuple[Optional[str], FieldMetadata]] = [
                (None, fm.metadata)
            ]
            for subfield_metadata, splitted_metadata in fm.split_metadata.items():
                field_metadatas.append((subfield_metadata, splitted_metadata))

            for subfield, field_metadata in field_metadatas:
                if enabled_metadata.labels:
                    metadata.labels.ClearField("field")
                    metadata.labels.field.extend(field_metadata.classifications)

                entities: dict[str, str] = {}
                if enabled_metadata.entities:
                    entities.update(field_metadata.ner)

                if extracted_text is not None:
                    if subfield is not None:
                        text = extracted_text.split_text[subfield]
                    else:
                        text = extracted_text.text

                for paragraph in field_metadata.paragraphs:
                    if subfield is not None:
                        paragraph_key = f"{self.uuid}/{field_key}/{subfield}/{paragraph.start}-{paragraph.end}"
                    else:
                        paragraph_key = (
                            f"{self.uuid}/{field_key}/{paragraph.start}-{paragraph.end}"
                        )

                    if enabled_metadata.labels:
                        metadata.labels.ClearField("paragraph")
                        metadata.labels.paragraph.extend(paragraph.classifications)

                        if extracted_text is not None and text is not None:
                            metadata.text = text[paragraph.start : paragraph.end]

                        metadata.ClearField("entities")
                        metadata.ClearField("entity_positions")
                        if enabled_metadata.entities and text is not None:
                            local_text = text[paragraph.start : paragraph.end]
                            add_entities_to_metadata(entities, local_text, metadata)

                        if paragraph_key in userdefinedparagraphclass:
                            metadata.labels.paragraph.extend(
                                userdefinedparagraphclass[paragraph_key].classifications
                            )

                        pb_paragraph = TrainParagraph()
                        pb_paragraph.uuid = self.uuid
                        pb_paragraph.field.CopyFrom(fieldid)
                        pb_paragraph.paragraph = paragraph_key
                        pb_paragraph.metadata.CopyFrom(metadata)

                        yield pb_paragraph

    async def iterate_fields(
        self, enabled_metadata: EnabledMetadata
    ) -> AsyncIterator[TrainField]:
        fields = await self.get_fields(force=True)
        metadata = TrainMetadata()
        if enabled_metadata.labels:
            if self.basic is None:
                self.basic = await self.get_basic()
            if self.basic is not None:
                metadata.labels.resource.extend(self.basic.usermetadata.classifications)

        for (type_id, field_id), field in fields.items():
            fieldid = FieldID(field_type=type_id, field=field_id)  # type: ignore
            fm = await field.get_field_metadata()
            extracted_text = None

            if enabled_metadata.text:
                extracted_text = await field.get_extracted_text()

            if fm is None:
                continue

            field_metadatas: list[tuple[Optional[str], FieldMetadata]] = [
                (None, fm.metadata)
            ]
            for subfield_metadata, splitted_metadata in fm.split_metadata.items():
                field_metadatas.append((subfield_metadata, splitted_metadata))

            for subfield, splitted_metadata in field_metadatas:
                if enabled_metadata.labels:
                    metadata.labels.ClearField("field")
                    metadata.labels.field.extend(splitted_metadata.classifications)

                if extracted_text is not None:
                    if subfield is not None:
                        metadata.text = extracted_text.split_text[subfield]
                    else:
                        metadata.text = extracted_text.text

                if enabled_metadata.entities:
                    metadata.ClearField("entities")
                    metadata.entities.update(splitted_metadata.ner)

                pb_field = TrainField()
                pb_field.uuid = self.uuid
                pb_field.field.CopyFrom(fieldid)
                pb_field.metadata.CopyFrom(metadata)
                yield pb_field

    async def generate_train_resource(
        self, enabled_metadata: EnabledMetadata
    ) -> TrainResource:
        fields = await self.get_fields(force=True)
        metadata = TrainMetadata()
        if enabled_metadata.labels:
            if self.basic is None:
                self.basic = await self.get_basic()
            if self.basic is not None:
                metadata.labels.resource.extend(self.basic.usermetadata.classifications)

        metadata.labels.ClearField("field")
        metadata.ClearField("entities")

        for (_, _), field in fields.items():
            extracted_text = None
            fm = await field.get_field_metadata()

            if enabled_metadata.text:
                extracted_text = await field.get_extracted_text()

            if extracted_text is not None:
                metadata.text += extracted_text.text
                for text in extracted_text.split_text.values():
                    metadata.text += f" {text}"

            if fm is None:
                continue

            field_metadatas: list[tuple[Optional[str], FieldMetadata]] = [
                (None, fm.metadata)
            ]
            for subfield_metadata, splitted_metadata in fm.split_metadata.items():
                field_metadatas.append((subfield_metadata, splitted_metadata))

            for _, splitted_metadata in field_metadatas:
                if enabled_metadata.labels:
                    metadata.labels.field.extend(splitted_metadata.classifications)

                if enabled_metadata.entities:
                    metadata.entities.update(splitted_metadata.ner)

        pb_resource = TrainResource()
        pb_resource.uuid = self.uuid
        if self.basic is not None:
            pb_resource.title = self.basic.title
            pb_resource.icon = self.basic.icon
            pb_resource.slug = self.basic.slug
            pb_resource.modified.CopyFrom(self.basic.modified)
            pb_resource.created.CopyFrom(self.basic.created)
        pb_resource.metadata.CopyFrom(metadata)
        return pb_resource


async def get_file_page_positions(field: File) -> FilePagePositions:
    positions: FilePagePositions = {}
    file_extracted_data = await field.get_file_extracted_data()
    if file_extracted_data is None:
        return positions
    for index, position in enumerate(file_extracted_data.file_pages_previews.positions):
        positions[index] = (position.start, position.end)
    return positions


def remove_field_classifications(basic: PBBasic, deleted_fields: list[FieldID]):
    """
    Clean classifications of fields that have been deleted
    """
    field_classifications = [
        fc
        for fc in basic.computedmetadata.field_classifications
        if fc.field not in deleted_fields
    ]
    basic.computedmetadata.ClearField("field_classifications")
    basic.computedmetadata.field_classifications.extend(field_classifications)


def add_field_classifications(
    basic: PBBasic, fcmw: FieldComputedMetadataWrapper
) -> bool:
    """
    Returns whether some new field classifications were added
    """
    if len(fcmw.metadata.metadata.classifications) == 0:
        return False
    remove_field_classifications(basic, [fcmw.field])
    fcfs = FieldClassifications()
    fcfs.field.CopyFrom(fcmw.field)
    fcfs.classifications.extend(fcmw.metadata.metadata.classifications)
    basic.computedmetadata.field_classifications.append(fcfs)
    return True


def add_entities_to_metadata(
    entities: dict[str, str], local_text: str, metadata: TrainMetadata
) -> None:
    for entity_key, entity_value in entities.items():
        if entity_key not in local_text:
            # Add the entity only if found in text
            continue
        metadata.entities[entity_key] = entity_value

        # Add positions for the entity relative to the local text
        poskey = f"{entity_value}/{entity_key}"
        metadata.entity_positions[poskey].entity = entity_key
        last_occurrence_end = 0
        for _ in range(local_text.count(entity_key)):
            start = local_text.index(entity_key, last_occurrence_end)
            end = start + len(entity_key)
            metadata.entity_positions[poskey].positions.append(
                TrainPosition(start=start, end=end)
            )
            last_occurrence_end = end


def maybe_update_basic_summary(basic: PBBasic, summary_text: str) -> bool:
    if basic.summary or not summary_text:
        return False
    basic.summary = summary_text
    return True


def maybe_update_basic_icon(basic: PBBasic, mimetype: Optional[str]) -> bool:
    if basic.icon not in (None, "", "application/octet-stream", GENERIC_MIME_TYPE):
        # Icon already set or detected
        return False
    if not mimetype:
        return False
    basic.icon = mimetype
    return True


def maybe_update_basic_thumbnail(
    basic: PBBasic, thumbnail: Optional[CloudFile]
) -> bool:
    if basic.thumbnail or thumbnail is None:
        return False
    basic.thumbnail = CloudLink.format_reader_download_uri(thumbnail.uri)
    return True


def update_basic_languages(basic: Basic, languages: list[str]) -> bool:
    if len(languages) == 0:
        return False

    updated = False
    for language in languages:
        if not language:
            continue

        if basic.metadata.language == "":
            basic.metadata.language = language
            updated = True

        if language not in basic.metadata.languages:
            basic.metadata.languages.append(language)
            updated = True

    return updated


def get_text_field_mimetype(bm: BrokerMessage) -> Optional[str]:
    if len(bm.texts) == 0:
        return None
    text_format = next(iter(bm.texts.values())).format
    return PB_TEXT_FORMAT_TO_MIMETYPE[text_format]


def extract_field_metadata_languages(
    field_metadata: FieldComputedMetadataWrapper,
) -> list[str]:
    languages: set[str] = set()
    languages.add(field_metadata.metadata.metadata.language)
    for _, splitted_metadata in field_metadata.metadata.split_metadata.items():
        languages.add(splitted_metadata.language)
    return list(languages)
