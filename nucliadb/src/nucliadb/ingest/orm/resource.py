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
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import TYPE_CHECKING, Any, Optional, Sequence, Type

from nucliadb.common import datamanagers
from nucliadb.common.datamanagers.resources import KB_RESOURCE_SLUG
from nucliadb.common.ids import FIELD_TYPE_PB_TO_STR, FieldId
from nucliadb.common.maindb.driver import Transaction
from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.fields.conversation import Conversation
from nucliadb.ingest.fields.file import File
from nucliadb.ingest.fields.generic import VALID_GENERIC_FIELDS, Generic
from nucliadb.ingest.fields.link import Link
from nucliadb.ingest.fields.text import Text
from nucliadb.ingest.orm.brain import FilePagePositions, ResourceBrain
from nucliadb.ingest.orm.metrics import processor_observer
from nucliadb_models import content_types
from nucliadb_models.common import CloudLink
from nucliadb_models.content_types import GENERIC_MIME_TYPE
from nucliadb_protos import utils_pb2, writer_pb2
from nucliadb_protos.resources_pb2 import AllFieldIDs as PBAllFieldIDs
from nucliadb_protos.resources_pb2 import (
    Basic,
    CloudFile,
    ExtractedTextWrapper,
    ExtractedVectorsWrapper,
    FieldClassifications,
    FieldComputedMetadataWrapper,
    FieldFile,
    FieldID,
    FieldQuestionAnswerWrapper,
    FieldText,
    FieldType,
    FileExtractedData,
    LargeComputedMetadataWrapper,
    LinkExtractedData,
    Metadata,
    Paragraph,
)
from nucliadb_protos.resources_pb2 import Basic as PBBasic
from nucliadb_protos.resources_pb2 import Conversation as PBConversation
from nucliadb_protos.resources_pb2 import Extra as PBExtra
from nucliadb_protos.resources_pb2 import Metadata as PBMetadata
from nucliadb_protos.resources_pb2 import Origin as PBOrigin
from nucliadb_protos.resources_pb2 import Relations as PBRelations
from nucliadb_protos.utils_pb2 import Relation as PBRelation
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_utils.storages.storage import Storage

if TYPE_CHECKING:  # pragma: no cover
    from nucliadb.ingest.orm.knowledgebox import KnowledgeBox

logger = logging.getLogger(__name__)

KB_FIELDS: dict[int, Type] = {
    FieldType.TEXT: Text,
    FieldType.FILE: File,
    FieldType.LINK: Link,
    FieldType.GENERIC: Generic,
    FieldType.CONVERSATION: Conversation,
}

_executor = ThreadPoolExecutor(10)


PB_TEXT_FORMAT_TO_MIMETYPE = {
    FieldText.Format.PLAIN: "text/plain",
    FieldText.Format.HTML: "text/html",
    FieldText.Format.RST: "text/x-rst",
    FieldText.Format.MARKDOWN: "text/markdown",
    FieldText.Format.JSON: "application/json",
    FieldText.Format.KEEP_MARKDOWN: "text/markdown",
    FieldText.Format.JSONL: "application/x-ndjson",
    FieldText.Format.PLAIN_BLANKLINE_SPLIT: "text/plain+blankline",
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

    # Basic
    async def get_basic(self) -> Optional[PBBasic]:
        if self.basic is None:
            basic = await datamanagers.resources.get_basic(self.txn, kbid=self.kb.kbid, rid=self.uuid)
            self.basic = basic if basic is not None else PBBasic()
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

                updated = [self.basic.fieldmetadata[positions[field]] for field in fields]

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
                            page_positions=page_positions,
                            extracted_text=await field_obj.get_extracted_text(),
                            basic_user_field_metadata=user_field_metadata,
                            replace_field=True,
                        )

        # Some basic fields are computed off field metadata.
        # This means we need to recompute upon field deletions.
        if deleted_fields is not None and len(deleted_fields) > 0:
            remove_field_classifications(self.basic, deleted_fields=deleted_fields)

        await datamanagers.resources.set_basic(
            self.txn, kbid=self.kb.kbid, rid=self.uuid, basic=self.basic
        )
        self.modified = True

    # Origin
    async def get_origin(self) -> Optional[PBOrigin]:
        if self.origin is None:
            origin = await datamanagers.resources.get_origin(self.txn, kbid=self.kb.kbid, rid=self.uuid)
            self.origin = origin
        return self.origin

    async def set_origin(self, payload: PBOrigin):
        await datamanagers.resources.set_origin(
            self.txn, kbid=self.kb.kbid, rid=self.uuid, origin=payload
        )
        self.modified = True
        self.origin = payload

    # Extra
    async def get_extra(self) -> Optional[PBExtra]:
        if self.extra is None:
            extra = await datamanagers.resources.get_extra(self.txn, kbid=self.kb.kbid, rid=self.uuid)
            self.extra = extra
        return self.extra

    async def set_extra(self, payload: PBExtra):
        await datamanagers.resources.set_extra(self.txn, kbid=self.kb.kbid, rid=self.uuid, extra=payload)
        self.modified = True
        self.extra = payload

    # Security
    async def get_security(self) -> Optional[utils_pb2.Security]:
        if self.security is None:
            security = await datamanagers.resources.get_security(
                self.txn, kbid=self.kb.kbid, rid=self.uuid
            )
            self.security = security
        return self.security

    async def set_security(self, payload: utils_pb2.Security) -> None:
        await datamanagers.resources.set_security(
            self.txn, kbid=self.kb.kbid, rid=self.uuid, security=payload
        )
        self.modified = True
        self.security = payload

    # Relations
    async def get_relations(self) -> Optional[PBRelations]:
        if self.relations is None:
            relations = await datamanagers.resources.get_relations(
                self.txn, kbid=self.kb.kbid, rid=self.uuid
            )
            self.relations = relations
        return self.relations

    async def set_relations(self, payload: list[PBRelation]):
        relations = PBRelations()
        for relation in payload:
            relations.relations.append(relation)
        await datamanagers.resources.set_relations(
            self.txn, kbid=self.kb.kbid, rid=self.uuid, relations=relations
        )
        self.modified = True
        self.relations = relations

    @processor_observer.wrap({"type": "generate_index_message"})
    async def generate_index_message(self, reindex: bool = False) -> ResourceBrain:
        brain = ResourceBrain(rid=self.uuid)
        origin = await self.get_origin()
        basic = await self.get_basic()
        if basic is not None:
            brain.set_resource_metadata(basic, origin)
        await self.compute_security(brain)
        await self.compute_global_tags(brain)
        fields = await self.get_fields(force=True)
        for (type_id, field_id), field in fields.items():
            fieldid = FieldID(field_type=type_id, field=field_id)
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
                            if fm.field.field == field_id and fm.field.field_type == type_id
                        ),
                        None,
                    )
                brain.apply_field_metadata(
                    field_key,
                    field_metadata,
                    page_positions=page_positions,
                    extracted_text=await field.get_extracted_text(),
                    basic_user_field_metadata=user_field_metadata,
                    replace_field=reindex,
                )

            if self.disable_vectors is False:
                vectorset_configs = []
                async for vectorset_id, vectorset_config in datamanagers.vectorsets.iter(
                    self.txn, kbid=self.kb.kbid
                ):
                    vectorset_configs.append(vectorset_config)

                for vectorset_config in vectorset_configs:
                    vo = await field.get_vectors(
                        vectorset=vectorset_config.vectorset_id,
                        storage_key_kind=vectorset_config.storage_key_kind,
                    )
                    if vo is not None:
                        dimension = vectorset_config.vectorset_index_config.vector_dimension
                        brain.apply_field_vectors(
                            field_key,
                            vo,
                            vectorset=vectorset_config.vectorset_id,
                            vector_dimension=dimension,
                            replace_field=reindex,
                        )
        return brain

    # Fields
    async def get_fields(self, force: bool = False) -> dict[tuple[FieldType.ValueType, str], Field]:
        # Get all fields
        for type, field in await self.get_fields_ids(force=force):
            if (type, field) not in self.fields:
                self.fields[(type, field)] = await self.get_field(field, type)
        return self.fields

    async def _inner_get_fields_ids(self) -> list[tuple[FieldType.ValueType, str]]:
        # Use a set to make sure we don't have duplicate field ids
        result = set()
        all_fields = await self.get_all_field_ids(for_update=False)
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

    async def get_fields_ids(self, force: bool = False) -> list[tuple[FieldType.ValueType, str]]:
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

        field_key = self.generate_field_id(FieldID(field_type=type, field=key))

        metadata = await field_obj.get_field_metadata()
        if metadata is not None:
            self.indexer.delete_field(field_key=field_key)

        await field_obj.delete()

    def has_field(self, type: FieldType.ValueType, field: str) -> bool:
        return (type, field) in self.fields

    async def get_all_field_ids(self, *, for_update: bool) -> Optional[PBAllFieldIDs]:
        return await datamanagers.resources.get_all_field_ids(
            self.txn, kbid=self.kb.kbid, rid=self.uuid, for_update=for_update
        )

    async def set_all_field_ids(self, all_fields: PBAllFieldIDs):
        return await datamanagers.resources.set_all_field_ids(
            self.txn, kbid=self.kb.kbid, rid=self.uuid, allfields=all_fields
        )

    async def update_all_field_ids(
        self,
        *,
        updated: Optional[list[FieldID]] = None,
        deleted: Optional[list[FieldID]] = None,
        errors: Optional[list[writer_pb2.Error]] = None,
    ):
        needs_update = False
        all_fields = await self.get_all_field_ids(for_update=True)
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
        for field, text in message.texts.items():
            fid = FieldID(field_type=FieldType.TEXT, field=field)
            await self.set_field(fid.field_type, fid.field, text)
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

        if len(message_updated_fields) or len(message.delete_fields) or len(message.errors):
            await self.update_all_field_ids(
                updated=message_updated_fields,
                deleted=message.delete_fields,  # type: ignore
                errors=message.errors,  # type: ignore
            )

    @processor_observer.wrap({"type": "apply_fields_status"})
    async def apply_fields_status(self, message: BrokerMessage, updated_fields: list[FieldID]):
        # Dictionary of all errors per field (we may have several due to DA tasks)
        errors_by_field: dict[tuple[FieldType.ValueType, str], list[writer_pb2.Error]] = defaultdict(
            list
        )

        # Make sure if a file is updated without errors, it ends up in errors_by_field
        for field_id in updated_fields:
            errors_by_field[(field_id.field_type, field_id.field)] = []
        for fs in message.field_statuses:
            errors_by_field[(fs.id.field_type, fs.id.field)] = []

        for error in message.errors:
            errors_by_field[(error.field_type, error.field)].append(error)

        # If this message comes from the processor (not a DA worker), we clear all previous errors
        # TODO: When generated_by is populated with DA tasks by processor, remove only related errors
        from_processor = any((x.WhichOneof("generator") == "processor" for x in message.generated_by))

        for (field_type, field), errors in errors_by_field.items():
            field_obj = await self.get_field(field, field_type, load=False)
            if from_processor:
                status = writer_pb2.FieldStatus()
            else:
                status = await field_obj.get_status() or writer_pb2.FieldStatus()

            for error in errors:
                field_error = writer_pb2.FieldError(
                    source_error=error,
                )
                field_error.created.GetCurrentTime()
                status.errors.append(field_error)

            # We infer the status for processor messages
            if message.source == BrokerMessage.MessageSource.PROCESSOR:
                if len(errors) > 0:
                    status.status = writer_pb2.FieldStatus.Status.ERROR
                else:
                    status.status = writer_pb2.FieldStatus.Status.PROCESSED
            else:
                field_status = next(
                    (
                        fs.status
                        for fs in message.field_statuses
                        if fs.id.field_type == field_type and fs.id.field == field
                    ),
                    None,
                )
                if field_status:
                    status.status = field_status

            await field_obj.set_status(status)

    async def update_status(self):
        field_ids = await self.get_all_field_ids(for_update=False)
        field_statuses = await datamanagers.fields.get_statuses(
            self.txn, kbid=self.kb.kbid, rid=self.uuid, fields=field_ids.fields
        )
        # If any field is processing -> PENDING
        if any((f.status == writer_pb2.FieldStatus.Status.PENDING for f in field_statuses)):
            self.basic.metadata.status = PBMetadata.Status.PENDING
        # If we have any non-DA error -> ERROR
        elif any(
            (
                f.status == writer_pb2.FieldStatus.Status.ERROR
                and any(
                    (
                        e.source_error.code != writer_pb2.Error.ErrorCode.DATAAUGMENTATION
                        for e in f.errors
                    )
                )
                for f in field_statuses
            )
        ):
            self.basic.metadata.status = PBMetadata.Status.ERROR
        # Otherwise (everything processed or we only have DA errors) -> PROCESSED
        else:
            self.basic.metadata.status = PBMetadata.Status.PROCESSED

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

        # TODO: Update field and resource status depending on processing results
        await self.apply_fields_status(message, self._modified_extracted_text)
        # await self.update_status()

        extracted_languages = []

        for link_extracted_data in message.link_extracted_data:
            await self._apply_link_extracted_data(link_extracted_data)
            await self.maybe_update_resource_title_from_link(link_extracted_data)
            extracted_languages.append(link_extracted_data.language)

        for file_extracted_data in message.file_extracted_data:
            await self._apply_file_extracted_data(file_extracted_data)
            extracted_languages.append(file_extracted_data.language)

        await self.maybe_update_resource_title_from_file_extracted_data(message)

        # Metadata should go first
        for field_metadata in message.field_metadata:
            await self._apply_field_computed_metadata(field_metadata)
            extracted_languages.extend(extract_field_metadata_languages(field_metadata))

        update_basic_languages(self.basic, extracted_languages)

        # Upload to binary storage
        # Vector indexing
        if self.disable_vectors is False:
            await self._apply_extracted_vectors(message.field_vectors)

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

    async def _apply_question_answers(self, question_answers: FieldQuestionAnswerWrapper):
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

    async def maybe_update_resource_title_from_link(self, link_extracted_data: LinkExtractedData):
        """
        When parsing link extracted data, we want to replace the resource title for the first link
        that gets processed and has a title, and only if the current title is a URL, which we take
        as a hint that the title was not set by the user.
        """
        assert self.basic is not None
        if not link_extracted_data.title:
            return
        if not (self.basic.title.startswith("http") or self.basic.title == ""):
            return
        title = link_extracted_data.title
        await self.update_resource_title(title)

    async def update_resource_title(self, computed_title: str) -> None:
        assert self.basic is not None
        self.basic.title = computed_title
        # Extracted text
        field = await self.get_field("title", FieldType.GENERIC, load=False)
        etw = ExtractedTextWrapper()
        etw.body.text = computed_title
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
        paragraph = Paragraph(start=0, end=len(computed_title), kind=Paragraph.TypeParagraph.TITLE)
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

    async def _should_update_resource_title_from_file_metadata(self) -> bool:
        """
        We only want to update resource title from file metadata if the title is empty,
        equal to the resource uuid or equal to any of the file filenames in the resource.
        """
        basic = await self.get_basic()
        if basic is None:
            return True
        current_title = basic.title
        if current_title == "":
            # If the title is empty, we should update it
            return True
        if current_title == self.uuid:
            # If the title is the same as the resource uuid, we should update it
            return True
        fields = await self.get_fields(force=True)
        filenames = set()
        for (field_type, _), field_obj in fields.items():
            if field_type == FieldType.FILE:
                field_value: Optional[FieldFile] = await field_obj.get_value()
                if field_value is not None:
                    if field_value.file.filename not in ("", None):
                        filenames.add(field_value.file.filename)
        if current_title in filenames:
            # If the title is equal to any of the file filenames, we should update it
            return True
        return False

    async def maybe_update_resource_title_from_file_extracted_data(self, message: BrokerMessage):
        """
        Update the resource title with the first file that has a title extracted.
        """
        if not await self._should_update_resource_title_from_file_metadata():
            return
        for fed in message.file_extracted_data:
            if fed.title == "":
                # Skip if the extracted title is empty
                continue
            fid = FieldId.from_pb(rid=self.uuid, field_type=FieldType.FILE, key=fed.field)
            logger.info(
                "Updating resource title from file extracted data",
                extra={"kbid": self.kb.kbid, "field": fid.full(), "new_title": fed.title},
            )
            await self.update_resource_title(fed.title)
            # Break after the first file with a title is found
            break

    async def _apply_field_computed_metadata(self, field_metadata: FieldComputedMetadataWrapper):
        assert self.basic is not None
        maybe_update_basic_summary(self.basic, field_metadata.metadata.metadata.summary)

        field_obj = await self.get_field(
            field_metadata.field.field,
            field_metadata.field.field_type,
            load=False,
        )
        metadata = await field_obj.set_field_metadata(field_metadata)
        field_key = self.generate_field_id(field_metadata.field)

        page_positions: Optional[FilePagePositions] = None
        if field_metadata.field.field_type == FieldType.FILE and isinstance(field_obj, File):
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
            page_positions=page_positions,
            extracted_text=extracted_text,
            basic_user_field_metadata=user_field_metadata,
            replace_field=True,
        )
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(_executor, apply_field_metadata)

        maybe_update_basic_thumbnail(self.basic, field_metadata.metadata.metadata.thumbnail)

        add_field_classifications(self.basic, field_metadata)

    async def _apply_extracted_vectors(
        self,
        fields_vectors: Sequence[ExtractedVectorsWrapper],
    ):
        await self.get_fields(force=True)
        vectorsets = {
            vectorset_id: vs
            async for vectorset_id, vs in datamanagers.vectorsets.iter(self.txn, kbid=self.kb.kbid)
        }

        for field_vectors in fields_vectors:
            # Bw/c with extracted vectors without vectorsets
            if not field_vectors.vectorset_id:
                assert (
                    len(vectorsets) == 1
                ), "Invalid broker message, can't ingest vectors from unknown vectorset to KB with multiple vectorsets"
                vectorset = list(vectorsets.values())[0]

            else:
                if field_vectors.vectorset_id not in vectorsets:
                    logger.warning(
                        "Dropping extracted vectors for unknown vectorset",
                        extra={"kbid": self.kb.kbid, "vectorset": field_vectors.vectorset_id},
                    )
                    continue

                vectorset = vectorsets[field_vectors.vectorset_id]

            # Store vectors in the resource

            if not self.has_field(field_vectors.field.field_type, field_vectors.field.field):
                # skipping because field does not exist
                logger.warning(f'Field "{field_vectors.field.field}" does not exist, skipping vectors')
                return

            field_obj = await self.get_field(
                field_vectors.field.field,
                field_vectors.field.field_type,
                load=False,
            )
            vo = await field_obj.set_vectors(
                field_vectors, vectorset.vectorset_id, vectorset.storage_key_kind
            )
            if vo is None:
                raise AttributeError("Vector object not found on set_vectors")

            # Prepare vectors to be indexed

            field_key = self.generate_field_id(field_vectors.field)
            dimension = vectorset.vectorset_index_config.vector_dimension
            if not dimension:
                raise ValueError(f"Vector dimension not set for vectorset '{vectorset.vectorset_id}'")

            apply_field_vectors_partial = partial(
                self.indexer.apply_field_vectors,
                field_key,
                vo,
                vectorset=vectorset.vectorset_id,
                replace_field=True,
                vector_dimension=dimension,
            )
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(_executor, apply_field_vectors_partial)

    async def _apply_field_large_metadata(self, field_large_metadata: LargeComputedMetadataWrapper):
        field_obj = await self.get_field(
            field_large_metadata.field.field,
            field_large_metadata.field.field_type,
            load=False,
        )
        await field_obj.set_large_field_metadata(field_large_metadata)

    def generate_field_id(self, field: FieldID) -> str:
        return f"{FIELD_TYPE_PB_TO_STR[field.field_type]}/{field.field}"

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
            fieldid = FieldID(field_type=type, field=field)
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

            generated_by = await fieldobj.generated_by()
            brain.apply_field_labels(
                fieldkey,
                extracted_metadata,
                self.uuid,
                generated_by,
                basic.usermetadata,
                valid_user_field_metadata,
            )

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
        fc for fc in basic.computedmetadata.field_classifications if fc.field not in deleted_fields
    ]
    basic.computedmetadata.ClearField("field_classifications")
    basic.computedmetadata.field_classifications.extend(field_classifications)


def add_field_classifications(basic: PBBasic, fcmw: FieldComputedMetadataWrapper) -> bool:
    """
    Returns whether some new field classifications were added
    """
    if len(fcmw.metadata.metadata.classifications) == 0 and all(
        len(split.classifications) == 0 for split in fcmw.metadata.split_metadata.values()
    ):
        return False

    remove_field_classifications(basic, [fcmw.field])
    fcfs = FieldClassifications()
    fcfs.field.CopyFrom(fcmw.field)
    fcfs.classifications.extend(fcmw.metadata.metadata.classifications)

    for split_id, split in fcmw.metadata.split_metadata.items():
        if split_id not in fcmw.metadata.deleted_splits:
            fcfs.classifications.extend(split.classifications)

    basic.computedmetadata.field_classifications.append(fcfs)
    return True


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

    if not content_types.valid(mimetype):
        logger.warning(
            "Invalid mimetype. Skipping icon update.",
            extra={"mimetype": mimetype, "rid": basic.uuid, "slug": basic.slug},
        )
        return False

    basic.icon = mimetype
    return True


def maybe_update_basic_thumbnail(basic: PBBasic, thumbnail: Optional[CloudFile]) -> bool:
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
