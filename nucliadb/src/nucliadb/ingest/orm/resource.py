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
import uuid
from collections import defaultdict
from collections.abc import Sequence
from typing import Any, cast

from nucliadb.common import datamanagers, file_md5
from nucliadb.common.ids import FIELD_TYPE_PB_TO_STR, FIELD_TYPE_STR_TO_PB
from nucliadb.common.maindb.driver import Transaction
from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.fields.conversation import Conversation
from nucliadb.ingest.fields.file import File
from nucliadb.ingest.fields.generic import VALID_GENERIC_FIELDS, Generic
from nucliadb.ingest.fields.key_value import KeyValue
from nucliadb.ingest.fields.link import Link
from nucliadb.ingest.fields.text import Text
from nucliadb.ingest.orm.brain_v2 import FilePagePositions
from nucliadb_protos import utils_pb2, writer_pb2
from nucliadb_protos.resources_pb2 import AllFieldIDs as PBAllFieldIDs
from nucliadb_protos.resources_pb2 import Basic as PBBasic
from nucliadb_protos.resources_pb2 import (
    CloudFile,
    ExtractedTextWrapper,
    ExtractedVectorsWrapper,
    FieldComputedMetadataWrapper,
    FieldFile,
    FieldID,
    FieldQuestionAnswerWrapper,
    FieldType,
    FileExtractedData,
    LargeComputedMetadataWrapper,
    LinkExtractedData,
    Metadata,
    SemanticGraphEdgeVectors,
    SemanticGraphNodeVectors,
)
from nucliadb_protos.resources_pb2 import Conversation as PBConversation
from nucliadb_protos.resources_pb2 import Extra as PBExtra
from nucliadb_protos.resources_pb2 import Origin as PBOrigin
from nucliadb_protos.resources_pb2 import Relations as PBRelations
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import get_storage

logger = logging.getLogger(__name__)

KB_FIELDS: dict[int, type] = {
    FieldType.TEXT: Text,
    FieldType.FILE: File,
    FieldType.LINK: Link,
    FieldType.GENERIC: Generic,
    FieldType.CONVERSATION: Conversation,
    FieldType.KEY_VALUE: KeyValue,
}


class Resource:
    def __init__(
        self,
        txn: Transaction,
        storage: Storage,
        kbid: str,
        uuid: str,
        basic: PBBasic | None = None,
        disable_vectors: bool = True,
    ):
        self.fields: dict[tuple[FieldType.ValueType, str], Field] = {}
        self.conversations: dict[int, PBConversation] = {}
        self.relations: PBRelations | None = None
        self.all_fields_keys: list[tuple[FieldType.ValueType, str]] | None = None
        self.origin: PBOrigin | None = None
        self.extra: PBExtra | None = None
        self.security: utils_pb2.Security | None = None
        self.modified: bool = False

        self.txn: Transaction = txn
        self.storage = storage
        self.kbid = kbid
        self.uuid = uuid
        self.basic = basic
        self.disable_vectors = disable_vectors
        self._previous_status: Metadata.Status.ValueType | None = None
        self.user_relations: PBRelations | None = None
        self.locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    @staticmethod
    def new_unique_rid() -> str:
        return uuid.uuid4().hex

    @classmethod
    async def get(cls, txn: Transaction, kbid: str, rid: str) -> Resource | None:
        basic = await datamanagers.resources.get_basic(txn, kbid=kbid, rid=rid)
        if basic is None:
            return None
        storage = await get_storage()
        return cls(
            txn=txn,
            storage=storage,
            kbid=kbid,
            uuid=rid,
            basic=basic,
            disable_vectors=False,
        )

    async def set_slug(self):
        basic = await self.get_basic()
        await datamanagers.resources.set_slug(self.txn, kbid=self.kbid, rid=self.uuid, slug=basic.slug)

    # Basic
    async def get_basic(self) -> PBBasic:
        if self.basic is None:
            basic = await datamanagers.resources.get_basic(self.txn, kbid=self.kbid, rid=self.uuid)
            self.basic = basic if basic is not None else PBBasic()
        return self.basic

    async def set_basic(self, payload: PBBasic) -> None:
        await datamanagers.resources.set_basic(self.txn, kbid=self.kbid, rid=self.uuid, basic=payload)
        self.basic = payload
        self.modified = True

    # Origin
    async def get_origin(self) -> PBOrigin | None:
        if self.origin is None:
            origin = await datamanagers.resources.get_origin(self.txn, kbid=self.kbid, rid=self.uuid)
            self.origin = origin
        return self.origin

    async def set_origin(self, payload: PBOrigin):
        await datamanagers.resources.set_origin(self.txn, kbid=self.kbid, rid=self.uuid, origin=payload)
        self.modified = True
        self.origin = payload

    # Extra
    async def get_extra(self) -> PBExtra | None:
        if self.extra is None:
            extra = await datamanagers.resources.get_extra(self.txn, kbid=self.kbid, rid=self.uuid)
            self.extra = extra
        return self.extra

    async def set_extra(self, payload: PBExtra):
        await datamanagers.resources.set_extra(self.txn, kbid=self.kbid, rid=self.uuid, extra=payload)
        self.modified = True
        self.extra = payload

    # Security
    async def get_security(self) -> utils_pb2.Security | None:
        if self.security is None:
            security = await datamanagers.resources.get_security(self.txn, kbid=self.kbid, rid=self.uuid)
            self.security = security
        return self.security

    async def set_security(self, payload: utils_pb2.Security) -> None:
        await datamanagers.resources.set_security(
            self.txn, kbid=self.kbid, rid=self.uuid, security=payload
        )
        self.modified = True
        self.security = payload

    # Relations
    async def get_user_relations(self) -> PBRelations:
        if self.user_relations is None:
            sf = self.storage.user_relations(self.kbid, self.uuid)
            relations = await self.storage.download_pb(sf, PBRelations)
            if relations is None:
                # Key not found = no relations
                self.user_relations = PBRelations()
            else:
                self.user_relations = relations
        return self.user_relations

    async def set_user_relations(self, payload: PBRelations):
        sf = self.storage.user_relations(self.kbid, self.uuid)
        await self.storage.upload_pb(sf, payload)
        self.modified = True
        self.user_relations = payload

    # Fields
    async def get_fields(
        self, force: bool = False, load_values: bool = False
    ) -> dict[tuple[FieldType.ValueType, str], Field]:
        """
        Get all fields of the resource.
        Params:
            force: If True, forces a refresh of the fields from the database, ignoring any cached values.
            load_values: If True, loads the values of the fields from the database. If False, only the field orm objects are created and cached.
        """
        for type, field in await self.get_fields_ids(force=force):
            if (type, field) not in self.fields:
                self.fields[(type, field)] = await self.get_field(field, type, load=load_values)
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
            async with self.locks["field"]:
                # Field could have been fetch while waiting for the lock
                if field not in self.fields:
                    field_obj: Field = KB_FIELDS[type](id=key, resource=self)
                    if load:
                        # Fetch the field value from the database.
                        # The value is cached in the field object for future use.
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
        await field_obj.delete()

    async def _apply_delete_splits(self, payload: writer_pb2.DeleteSplits) -> None:
        if payload.field.field_type != FieldType.CONVERSATION:
            raise ValueError("_apply_delete_splits can only be applied to conversation fields")
        field = await self.get_field(payload.field.field, FieldType.CONVERSATION, load=False)
        conv = cast(Conversation, field)
        await conv.delete_messages(payload.splits)
        self.modified = True

    async def field_exists(self, type: FieldType.ValueType, field: str) -> bool:
        """Return whether this resource has this field or not."""
        all_fields_ids = await self.get_fields_ids()
        for field_type, field_id in all_fields_ids:
            if field_type == type and field_id == field:
                return True
        return False

    async def get_all_field_ids(self, *, for_update: bool) -> PBAllFieldIDs | None:
        return await datamanagers.resources.get_all_field_ids(
            self.txn, kbid=self.kbid, rid=self.uuid, for_update=for_update
        )

    async def set_all_field_ids(self, all_fields: PBAllFieldIDs):
        return await datamanagers.resources.set_all_field_ids(
            self.txn, kbid=self.kbid, rid=self.uuid, allfields=all_fields
        )

    async def update_all_field_ids(
        self,
        *,
        updated: list[FieldID] | None = None,
        deleted: list[FieldID] | None = None,
        errors: list[writer_pb2.Error] | None = None,
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

    async def apply_field_values(self, message: BrokerMessage):
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
            await self.set_file_field_md5(field, file.file)
            message_updated_fields.append(fid)

        for field, conversation in message.conversations.items():
            fid = FieldID(field_type=FieldType.CONVERSATION, field=field)
            await self.set_field(fid.field_type, fid.field, conversation)
            message_updated_fields.append(fid)

        for field, kv in message.key_value_fields.items():
            fid = FieldID(field_type=FieldType.KEY_VALUE, field=field)
            await self.set_field(fid.field_type, fid.field, kv)
            message_updated_fields.append(fid)

        for fieldid in message.delete_fields:
            await self.delete_field(fieldid.field_type, fieldid.field)
            if fieldid.field_type == FieldType.FILE:
                await self.delete_file_field_md5(fieldid.field)

        for delete_splits in message.delete_splits:
            await self._apply_delete_splits(delete_splits)

        if len(message_updated_fields) or len(message.delete_fields) or len(message.errors):
            await self.update_all_field_ids(
                updated=message_updated_fields,
                deleted=message.delete_fields,  # type: ignore
                errors=message.errors,  # type: ignore
            )
            self.modified = True

        await self.apply_fields_status(message)

    async def set_file_field_md5(self, field_id: str, file: CloudFile):
        """
        Record file MD5 hashes for deduplication checks.
        """
        if not file.md5:
            # To be safe, delete any existing MD5 record for this field if the new file doesn't have an MD5.
            # This is for PUT operations where the file is being replaced but the new md5 is not provided.
            await self.delete_file_field_md5(field_id)
        else:
            await file_md5.set(self.txn, kbid=self.kbid, md5=file.md5, rid=self.uuid, field_id=field_id)

    async def delete_file_field_md5(self, field_id: str):
        """
        Delete file MD5 hash records for a given field.
        """
        await file_md5.delete(self.txn, kbid=self.kbid, rid=self.uuid, field_id=field_id)

    async def apply_fields_status(self, message: BrokerMessage):
        # Update the status for fields for which the extracted text has been updated.
        updated_fields = [et.field for et in message.extracted_text]

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
        from_processor = any(x.WhichOneof("generator") == "processor" for x in message.generated_by)

        for (field_type, field), errors in errors_by_field.items():
            field_obj = await self.get_field(field, field_type, load=False)
            if from_processor:
                # Create a new field status to clear all errors
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
                if any(
                    e.source_error.severity == writer_pb2.Error.Severity.ERROR for e in status.errors
                ):
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
                if field_status is not None:
                    status.status = field_status
                # If the field was not found and the message comes from the writer, this implicitly sets the
                # status to the default value, which is PROCESSING. This covers the case of new field creation.

            await field_obj.set_status(status)
            self.modified = True

    async def add_field_error(
        self, field_id: str, message: str, severity: writer_pb2.Error.Severity.ValueType
    ):
        (field_type_str, field_name) = field_id.split("/")
        field_type = FIELD_TYPE_STR_TO_PB[field_type_str]
        field = await self.get_field(field_name, field_type, load=False)
        status = await field.get_status()
        if status is not None:
            field_error = writer_pb2.FieldError(
                source_error=writer_pb2.Error(
                    field=field_name,
                    field_type=field_type,
                    error=message,
                    code=writer_pb2.Error.ErrorCode.INDEX,
                    severity=severity,
                )
            )
            field_error.created.GetCurrentTime()
            status.errors.append(field_error)
            if severity == writer_pb2.Error.Severity.ERROR:
                status.status = writer_pb2.FieldStatus.Status.ERROR
            await field.set_status(status)
        self.modified = True

    async def apply_field_extracted_data(self, message: BrokerMessage) -> None:
        """
        Apply extracted field data from a broker message (extracted text, vectors,
        question answers, field metadata, large metadata). Does not touch basic.
        """
        tasks = []
        if message.question_answers or message.delete_question_answers:
            tasks.append(self._apply_question_answers_ops(message))

        for extracted_text in message.extracted_text:
            tasks.append(self._apply_extracted_text(extracted_text))

        for link_extracted_data in message.link_extracted_data:
            tasks.append(self._apply_link_extracted_data(link_extracted_data))

        for file_extracted_data in message.file_extracted_data:
            tasks.append(self._apply_file_extracted_data(file_extracted_data))

        for field_metadata in message.field_metadata:
            tasks.append(self._apply_field_computed_metadata(field_metadata))

        if self.disable_vectors is False:
            tasks.append(self._apply_extracted_vectors(message.field_vectors))
            tasks.append(
                self._apply_semantic_graph_node_vectors(message.field_semantic_graph_node_vectors)
            )
            tasks.append(
                self._apply_semantic_graph_edge_vectors(message.field_semantic_graph_edge_vectors)
            )

        for field_large_metadata in message.field_large_metadata:
            tasks.append(self._apply_field_large_metadata(field_large_metadata))

        await asyncio.gather(*tasks)
        tasks.clear()

    async def _apply_question_answers_ops(self, message: BrokerMessage):
        tasks = []
        updated = [qa.field for qa in message.question_answers]
        for question_answers in message.question_answers:
            tasks.append(self._apply_question_answers(question_answers))
        for field_id in message.delete_question_answers:
            if field_id not in updated:
                # Only delete those question answers that we are not adding/updating, just to be safe.
                tasks.append(self._delete_question_answers(field_id))
        await asyncio.gather(*tasks)

    async def _apply_extracted_text(self, extracted_text: ExtractedTextWrapper):
        field_obj = await self.get_field(
            extracted_text.field.field, extracted_text.field.field_type, load=False
        )
        await field_obj.set_extracted_text(extracted_text)
        self.modified = True

    async def _apply_question_answers(self, question_answers: FieldQuestionAnswerWrapper):
        field = question_answers.field
        field_obj = await self.get_field(field.field, field.field_type, load=False)
        await field_obj.set_question_answers(question_answers)
        self.modified = True

    async def _delete_question_answers(self, field_id: FieldID):
        field_obj = await self.get_field(field_id.field, field_id.field_type, load=False)
        await field_obj.delete_question_answers()
        self.modified = True

    async def _apply_link_extracted_data(self, link_extracted_data: LinkExtractedData):
        field_link: Link = await self.get_field(
            link_extracted_data.field,
            FieldType.LINK,
            load=False,
        )
        await field_link.set_link_extracted_data(link_extracted_data)
        self.modified = True

    async def _apply_file_extracted_data(self, file_extracted_data: FileExtractedData):
        field_file: File = await self.get_field(
            file_extracted_data.field,
            FieldType.FILE,
            load=False,
        )
        # uri can change after extraction
        await field_file.set_file_extracted_data(file_extracted_data)
        self.modified = True

    async def _apply_field_computed_metadata(self, field_metadata: FieldComputedMetadataWrapper):
        field_obj = await self.get_field(
            field_metadata.field.field,
            field_metadata.field.field_type,
            load=False,
        )
        await field_obj.set_field_metadata(field_metadata)
        self.modified = True

    async def _apply_extracted_vectors(
        self,
        fields_vectors: Sequence[ExtractedVectorsWrapper],
    ):
        vectorsets = {
            vectorset_id: vs
            async for vectorset_id, vs in datamanagers.vectorsets.iter(self.txn, kbid=self.kbid)
        }

        for field_vectors in fields_vectors:
            # Bw/c with extracted vectors without vectorsets
            if not field_vectors.vectorset_id:
                assert len(vectorsets) == 1, (
                    "Invalid broker message, can't ingest vectors from unknown vectorset to KB with multiple vectorsets"
                )
                vectorset = next(iter(vectorsets.values()))

            else:
                if field_vectors.vectorset_id not in vectorsets:
                    logger.warning(
                        "Dropping extracted vectors for unknown vectorset",
                        extra={"kbid": self.kbid, "vectorset": field_vectors.vectorset_id},
                    )
                    continue

                vectorset = vectorsets[field_vectors.vectorset_id]

            # Store vectors in the resource

            if not await self.field_exists(field_vectors.field.field_type, field_vectors.field.field):
                # skipping because field does not exist
                logger.warning(f'Field "{field_vectors.field.field}" does not exist, skipping vectors')
                continue

            field_obj = await self.get_field(
                field_vectors.field.field,
                field_vectors.field.field_type,
                load=False,
            )
            vo = await field_obj.set_vectors(
                field_vectors, vectorset.vectorset_id, vectorset.storage_key_kind
            )
            self.modified = True
            if vo is None:
                raise AttributeError("Vector object not found on set_vectors")

    async def _apply_semantic_graph_node_vectors(
        self,
        fields_vectors: Sequence[SemanticGraphNodeVectors],
    ):
        vectorset_ids = [
            vs.vectorset_id
            for vs in await datamanagers.graph_vectorsets.node.get_all(self.txn, kbid=self.kbid)
        ]

        for field_vectors in fields_vectors:
            if field_vectors.vectorset_id not in vectorset_ids:
                logger.warning(
                    "Dropping extracted vectors for unknown vectorset",
                    extra={"kbid": self.kbid, "vectorset": field_vectors.vectorset_id},
                )
                continue

            vectorset_id = field_vectors.vectorset_id

            # Store vectors in the resource
            if not await self.field_exists(field_vectors.field.field_type, field_vectors.field.field):
                # skipping because field does not exist
                logger.warning(f'Field "{field_vectors.field.field}" does not exist, skipping vectors')
                continue

            field_obj = await self.get_field(
                field_vectors.field.field,
                field_vectors.field.field_type,
                load=False,
            )
            await field_obj.set_relation_node_vectors(field_vectors, vectorset_id)
            self.modified = True

    async def _apply_semantic_graph_edge_vectors(
        self,
        fields_vectors: Sequence[SemanticGraphEdgeVectors],
    ):
        vectorset_ids = [
            vs.vectorset_id
            for vs in await datamanagers.graph_vectorsets.edge.get_all(self.txn, kbid=self.kbid)
        ]

        for field_vectors in fields_vectors:
            if field_vectors.vectorset_id not in vectorset_ids:
                logger.warning(
                    "Dropping extracted vectors for unknown vectorset",
                    extra={"kbid": self.kbid, "vectorset": field_vectors.vectorset_id},
                )
                continue

            vectorset_id = field_vectors.vectorset_id

            # Store vectors in the resource
            if not await self.field_exists(field_vectors.field.field_type, field_vectors.field.field):
                # skipping because field does not exist
                logger.warning(f'Field "{field_vectors.field.field}" does not exist, skipping vectors')
                continue

            field_obj = await self.get_field(
                field_vectors.field.field,
                field_vectors.field.field_type,
                load=False,
            )
            await field_obj.set_relation_edge_vectors(field_vectors, vectorset_id)
            self.modified = True

    async def _apply_field_large_metadata(self, field_large_metadata: LargeComputedMetadataWrapper):
        field_obj = await self.get_field(
            field_large_metadata.field.field,
            field_large_metadata.field.field_type,
            load=False,
        )
        await field_obj.set_large_field_metadata(field_large_metadata)
        self.modified = True

    async def get_filenames(self) -> list[str]:
        """
        Get all filenames from the resource file fields.
        """
        fields = await self.get_fields(force=True, load_values=False)
        filenames = set()
        for (field_type, _), field_obj in fields.items():
            if field_type == FieldType.FILE:
                field_value: FieldFile | None = await field_obj.get_value()
                if field_value is not None:
                    if field_value.file.filename not in ("", None):
                        filenames.add(field_value.file.filename)
        return list(filenames)

    def generate_field_id(self, field: FieldID) -> str:
        return f"{FIELD_TYPE_PB_TO_STR[field.field_type]}/{field.field}"

    def clean(self):
        # This intentionally unsets the transaction to release it
        # We set a type-checker exception here rather than asserting
        # that transaction is set all through the code
        self.txn = None  # type: ignore[ty:invalid-assignment]


async def get_file_page_positions(field: File) -> FilePagePositions:
    positions: FilePagePositions = {}
    file_extracted_data = await field.get_file_extracted_data()
    if file_extracted_data is None:
        return positions
    for index, position in enumerate(file_extracted_data.file_pages_previews.positions):
        positions[index] = (position.start, position.end)
    return positions
