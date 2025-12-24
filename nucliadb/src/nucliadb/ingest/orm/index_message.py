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
from collections.abc import Sequence

from nidx_protos.noderesources_pb2 import Resource as IndexMessage

from nucliadb.common import datamanagers
from nucliadb.ingest.fields.conversation import Conversation
from nucliadb.ingest.fields.exceptions import FieldAuthorNotFound
from nucliadb.ingest.fields.file import File
from nucliadb.ingest.orm.brain_v2 import ResourceBrain
from nucliadb.ingest.orm.metrics import index_message_observer as observer
from nucliadb.ingest.orm.resource import Resource, get_file_page_positions
from nucliadb_protos.knowledgebox_pb2 import VectorSetConfig
from nucliadb_protos.resources_pb2 import Basic, FieldID, FieldType
from nucliadb_protos.utils_pb2 import ExtractedText
from nucliadb_protos.writer_pb2 import BrokerMessage


class IndexMessageBuilder:
    def __init__(self, resource: Resource):
        self.resource = resource
        self.brain = ResourceBrain(resource.uuid)

    @observer.wrap({"type": "resource_data"})
    async def _apply_resource_index_data(self, brain: ResourceBrain) -> None:
        # Set the metadata at the resource level
        basic = await self.resource.get_basic()
        assert basic is not None
        user_relations = await self.resource.get_user_relations()
        origin = await self.resource.get_origin()
        security = await self.resource.get_security()
        await asyncio.to_thread(
            brain.generate_resource_metadata,
            basic,
            user_relations,
            origin,
            self.resource._previous_status,
            security,
        )

    @observer.wrap({"type": "field_data"})
    async def _apply_field_index_data(
        self,
        brain: ResourceBrain,
        fieldid: FieldID,
        basic: Basic,
        texts: bool = True,
        paragraphs: bool = True,
        vectors: bool = True,
        relations: bool = True,
        replace: bool = True,
        vectorset_configs: list[VectorSetConfig] | None = None,
        append_splits: set[str] | None = None,
    ):
        field = await self.resource.get_field(fieldid.field, fieldid.field_type)
        extracted_text = await field.get_extracted_text()
        field_computed_metadata = await field.get_field_metadata()
        user_field_metadata = next(
            (fm for fm in basic.fieldmetadata if fm.field == fieldid),
            None,
        )
        if texts or paragraphs:
            # We need to compute the texts when we're going to generate the paragraphs too, but we may not
            # want to index them always.
            skip_texts_index = not texts
            replace_texts = replace and not skip_texts_index

            if extracted_text is not None:
                try:
                    field_author = await field.generated_by()
                except FieldAuthorNotFound:
                    field_author = None
                await asyncio.to_thread(
                    brain.generate_texts,
                    self.resource.generate_field_id(fieldid),
                    extracted_text,
                    field_computed_metadata,
                    basic.usermetadata,
                    field_author,
                    replace_field=replace_texts,
                    skip_index=skip_texts_index,
                )
        if paragraphs or vectors:
            # The paragraphs are needed to generate the vectors. However, we don't need to index them
            # in all cases.
            skip_paragraphs_index = not paragraphs
            skip_texts_index = not texts
            replace_paragraphs = replace and not skip_paragraphs_index

            # We need to compute the paragraphs when we're going to generate the vectors too.
            if extracted_text is not None and field_computed_metadata is not None:
                page_positions = (
                    await get_file_page_positions(field) if isinstance(field, File) else None
                )
                await asyncio.to_thread(
                    brain.generate_paragraphs,
                    self.resource.generate_field_id(fieldid),
                    field_computed_metadata,
                    extracted_text,
                    page_positions,
                    user_field_metadata,
                    replace_field=replace_paragraphs,
                    skip_paragraphs_index=skip_paragraphs_index,
                    skip_texts_index=skip_texts_index,
                    append_splits=append_splits,
                )
        if vectors:
            assert vectorset_configs is not None
            for vectorset_config in vectorset_configs:
                vo = await field.get_vectors(
                    vectorset=vectorset_config.vectorset_id,
                    storage_key_kind=vectorset_config.storage_key_kind,
                )
                if vo is not None:
                    dimension = vectorset_config.vectorset_index_config.vector_dimension
                    await asyncio.to_thread(
                        brain.generate_vectors,
                        self.resource.generate_field_id(fieldid),
                        vo,
                        vectorset=vectorset_config.vectorset_id,
                        replace_field=replace,
                        vector_dimension=dimension,
                        append_splits=append_splits,
                    )
        if relations:
            await asyncio.to_thread(
                brain.generate_relations,
                self.resource.generate_field_id(fieldid),
                field_computed_metadata,
                basic.usermetadata,
                replace_field=replace,
            )

    def _apply_field_deletions(
        self,
        brain: ResourceBrain,
        field_ids: Sequence[FieldID],
    ) -> None:
        for field_id in field_ids:
            brain.delete_field(self.resource.generate_field_id(field_id))

    @observer.wrap({"type": "writer_bm"})
    async def for_writer_bm(
        self,
        message: BrokerMessage,
        resource_created: bool,
    ) -> IndexMessage:
        """
        Builds the index message for the broker message coming from the writer.
        The writer messages are not adding new vectors to the index.
        """
        assert message.source == BrokerMessage.MessageSource.WRITER

        self._apply_field_deletions(self.brain, message.delete_fields)
        await self._apply_resource_index_data(self.brain)
        basic = await self.get_basic()
        prefilter_update = needs_prefilter_update(message)
        if prefilter_update:
            # Changes on some metadata at the resource level that is used for filtering require that we reindex all the fields
            # in the texts index (as it is the one used for prefiltering).
            fields_to_index = [
                FieldID(field=field_id, field_type=field_type)
                for field_type, field_id in await self.resource.get_fields(force=True)
            ]
        else:
            # Simply process the fields that are in the message
            fields_to_index = get_bm_modified_fields(message)
        for fieldid in fields_to_index:
            if fieldid in message.delete_fields:
                continue
            await self._apply_field_index_data(
                self.brain,
                fieldid,
                basic,
                texts=prefilter_update or needs_texts_update(fieldid, message),
                paragraphs=needs_paragraphs_update(fieldid, message),
                relations=False,  # Relations at the field level are not modified by the writer
                vectors=False,  # Vectors are never added by the writer
                replace=not resource_created,
            )
        return self.brain.brain

    @observer.wrap({"type": "processor_bm"})
    async def for_processor_bm(
        self,
        message: BrokerMessage,
    ) -> IndexMessage:
        """
        Builds the index message for the broker messages coming from the processor.
        The processor can index new data to any index.
        """
        assert message.source == BrokerMessage.MessageSource.PROCESSOR
        self._apply_field_deletions(self.brain, message.delete_fields)
        await self._apply_resource_index_data(self.brain)
        basic = await self.get_basic()
        fields_to_index = get_bm_modified_fields(message)
        vectorsets_configs = await self.get_vectorsets_configs()
        for fieldid in fields_to_index:
            if fieldid in message.delete_fields:
                continue

            # For conversation fields, we only replace the full field if it is not an append messages operation.
            # All other fields are always replaced upon modification.
            replace_field = True
            modified_splits = None
            if fieldid.field_type == FieldType.CONVERSATION:
                modified_splits = await get_bm_modified_split_ids(fieldid, message, self.resource)
                stored_splits = await get_stored_split_ids(fieldid, self.resource)
                is_append_messages_op = modified_splits.issubset(stored_splits) and 0 < len(
                    modified_splits
                ) < len(stored_splits)
                replace_field = not is_append_messages_op

            await self._apply_field_index_data(
                self.brain,
                fieldid,
                basic,
                texts=needs_texts_update(fieldid, message),
                paragraphs=needs_paragraphs_update(fieldid, message),
                relations=needs_relations_update(fieldid, message),
                vectors=needs_vectors_update(fieldid, message),
                replace=replace_field,
                vectorset_configs=vectorsets_configs,
                append_splits=modified_splits,
            )
        return self.brain.brain

    @observer.wrap({"type": "full"})
    async def full(self, reindex: bool) -> IndexMessage:
        await self._apply_resource_index_data(self.brain)
        basic = await self.get_basic()
        fields_to_index = [
            FieldID(field=field_id, field_type=field_type)
            for field_type, field_id in await self.resource.get_fields(force=True)
        ]
        vectorsets_configs = await self.get_vectorsets_configs()
        for fieldid in fields_to_index:
            await self._apply_field_index_data(
                self.brain,
                fieldid,
                basic,
                texts=True,
                paragraphs=True,
                relations=True,
                vectors=True,
                replace=reindex,
                vectorset_configs=vectorsets_configs,
            )
        return self.brain.brain

    async def get_basic(self) -> Basic:
        basic = await self.resource.get_basic()
        assert basic is not None
        return basic

    async def get_vectorsets_configs(self) -> list[VectorSetConfig]:
        """
        Get the vectorsets config for the resource.
        """
        vectorset_configs = [
            vectorset_config
            async for _, vectorset_config in datamanagers.vectorsets.iter(
                self.resource.txn, kbid=self.resource.kbid
            )
        ]
        return vectorset_configs


def get_bm_modified_fields(message: BrokerMessage) -> list[FieldID]:
    modified = set()
    # Added or modified fields need indexing
    for link in message.links:
        modified.add((link, FieldType.LINK))
    for file in message.files:
        modified.add((file, FieldType.FILE))
    for conv in message.conversations:
        modified.add((conv, FieldType.CONVERSATION))
    for text in message.texts:
        modified.add((text, FieldType.TEXT))
    if message.HasField("basic"):
        # Add title and summary only if they have changed
        if message.basic.title != "":
            modified.add(("title", FieldType.GENERIC))
        if message.basic.summary != "":
            modified.add(("summary", FieldType.GENERIC))

    if message.source == BrokerMessage.MessageSource.PROCESSOR:
        # Messages with field metadata, extracted text or field vectors need indexing
        for fm in message.field_metadata:
            modified.add((fm.field.field, fm.field.field_type))
        for et in message.extracted_text:
            modified.add((et.field.field, et.field.field_type))
        for fv in message.field_vectors:
            modified.add((fv.field.field, fv.field.field_type))

    if message.source == BrokerMessage.MessageSource.WRITER:
        # Any field that has fieldmetadata annotations should be considered as modified
        # and needs to be reindexed
        if message.HasField("basic"):
            for ufm in message.basic.fieldmetadata:
                modified.add((ufm.field.field, ufm.field.field_type))
    return [FieldID(field=field, field_type=field_type) for field, field_type in modified]


def needs_prefilter_update(message: BrokerMessage) -> bool:
    return message.reindex


def needs_paragraphs_update(field_id: FieldID, message: BrokerMessage) -> bool:
    return (
        has_paragraph_annotations(field_id, message)
        or has_new_extracted_text(field_id, message)
        or has_new_field_metadata(field_id, message)
    )


def has_paragraph_annotations(field_id: FieldID, message: BrokerMessage) -> bool:
    ufm = next(
        (fm for fm in message.basic.fieldmetadata if fm.field == field_id),
        None,
    )
    if ufm is None:
        return False
    return len(ufm.paragraphs) > 0


def has_new_field_metadata(
    field_id: FieldID,
    message: BrokerMessage,
) -> bool:
    return any(field_metadata.field == field_id for field_metadata in message.field_metadata)


def has_new_extracted_text(
    field_id: FieldID,
    message: BrokerMessage,
) -> bool:
    return any(extracted_text.field == field_id for extracted_text in message.extracted_text)


def needs_texts_update(
    field_id: FieldID,
    message: BrokerMessage,
) -> bool:
    return has_new_extracted_text(field_id, message) or has_new_field_metadata(field_id, message)


def needs_vectors_update(
    field_id: FieldID,
    message: BrokerMessage,
) -> bool:
    return any(field_vectors.field == field_id for field_vectors in message.field_vectors)


async def get_bm_modified_split_ids(
    conversation_field_id: FieldID,
    message: BrokerMessage,
    resource: Resource,
) -> set[str]:
    message_etw = next(
        (etw for etw in message.extracted_text if etw.field == conversation_field_id), None
    )
    if message_etw is None:
        return set()
    storage = resource.storage
    if message_etw.HasField("file"):
        raw_payload = await storage.downloadbytescf(message_etw.file)
        message_extracted_text = ExtractedText()
        message_extracted_text.ParseFromString(raw_payload.read())
        raw_payload.flush()
    else:
        message_extracted_text = message_etw.body
    return set(message_extracted_text.split_text.keys())


async def get_stored_split_ids(
    conversation_field_id: FieldID,
    resource: Resource,
) -> set[str]:
    fid = conversation_field_id
    conv: Conversation = await resource.get_field(fid.field, fid.field_type, load=False)
    splits_metadata = await conv.get_splits_metadata()
    return set(splits_metadata.metadata)


def needs_relations_update(
    field_id: FieldID,
    message: BrokerMessage,
) -> bool:
    return has_new_field_metadata(field_id, message) or has_new_extracted_text(field_id, message)


async def get_resource_index_message(
    resource: Resource,
    reindex: bool = False,
) -> IndexMessage:
    """
    Get the full index message for a resource.
    """
    im_builder = IndexMessageBuilder(resource)
    return await im_builder.full(reindex=reindex)
