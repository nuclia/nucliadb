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
from typing import Optional

from nidx_protos.noderesources_pb2 import Resource as IndexMessage

from nucliadb.common import datamanagers
from nucliadb.ingest.fields.exceptions import FieldAuthorNotFound
from nucliadb.ingest.fields.file import File
from nucliadb.ingest.orm.brain_v2 import ResourceBrain
from nucliadb.ingest.orm.metrics import index_message_observer as observer
from nucliadb.ingest.orm.resource import Resource, get_file_page_positions
from nucliadb_protos.knowledgebox_pb2 import VectorSetConfig
from nucliadb_protos.resources_pb2 import Basic, FieldID, FieldType
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
        vectorset_configs: Optional[list[VectorSetConfig]] = None,
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
            skip_index_texts = not texts
            replace_texts = replace and not skip_index_texts

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
                    skip_index=skip_index_texts,
                )
        if paragraphs or vectors:
            # The paragraphs are needed to generate the vectors. However, we don't need to index them
            # in all cases.
            skip_index_paragraphs = not paragraphs
            replace_paragraphs = replace and not skip_index_paragraphs

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
                    skip_index=skip_index_paragraphs,
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
        field_ids: list[FieldID],
    ) -> None:
        for field_id in field_ids:
            brain.delete_field(self.resource.generate_field_id(field_id))

    @observer.wrap({"type": "writer_bm"})
    async def for_writer_bm(
        self,
        messages: list[BrokerMessage],
        resource_created: bool,
    ) -> IndexMessage:
        """
        Builds the index message for the broker messages coming from the writer.
        The writer messages are not adding new vectors to the index.
        """
        assert all(message.source == BrokerMessage.MessageSource.WRITER for message in messages)

        deleted_fields = get_bm_deleted_fields(messages)
        self._apply_field_deletions(self.brain, deleted_fields)
        await self._apply_resource_index_data(self.brain)
        basic = await self.get_basic()
        prefilter_update = needs_prefilter_update(messages)
        if prefilter_update:
            # Changes on some metadata at the resource level that is used for filtering require that we reindex all the fields
            # in the texts index (as it is the one used for prefiltering).
            fields_to_index = [
                FieldID(field=field_id, field_type=field_type)
                for field_type, field_id in await self.resource.get_fields(force=True)
            ]
        else:
            # Simply process the fields that are in the message
            fields_to_index = get_bm_modified_fields(messages)
        for fieldid in fields_to_index:
            if fieldid in deleted_fields:
                continue
            await self._apply_field_index_data(
                self.brain,
                fieldid,
                basic,
                texts=prefilter_update or needs_texts_update(fieldid, messages),
                paragraphs=needs_paragraphs_update(fieldid, messages),
                relations=False,  # Relations at the field level are not modified by the writer
                vectors=False,  # Vectors are never added by the writer
                replace=not resource_created,
            )
        return self.brain.brain

    @observer.wrap({"type": "processor_bm"})
    async def for_processor_bm(
        self,
        messages: list[BrokerMessage],
    ) -> IndexMessage:
        """
        Builds the index message for the broker messages coming from the processor.
        The processor can index new data to any index.
        """
        assert all(message.source == BrokerMessage.MessageSource.PROCESSOR for message in messages)
        deleted_fields = get_bm_deleted_fields(messages)
        self._apply_field_deletions(self.brain, deleted_fields)
        await self._apply_resource_index_data(self.brain)
        basic = await self.get_basic()
        fields_to_index = get_bm_modified_fields(messages)
        vectorsets_configs = await self.get_vectorsets_configs()
        for fieldid in fields_to_index:
            if fieldid in deleted_fields:
                continue
            await self._apply_field_index_data(
                self.brain,
                fieldid,
                basic,
                texts=needs_texts_update(fieldid, messages),
                paragraphs=needs_paragraphs_update(fieldid, messages),
                relations=needs_relations_update(fieldid, messages),
                vectors=needs_vectors_update(fieldid, messages),
                replace=True,
                vectorset_configs=vectorsets_configs,
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
                self.resource.txn, kbid=self.resource.kb.kbid
            )
        ]
        return vectorset_configs


def get_bm_deleted_fields(
    messages: list[BrokerMessage],
) -> list[FieldID]:
    deleted = []
    for message in messages:
        for field in message.delete_fields:
            if field not in deleted:
                deleted.append(field)
    return deleted


def get_bm_modified_fields(messages: list[BrokerMessage]) -> list[FieldID]:
    message_source = get_messages_source(messages)
    modified = set()
    for message in messages:
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

        if message_source == BrokerMessage.MessageSource.PROCESSOR:
            # Messages with field metadata, extracted text or field vectors need indexing
            for fm in message.field_metadata:
                modified.add((fm.field.field, fm.field.field_type))
            for et in message.extracted_text:
                modified.add((et.field.field, et.field.field_type))
            for fv in message.field_vectors:
                modified.add((fv.field.field, fv.field.field_type))

        if message_source == BrokerMessage.MessageSource.WRITER:
            # Any field that has fieldmetadata annotations should be considered as modified
            # and needs to be reindexed
            if message.HasField("basic"):
                for ufm in message.basic.fieldmetadata:
                    modified.add((ufm.field.field, ufm.field.field_type))
    return [FieldID(field=field, field_type=field_type) for field, field_type in modified]


def get_messages_source(messages: list[BrokerMessage]) -> BrokerMessage.MessageSource.ValueType:
    assert len(set(message.source for message in messages)) == 1
    return messages[0].source


def needs_prefilter_update(messages: list[BrokerMessage]) -> bool:
    return any(message.reindex for message in messages)


def needs_paragraphs_update(field_id: FieldID, messages: list[BrokerMessage]) -> bool:
    return (
        has_paragraph_annotations(field_id, messages)
        or has_new_extracted_text(field_id, messages)
        or has_new_field_metadata(field_id, messages)
    )


def has_paragraph_annotations(field_id: FieldID, messages: list[BrokerMessage]) -> bool:
    for message in messages:
        ufm = next(
            (fm for fm in message.basic.fieldmetadata if fm.field == field_id),
            None,
        )
        if ufm is None:
            continue
        if len(ufm.paragraphs) > 0:
            return True
    return False


def has_new_field_metadata(
    field_id: FieldID,
    messages: list[BrokerMessage],
) -> bool:
    for message in messages:
        for field_metadata in message.field_metadata:
            if field_metadata.field == field_id:
                return True
    return False


def has_new_extracted_text(
    field_id: FieldID,
    messages: list[BrokerMessage],
) -> bool:
    for message in messages:
        for extracted_text in message.extracted_text:
            if extracted_text.field == field_id:
                return True
    return False


def needs_texts_update(
    field_id: FieldID,
    messages: list[BrokerMessage],
) -> bool:
    return has_new_extracted_text(field_id, messages) or has_new_field_metadata(field_id, messages)


def needs_vectors_update(
    field_id: FieldID,
    messages: list[BrokerMessage],
) -> bool:
    for message in messages:
        for field_vectors in message.field_vectors:
            if field_vectors.field == field_id:
                return True
    return False


def needs_relations_update(
    field_id: FieldID,
    messages: list[BrokerMessage],
) -> bool:
    return has_new_field_metadata(field_id, messages) or has_new_extracted_text(field_id, messages)


async def get_resource_index_message(
    resource: Resource,
    reindex: bool = False,
) -> IndexMessage:
    """
    Get the full index message for a resource.
    """
    im_builder = IndexMessageBuilder(resource)
    return await im_builder.full(reindex=reindex)
