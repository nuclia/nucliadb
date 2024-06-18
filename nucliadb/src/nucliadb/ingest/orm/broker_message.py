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

from typing import cast

from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.fields.conversation import Conversation
from nucliadb.ingest.fields.file import File
from nucliadb.ingest.fields.link import Link
from nucliadb.ingest.orm.resource import Resource
from nucliadb_protos.resources_pb2 import (
    ExtractedTextWrapper,
    ExtractedVectorsWrapper,
    FieldComputedMetadataWrapper,
    FieldType,
    LargeComputedMetadataWrapper,
)
from nucliadb_protos.writer_pb2 import BrokerMessage


async def generate_broker_message(resource: Resource) -> BrokerMessage:
    """Generate a full broker message from a resource. This means downloading
    all the pointers minus the ones to external files that are not PB. Iterate
    all resource fields and create a BrokerMessage
    """
    builder = _BrokerMessageBuilder()
    bm = await builder.build_from(resource)
    return bm


class _BrokerMessageBuilder:
    def __init__(self):
        self.bm = BrokerMessage()

    async def build_from(self, resource: Resource):
        # clear the state and generate a new broker message
        self.bm.Clear()

        self.bm.kbid = resource.kb.kbid
        self.bm.uuid = resource.uuid
        basic = await resource.get_basic()
        if basic is not None:
            self.bm.basic.CopyFrom(basic)

        self.bm.slug = self.bm.basic.slug
        origin = await resource.get_origin()
        if origin is not None:
            self.bm.origin.CopyFrom(origin)
        relations = await resource.get_relations()
        if relations is not None:
            for relation in relations.relations:
                self.bm.relations.append(relation)

        fields = await resource.get_fields(force=True)
        for (type_id, field_id), field in fields.items():
            # Value
            await self.generate_field(type_id, field_id, field)

            # Extracted text
            await self.generate_extracted_text(type_id, field_id, field)

            # Field Computed Metadata
            await self.generate_field_computed_metadata(type_id, field_id, field)

            if type_id == FieldType.FILE and isinstance(field, File):
                field_extracted_data = await field.get_file_extracted_data()
                if field_extracted_data is not None:
                    self.bm.file_extracted_data.append(field_extracted_data)

            elif type_id == FieldType.LINK and isinstance(field, Link):
                link_extracted_data = await field.get_link_extracted_data()
                if link_extracted_data is not None:
                    self.bm.link_extracted_data.append(link_extracted_data)

            # Field vectors
            await self.generate_field_vectors(type_id, field_id, field)

            # Large metadata
            await self.generate_field_large_computed_metadata(type_id, field_id, field)

        return self.bm

    async def generate_field(
        self,
        type_id: FieldType.ValueType,
        field_id: str,
        field: Field,
    ):
        # Used for exporting a field
        if type_id == FieldType.TEXT:
            value = await field.get_value()
            self.bm.texts[field_id].CopyFrom(value)
        elif type_id == FieldType.LINK:
            value = await field.get_value()
            self.bm.links[field_id].CopyFrom(value)
        elif type_id == FieldType.FILE:
            value = await field.get_value()
            self.bm.files[field_id].CopyFrom(value)
        elif type_id == FieldType.CONVERSATION:
            field = cast(Conversation, field)
            value = await field.get_full_conversation()
            self.bm.conversations[field_id].CopyFrom(value)

    async def generate_extracted_text(
        self,
        type_id: FieldType.ValueType,
        field_id: str,
        field: Field,
    ):
        etw = ExtractedTextWrapper()
        etw.field.field = field_id
        etw.field.field_type = type_id
        extracted_text = await field.get_extracted_text()
        if extracted_text is not None:
            etw.body.CopyFrom(extracted_text)
            self.bm.extracted_text.append(etw)

    async def generate_field_computed_metadata(
        self,
        type_id: FieldType.ValueType,
        field_id: str,
        field: Field,
    ):
        fcmw = FieldComputedMetadataWrapper()
        fcmw.field.field = field_id
        fcmw.field.field_type = type_id

        field_metadata = await field.get_field_metadata()
        if field_metadata is not None:
            fcmw.metadata.CopyFrom(field_metadata)
            fcmw.field.field = field_id
            fcmw.field.field_type = type_id
            self.bm.field_metadata.append(fcmw)
            # Make sure cloud files are removed for exporting

    async def generate_field_vectors(
        self,
        type_id: FieldType.ValueType,
        field_id: str,
        field: Field,
    ):
        vo = await field.get_vectors()
        if vo is None:
            return
        evw = ExtractedVectorsWrapper()
        evw.field.field = field_id
        evw.field.field_type = type_id
        evw.vectors.CopyFrom(vo)
        self.bm.field_vectors.append(evw)

    async def generate_field_large_computed_metadata(
        self,
        type_id: FieldType.ValueType,
        field_id: str,
        field: Field,
    ):
        lcm = await field.get_large_field_metadata()
        if lcm is None:
            return
        lcmw = LargeComputedMetadataWrapper()
        lcmw.field.field = field_id
        lcmw.field.field_type = type_id
        lcmw.real.CopyFrom(lcm)
        self.bm.field_large_metadata.append(lcmw)
