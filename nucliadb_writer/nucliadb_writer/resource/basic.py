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
from datetime import datetime

from nucliadb_protos.resources_pb2 import (
    Basic,
    Classification,
    ExtractedTextWrapper,
    FieldType,
    Metadata,
)
from nucliadb_protos.utils_pb2 import Relation
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb_models import RelationType
from nucliadb_models.text import PushTextFormat, Text
from nucliadb_writer.api.models import (
    ComminResourcePayload,
    CreateResourcePayload,
    UpdateResourcePayload,
)
from nucliadb_writer.processing import PushPayload


def parse_basic_modify(
    bm: BrokerMessage, item: ComminResourcePayload, toprocess: PushPayload
):
    bm.basic.modified.FromDatetime(datetime.now())
    if item.title:
        bm.basic.title = item.title
        etw = ExtractedTextWrapper()
        etw.field.field = "title"
        etw.field.field_type = FieldType.GENERIC
        etw.body.text = item.title
        bm.extracted_text.append(etw)
        toprocess.genericfield["title"] = Text(
            body=item.title, format=PushTextFormat.PLAIN
        )
    if item.summary:
        bm.basic.summary = item.summary
        etw = ExtractedTextWrapper()
        etw.field.field = "summary"
        etw.field.field_type = FieldType.GENERIC
        etw.body.text = item.summary
        bm.extracted_text.append(etw)
        toprocess.genericfield["summary"] = Text(
            body=item.summary, format=PushTextFormat.PLAIN
        )
    if item.layout:
        bm.basic.layout = item.layout
    if item.icon:
        bm.basic.icon = item.icon
    if item.usermetadata is not None:
        bm.basic.usermetadata.classifications.extend(
            [
                Classification(labelset=x.labelset, label=x.label)
                for x in item.usermetadata.classifications
            ]
        )

        relations = []
        for relation in item.usermetadata.relations:
            relation_type = Relation.RelationType.Value(relation.relation.name)
            if relation.relation == RelationType.CHILD and relation.resource:
                relations.append(
                    Relation(
                        relation=relation_type,
                        resource=relation.resource,
                    )
                )
            if relation.relation == RelationType.ABOUT and relation.label:
                relations.append(
                    Relation(
                        relation=relation_type,
                        label=relation.label,
                    )
                )

            if relation.relation == RelationType.ENTITY and relation.entity:
                rel = Relation(
                    relation=relation_type,
                )
                rel.entity.entity = relation.entity.entity
                rel.entity.entity_type = relation.entity.entity_type
                relations.append(rel)

            if relation.relation == RelationType.COLAB and relation.user:
                relations.append(
                    Relation(
                        relation=relation_type,
                        user=relation.user,
                    )
                )

            if relation.relation == RelationType.OTHER and relation.other:
                relations.append(
                    Relation(
                        relation=relation_type,
                        other=relation.other,
                    )
                )

        bm.basic.usermetadata.relations.extend(relations)


def parse_basic(bm: BrokerMessage, item: CreateResourcePayload, toprocess: PushPayload):

    bm.basic.created.FromDatetime(datetime.now())

    if item.metadata is not None:
        bm.basic.metadata.Clear()
        bm.basic.metadata.metadata.update(item.metadata.metadata)
        if item.metadata.language:
            bm.basic.metadata.language = item.metadata.language
        bm.basic.metadata.languages.extend(item.metadata.languages)
        # basic.metadata.useful = item.metadata.useful
        # basic.metadata.status = item.metadata.status
    parse_basic_modify(bm, item, toprocess)


def set_status(basic: Basic, item: CreateResourcePayload):
    basic.metadata.status = Metadata.Status.PENDING


def set_status_modify(basic: Basic, item: UpdateResourcePayload):
    basic.metadata.status = Metadata.Status.PENDING
