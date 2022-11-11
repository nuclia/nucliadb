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
    FieldComputedMetadataWrapper,
    FieldType,
    Metadata,
    Paragraph,
    ParagraphAnnotation,
    TokenSplit,
    UserFieldMetadata,
)
from nucliadb_protos.utils_pb2 import Relation, RelationNode
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb.ingest.orm.utils import set_title
from nucliadb.ingest.processing import PushPayload
from nucliadb.models import RelationType
from nucliadb.models.common import FIELD_TYPES_MAP_REVERSE
from nucliadb.models.text import PushTextFormat, Text
from nucliadb.models.writer import (
    ComminResourcePayload,
    CreateResourcePayload,
    UpdateResourcePayload,
)


def parse_basic_modify(
    bm: BrokerMessage, item: ComminResourcePayload, toprocess: PushPayload
):
    bm.basic.modified.FromDatetime(datetime.now())
    if item.title:
        set_title(bm, toprocess, item.title)
    if item.summary:
        bm.basic.summary = item.summary
        etw = ExtractedTextWrapper()
        etw.field.field = "summary"
        etw.field.field_type = FieldType.GENERIC
        etw.body.text = item.summary
        bm.extracted_text.append(etw)
        fmw = FieldComputedMetadataWrapper()
        basic_paragraph = Paragraph(
            start=0, end=len(item.summary), kind=Paragraph.TypeParagraph.DESCRIPTION
        )
        fmw.metadata.metadata.paragraphs.append(basic_paragraph)
        fmw.field.field = "summary"
        fmw.field.field_type = FieldType.GENERIC
        bm.field_metadata.append(fmw)
        bm.basic.metadata.useful = True
        bm.basic.metadata.status = Metadata.Status.PENDING

        toprocess.genericfield["summary"] = Text(
            body=item.summary, format=PushTextFormat.PLAIN
        )
    if item.thumbnail:
        bm.basic.thumbnail = item.thumbnail
    if item.layout:
        bm.basic.layout = item.layout
    if item.icon:
        bm.basic.icon = item.icon
    if item.fieldmetadata is not None:
        for fieldmetadata in item.fieldmetadata:
            userfieldmetadata = UserFieldMetadata()
            for token in fieldmetadata.token:
                userfieldmetadata.token.append(
                    TokenSplit(
                        token=token.token,
                        klass=token.klass,
                        start=token.start,
                        end=token.end,
                    )
                )
            for paragraph in fieldmetadata.paragraphs:
                paragraphpb = ParagraphAnnotation(key=paragraph.key)
                for classification in paragraph.classifications:
                    paragraphpb.classifications.append(
                        Classification(
                            labelset=classification.labelset,
                            label=classification.label,
                        )
                    )
                userfieldmetadata.paragraphs.append(paragraphpb)

            userfieldmetadata.field.field = fieldmetadata.field.field
            userfieldmetadata.field.field_type = FIELD_TYPES_MAP_REVERSE[  # type: ignore
                fieldmetadata.field.field_type.value
            ]

            bm.basic.fieldmetadata.append(userfieldmetadata)

    if item.usermetadata is not None:
        # protobuferrs repeated fields don't support assignment
        # will allways be a clean basic

        bm.basic.usermetadata.classifications.extend(
            [
                Classification(labelset=x.labelset, label=x.label)
                for x in item.usermetadata.classifications
            ]
        )

        relation_node_document = RelationNode(
            value=bm.uuid, ntype=RelationNode.NodeType.RESOURCE
        )
        relations = []
        for relation in item.usermetadata.relations:
            if relation.relation == RelationType.CHILD and relation.resource:
                relation_node_document_child = RelationNode(
                    value=relation.resource, ntype=RelationNode.NodeType.RESOURCE
                )
                relations.append(
                    Relation(
                        relation=Relation.RelationType.CHILD,
                        source=relation_node_document,
                        to=relation_node_document_child,
                    )
                )

            if relation.relation == RelationType.ABOUT and relation.label:
                relation_node_label = RelationNode(
                    value=relation.label, ntype=RelationNode.NodeType.LABEL
                )
                relations.append(
                    Relation(
                        relation=Relation.RelationType.ABOUT,
                        source=relation_node_document,
                        to=relation_node_label,
                    )
                )

            if relation.relation == RelationType.ENTITY and relation.entity:
                relation_node_entity = RelationNode(
                    value=relation.entity.entity,
                    ntype=RelationNode.NodeType.ENTITY,
                    subtype=relation.entity.entity_type,
                )
                relations.append(
                    Relation(
                        relation=Relation.RelationType.ENTITY,
                        source=relation_node_document,
                        to=relation_node_entity,
                    )
                )

            if relation.relation == RelationType.COLAB and relation.user:
                relation_node_user = RelationNode(
                    value=relation.user, ntype=RelationNode.NodeType.USER
                )
                relations.append(
                    Relation(
                        relation=Relation.RelationType.COLAB,
                        source=relation_node_document,
                        to=relation_node_user,
                    )
                )

            if relation.relation == RelationType.OTHER and relation.other:
                relation_node_other = RelationNode(
                    value=relation.other, ntype=RelationNode.NodeType.RESOURCE
                )
                relations.append(
                    Relation(
                        relation=Relation.RelationType.OTHER,
                        source=relation_node_document,
                        to=relation_node_other,
                    )
                )

        # protobuferrs repeated fields don't support assignment so
        # in order to replace relations, we need to clear them first

        bm.basic.usermetadata.ClearField("relations")
        bm.basic.usermetadata.relations.extend(relations)


def parse_basic(bm: BrokerMessage, item: CreateResourcePayload, toprocess: PushPayload):

    bm.basic.created.FromDatetime(datetime.now())

    if item.metadata is not None:
        bm.basic.metadata.Clear()
        bm.basic.metadata.metadata.update(item.metadata.metadata)
        if item.metadata.language:
            bm.basic.metadata.language = item.metadata.language
        if item.metadata.languages:
            bm.basic.metadata.languages.extend(item.metadata.languages)
        # basic.metadata.useful = item.metadata.useful
        # basic.metadata.status = item.metadata.status
    parse_basic_modify(bm, item, toprocess)


def set_status(basic: Basic, item: CreateResourcePayload):
    basic.metadata.status = Metadata.Status.PENDING


def set_status_modify(basic: Basic, item: UpdateResourcePayload):
    basic.metadata.status = Metadata.Status.PENDING


def set_last_seqid(bm: BrokerMessage, seqid: int):
    bm.basic.last_seqid = seqid
