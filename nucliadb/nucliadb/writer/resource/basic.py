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

from fastapi import HTTPException
from nucliadb_protos.resources_pb2 import (
    Basic,
    Classification,
    ExtractedTextWrapper,
    FieldComputedMetadataWrapper,
    FieldType,
    Metadata,
    PageSelections,
    Paragraph,
)
from nucliadb_protos.resources_pb2 import ParagraphAnnotation as PBParagraphAnnotation
from nucliadb_protos.resources_pb2 import TokenSplit, UserFieldMetadata, VisualSelection
from nucliadb_protos.utils_pb2 import Relation, RelationNode
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb.ingest.orm.utils import set_title
from nucliadb.ingest.processing import ProcessingInfo, PushPayload
from nucliadb_models.common import FIELD_TYPES_MAP_REVERSE
from nucliadb_models.file import FileField
from nucliadb_models.link import LinkField
from nucliadb_models.metadata import (
    ParagraphAnnotation,
    RelationNodeTypeMap,
    RelationTypeMap,
)
from nucliadb_models.text import TEXT_FORMAT_TO_MIMETYPE, PushTextFormat, Text
from nucliadb_models.writer import (
    GENERIC_MIME_TYPE,
    ComingResourcePayload,
    CreateResourcePayload,
    UpdateResourcePayload,
)


def parse_basic_modify(
    bm: BrokerMessage, item: ComingResourcePayload, toprocess: PushPayload
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

    if item.metadata is not None:
        bm.basic.metadata.metadata.update(item.metadata.metadata)
        if item.metadata.language:
            bm.basic.metadata.language = item.metadata.language
        if item.metadata.languages:
            bm.basic.metadata.languages.extend(item.metadata.languages)

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
                        cancelled_by_user=token.cancelled_by_user,
                    )
                )
            for paragraph in fieldmetadata.paragraphs:
                validate_classifications(paragraph)
                paragraphpb = PBParagraphAnnotation(key=paragraph.key)
                for classification in paragraph.classifications:
                    paragraphpb.classifications.append(
                        Classification(
                            labelset=classification.labelset,
                            label=classification.label,
                            cancelled_by_user=classification.cancelled_by_user,
                        )
                    )
                userfieldmetadata.paragraphs.append(paragraphpb)

            for page_selections in fieldmetadata.selections:
                page_selections_pb = PageSelections()
                page_selections_pb.page = page_selections.page
                page_selections_pb.visual.extend(
                    [
                        VisualSelection(
                            label=visual_selection.label,
                            top=visual_selection.top,
                            left=visual_selection.left,
                            right=visual_selection.right,
                            bottom=visual_selection.bottom,
                            token_ids=visual_selection.token_ids,
                        )
                        for visual_selection in page_selections.visual
                    ]
                )
                userfieldmetadata.page_selections.append(page_selections_pb)

            userfieldmetadata.field.field = fieldmetadata.field.field
            userfieldmetadata.field.field_type = FIELD_TYPES_MAP_REVERSE[  # type: ignore
                fieldmetadata.field.field_type.value
            ]

            bm.basic.fieldmetadata.append(userfieldmetadata)

    if item.usermetadata is not None:
        # user metadata like labels need to be applied to all
        # paragraphs in a resource and that means this message should
        # force reindexing everything
        # XXX This is not ideal. Long term, we should handle label
        # indexes differently so this is not required
        bm.reindex = True

        # protobufers repeated fields don't support assignment
        # will allways be a clean basic
        bm.basic.usermetadata.classifications.extend(
            [
                Classification(
                    labelset=x.labelset,
                    label=x.label,
                    cancelled_by_user=x.cancelled_by_user,
                )
                for x in item.usermetadata.classifications
            ]
        )

        relation_node_resource = RelationNode(
            value=bm.uuid, ntype=RelationNode.NodeType.RESOURCE
        )
        relations = []
        for relation in item.usermetadata.relations:
            if relation.from_ is None:
                relation_node_from = relation_node_resource
            else:
                relation_node_from = RelationNode(
                    value=relation.from_.value,
                    ntype=RelationNodeTypeMap[relation.from_.type],
                    subtype=relation.from_.group or "",
                )

            relation_node_to = RelationNode(
                value=relation.to.value,
                ntype=RelationNodeTypeMap[relation.to.type],
                subtype=relation.to.group or "",
            )

            relations.append(
                Relation(
                    relation=RelationTypeMap[relation.relation],
                    source=relation_node_from,
                    to=relation_node_to,
                    relation_label=relation.label or "",
                )
            )

        # protobuferrs repeated fields don't support assignment so
        # in order to replace relations, we need to clear them first
        bm.basic.usermetadata.ClearField("relations")
        bm.basic.usermetadata.relations.extend(relations)


def parse_basic(bm: BrokerMessage, item: CreateResourcePayload, toprocess: PushPayload):
    bm.basic.created.FromDatetime(datetime.now())

    if item.title is None:
        item.title = compute_title(item, bm.uuid)
    parse_icon_on_create(bm, item)

    parse_basic_modify(bm, item, toprocess)


def set_status(basic: Basic, item: CreateResourcePayload):
    basic.metadata.status = Metadata.Status.PENDING


def set_status_modify(basic: Basic, item: UpdateResourcePayload):
    basic.metadata.status = Metadata.Status.PENDING


def set_processing_info(bm: BrokerMessage, processing_info: ProcessingInfo):
    bm.basic.last_seqid = processing_info.seqid
    if processing_info.account_seq is not None:
        bm.basic.last_account_seq = processing_info.account_seq
    bm.basic.queue = bm.basic.QueueType.Value(processing_info.queue.name)


def validate_classifications(paragraph: ParagraphAnnotation):
    classifications = paragraph.classifications
    if len(classifications) == 0:
        raise HTTPException(
            status_code=422, detail="ensure classifications has at least 1 items"
        )

    unique_classifications = {tuple(cf.dict().values()) for cf in classifications}
    if len(unique_classifications) != len(classifications):
        raise HTTPException(
            status_code=422, detail="Paragraph classifications need to be unique"
        )


def compute_title(item: CreateResourcePayload, rid: str) -> str:
    link_field: LinkField
    for link_field in item.links.values():
        return link_field.uri

    file_field: FileField
    for file_field in item.files.values():
        if file_field.file.filename:
            return file_field.file.filename

    return item.slug or rid


def parse_icon_on_create(bm: BrokerMessage, item: CreateResourcePayload):
    if item.icon:
        # User input icon takes precedence
        bm.basic.icon = item.icon
        return

    icon = GENERIC_MIME_TYPE
    if len(item.texts) > 0:
        # Infer icon from text file format
        format = next(iter(item.texts.values())).format
        icon = TEXT_FORMAT_TO_MIMETYPE[format]
    item.icon = icon
    bm.basic.icon = icon
