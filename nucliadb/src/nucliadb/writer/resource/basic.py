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
from typing import Optional, Union

from fastapi import HTTPException

from nucliadb.common.models_utils import to_proto
from nucliadb.common.models_utils.from_proto import (
    RelationNodeTypeMap,
    RelationTypeMap,
)
from nucliadb.ingest.orm.utils import set_title
from nucliadb.models.internal.processing import ClassificationLabel, PushPayload, PushTextFormat, Text
from nucliadb_models.content_types import GENERIC_MIME_TYPE
from nucliadb_models.file import FileField
from nucliadb_models.link import LinkField
from nucliadb_models.metadata import (
    ParagraphAnnotation,
    QuestionAnswerAnnotation,
)
from nucliadb_models.text import TEXT_FORMAT_TO_MIMETYPE
from nucliadb_models.writer import (
    ComingResourcePayload,
    CreateResourcePayload,
    UpdateResourcePayload,
)
from nucliadb_protos.knowledgebox_pb2 import KnowledgeBoxConfig
from nucliadb_protos.resources_pb2 import (
    Answers,
    Basic,
    Classification,
    ExtractedTextWrapper,
    FieldComputedMetadataWrapper,
    FieldType,
    Metadata,
    Paragraph,
    UserFieldMetadata,
)
from nucliadb_protos.resources_pb2 import ParagraphAnnotation as PBParagraphAnnotation
from nucliadb_protos.resources_pb2 import (
    QuestionAnswerAnnotation as PBQuestionAnswerAnnotation,
)
from nucliadb_protos.utils_pb2 import Relation, RelationNode
from nucliadb_protos.writer_pb2 import BrokerMessage


def parse_basic_modify(bm: BrokerMessage, item: ComingResourcePayload, toprocess: PushPayload):
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

        toprocess.genericfield["summary"] = Text(body=item.summary, format=PushTextFormat.PLAIN)
    if item.thumbnail:
        bm.basic.thumbnail = item.thumbnail
    if item.metadata is not None:
        bm.basic.metadata.metadata.update(item.metadata.metadata)
        if item.metadata.language:
            bm.basic.metadata.language = item.metadata.language
        if item.metadata.languages:
            unique_languages = list(set(item.metadata.languages))
            bm.basic.metadata.languages.extend(unique_languages)

    if item.fieldmetadata is not None:
        for fieldmetadata in item.fieldmetadata:
            userfieldmetadata = UserFieldMetadata()
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

            for qa_annotation in fieldmetadata.question_answers:
                qa_annotation_pb = build_question_answer_annotation_pb(qa_annotation)
                userfieldmetadata.question_answers.append(qa_annotation_pb)

            userfieldmetadata.field.field = fieldmetadata.field.field

            userfieldmetadata.field.field_type = to_proto.field_type(fieldmetadata.field.field_type)

            bm.basic.fieldmetadata.append(userfieldmetadata)

    if item.usermetadata is not None:
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

        relation_node_resource = RelationNode(value=bm.uuid, ntype=RelationNode.NodeType.RESOURCE)
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
                    # XXX: we are not propagating metadata, is this intended?
                )
            )

        # protobuferrs repeated fields don't support assignment so
        # in order to replace relations, we need to clear them first
        bm.user_relations.ClearField("relations")
        bm.user_relations.relations.extend(relations)

    if item.security is not None:
        unique_groups = list(set(item.security.access_groups))
        bm.security.access_groups.extend(unique_groups)

    if item.hidden is not None:
        bm.basic.hidden = item.hidden


def parse_basic_creation(
    bm: BrokerMessage,
    item: CreateResourcePayload,
    toprocess: PushPayload,
    kb_config: Optional[KnowledgeBoxConfig],
):
    bm.basic.created.FromDatetime(datetime.now())

    if item.title is None:
        item.title = compute_title(item, bm.uuid)
    parse_icon_on_create(bm, item)

    parse_basic_modify(bm, item, toprocess)

    if item.hidden is None:
        if kb_config and kb_config.hidden_resources_hide_on_creation:
            bm.basic.hidden = True


def set_status(basic: Basic, item: CreateResourcePayload):
    basic.metadata.status = Metadata.Status.PENDING


def set_status_modify(basic: Basic, item: UpdateResourcePayload):
    basic.metadata.status = Metadata.Status.PENDING


def validate_classifications(paragraph: ParagraphAnnotation):
    classifications = paragraph.classifications
    if len(classifications) == 0:
        raise HTTPException(status_code=422, detail="ensure classifications has at least 1 items")

    unique_classifications = {tuple(cf.model_dump().values()) for cf in classifications}
    if len(unique_classifications) != len(classifications):
        raise HTTPException(status_code=422, detail="Paragraph classifications need to be unique")


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


def build_question_answer_annotation_pb(
    qa_annotation: QuestionAnswerAnnotation,
) -> PBQuestionAnswerAnnotation:
    pb = PBQuestionAnswerAnnotation()
    pb.cancelled_by_user = qa_annotation.cancelled_by_user
    pb.question_answer.question.text = qa_annotation.question_answer.question.text
    if qa_annotation.question_answer.question.language is not None:
        pb.question_answer.question.language = qa_annotation.question_answer.question.language
    pb.question_answer.question.ids_paragraphs.extend(
        qa_annotation.question_answer.question.ids_paragraphs
    )
    for answer_annotation in qa_annotation.question_answer.answers:
        answer = Answers()
        answer.text = answer_annotation.text
        if answer_annotation.language is not None:
            answer.language = answer_annotation.language
        answer.ids_paragraphs.extend(answer_annotation.ids_paragraphs)
        pb.question_answer.answers.append(answer)
    return pb


def parse_user_classifications(
    item: Union[CreateResourcePayload, UpdateResourcePayload],
) -> list[ClassificationLabel]:
    return (
        [
            ClassificationLabel(
                labelset=classification.labelset,
                label=classification.label,
            )
            for classification in item.usermetadata.classifications
            if classification.cancelled_by_user is False
        ]
        if item.usermetadata is not None
        else []
    )
