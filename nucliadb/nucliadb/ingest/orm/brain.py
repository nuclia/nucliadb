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
from copy import deepcopy
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple, Union

from google.protobuf.internal.containers import MessageMap
from nucliadb_protos.noderesources_pb2 import IndexParagraph as BrainParagraph
from nucliadb_protos.noderesources_pb2 import ParagraphMetadata
from nucliadb_protos.noderesources_pb2 import Position as TextPosition
from nucliadb_protos.noderesources_pb2 import Resource as PBBrainResource
from nucliadb_protos.noderesources_pb2 import ResourceID
from nucliadb_protos.resources_pb2 import (
    Basic,
    ExtractedText,
    FieldComputedMetadata,
    FieldKeywordset,
    FieldMetadata,
    Metadata,
    Origin,
    Paragraph,
    UserFieldMetadata,
    UserMetadata,
)
from nucliadb_protos.utils_pb2 import (
    Relation,
    RelationNode,
    UserVectorSet,
    UserVectorsList,
    VectorObject,
)

from nucliadb.ingest import logger
from nucliadb.ingest.orm.labels import BASE_TAGS, flat_resource_tags
from nucliadb.ingest.orm.utils import compute_paragraph_key
from nucliadb_models.metadata import ResourceProcessingStatus

if TYPE_CHECKING:  # pragma: no cover
    StatusValue = Union[Metadata.Status.V, int]
else:
    StatusValue = int

FilePagePositions = Dict[int, Tuple[int, int]]


METADATA_STATUS_PB_TYPE_TO_NAME_MAP = {
    Metadata.Status.ERROR: ResourceProcessingStatus.ERROR.name,
    Metadata.Status.PROCESSED: ResourceProcessingStatus.PROCESSED.name,
    Metadata.Status.PENDING: ResourceProcessingStatus.PENDING.name,
    Metadata.Status.BLOCKED: ResourceProcessingStatus.BLOCKED.name,
    Metadata.Status.EXPIRED: ResourceProcessingStatus.EXPIRED.name,
}


class ResourceBrain:
    def __init__(self, rid: str):
        self.rid = rid
        ridobj = ResourceID(uuid=rid)
        self.brain: PBBrainResource = PBBrainResource(resource=ridobj)
        self.tags: Dict[str, List[str]] = deepcopy(BASE_TAGS)

    def apply_field_text(self, field_key: str, text: str):
        self.brain.texts[field_key].text = text

    def apply_field_metadata(
        self,
        field_key: str,
        metadata: FieldComputedMetadata,
        replace_field: List[str],
        replace_splits: Dict[str, List[str]],
        page_positions: Optional[FilePagePositions],
        extracted_text: Optional[ExtractedText],
        basic_user_field_metadata: Optional[UserFieldMetadata] = None,
    ):
        # To check for duplicate paragraphs
        unique_paragraphs: Set[str] = set()

        # Expose also user classes

        if basic_user_field_metadata is not None:
            paragraphs = {
                compute_paragraph_key(self.rid, paragraph.key): paragraph
                for paragraph in basic_user_field_metadata.paragraphs
            }
        else:
            paragraphs = {}

        # We should set paragraphs and labels
        for subfield, metadata_split in metadata.split_metadata.items():
            # For each split of this field
            for index, paragraph in enumerate(metadata_split.paragraphs):
                key = f"{self.rid}/{field_key}/{subfield}/{paragraph.start}-{paragraph.end}"

                denied_classifications = []
                if key in paragraphs:
                    denied_classifications = [
                        f"/l/{classification.labelset}/{classification.label}"
                        for classification in paragraphs[key].classifications
                        if classification.cancelled_by_user is True
                    ]
                position = TextPosition(
                    index=index,
                    start=paragraph.start,
                    end=paragraph.end,
                    start_seconds=paragraph.start_seconds,
                    end_seconds=paragraph.end_seconds,
                )
                if page_positions:
                    position.page_number = get_page_number(
                        paragraph.start, page_positions
                    )
                p = BrainParagraph(
                    start=paragraph.start,
                    end=paragraph.end,
                    field=field_key,
                    split=subfield,
                    index=index,
                    repeated_in_field=is_paragraph_repeated_in_field(
                        paragraph,
                        extracted_text,
                        unique_paragraphs,
                        split=subfield,
                    ),
                    metadata=ParagraphMetadata(position=position),
                )
                for classification in paragraph.classifications:
                    label = f"/l/{classification.labelset}/{classification.label}"
                    if label not in denied_classifications:
                        p.labels.append(label)

                self.brain.paragraphs[field_key].paragraphs[key].CopyFrom(p)

        for index, paragraph in enumerate(metadata.metadata.paragraphs):
            key = f"{self.rid}/{field_key}/{paragraph.start}-{paragraph.end}"
            denied_classifications = []
            if key in paragraphs:
                denied_classifications = [
                    f"/l/{classification.labelset}/{classification.label}"
                    for classification in paragraphs[key].classifications
                    if classification.cancelled_by_user is True
                ]

            position = TextPosition(
                index=index,
                start=paragraph.start,
                end=paragraph.end,
                start_seconds=paragraph.start_seconds,
                end_seconds=paragraph.end_seconds,
            )
            if page_positions:
                position.page_number = get_page_number(paragraph.start, page_positions)
            p = BrainParagraph(
                start=paragraph.start,
                end=paragraph.end,
                field=field_key,
                index=index,
                repeated_in_field=is_paragraph_repeated_in_field(
                    paragraph, extracted_text, unique_paragraphs
                ),
                metadata=ParagraphMetadata(position=position),
            )
            for classification in paragraph.classifications:
                label = f"/l/{classification.labelset}/{classification.label}"
                if label not in denied_classifications:
                    p.labels.append(label)

            self.brain.paragraphs[field_key].paragraphs[key].CopyFrom(p)

        for relations in metadata.metadata.relations:
            for relation in relations.relations:
                self.brain.relations.append(relation)

        for split, sentences in replace_splits.items():
            for sentence in sentences:
                self.brain.paragraphs_to_delete.append(
                    f"{self.rid}/{field_key}/{split}/{sentence}"
                )

        for sentence_to_delete in replace_field:
            self.brain.paragraphs_to_delete.append(
                f"{self.rid}/{field_key}/{sentence_to_delete}"
            )

    def delete_metadata(self, field_key: str, metadata: FieldComputedMetadata):
        for subfield, metadata_split in metadata.split_metadata.items():
            for paragraph in metadata_split.paragraphs:
                self.brain.paragraphs_to_delete.append(
                    f"{self.rid}/{field_key}/{subfield}/{paragraph.start}-{paragraph.end}"
                )

        for paragraph in metadata.metadata.paragraphs:
            self.brain.sentences_to_delete.append(
                f"{self.rid}/{field_key}/{paragraph.start}-{paragraph.end}"
            )

    def apply_user_vectors(
        self,
        field_key: str,
        user_vectors: UserVectorSet,
        vectors_to_delete: MessageMap[str, UserVectorsList],
    ):
        for vectorset, vectors in user_vectors.vectors.items():
            for vector_id, user_vector in vectors.vectors.items():
                self.brain.vectors[vectorset].vectors[
                    f"{self.rid}/{field_key}/{vector_id}/{user_vector.start}-{user_vector.end}"
                ].CopyFrom(user_vector)

        for vectorset, vectorslist in vectors_to_delete.items():
            for vector in vectorslist.vectors:
                self.brain.vectors_to_delete[vectorset].vectors.append(
                    f"{self.rid}/{field_key}/{vector}"
                )

    def apply_field_vectors(
        self,
        field_key: str,
        vo: VectorObject,
        replace_field: bool,
        replace_splits: List[str],
    ):
        for subfield, vectors in vo.split_vectors.items():
            # For each split of this field

            for index, vector in enumerate(vectors.vectors):
                sparagraph = self.brain.paragraphs[field_key].paragraphs[
                    f"{self.rid}/{field_key}/{subfield}/{vector.start_paragraph}-{vector.end_paragraph}"
                ]
                ssentence = sparagraph.sentences[
                    f"{self.rid}/{field_key}/{subfield}/{index}/{vector.start}-{vector.end}"
                ]

                ssentence.ClearField("vector")  # clear first to prevent duplicates
                ssentence.vector.extend(vector.vector)

        for index, vector in enumerate(vo.vectors.vectors):
            para_key = f"{self.rid}/{field_key}/{vector.start_paragraph}-{vector.end_paragraph}"
            paragraph = self.brain.paragraphs[field_key].paragraphs[para_key]
            sent_key = f"{self.rid}/{field_key}/{index}/{vector.start}-{vector.end}"
            sentence = paragraph.sentences[sent_key]

            sentence.ClearField("vector")  # clear first to prevent duplicates
            sentence.vector.extend(vector.vector)

            sentence.metadata.position.start = vector.start
            sentence.metadata.position.end = vector.end

            # does it make sense to copy forward paragraph values here?
            sentence.metadata.position.page_number = (
                paragraph.metadata.position.page_number
            )
            sentence.metadata.position.index = paragraph.metadata.position.index

        for split in replace_splits:
            self.brain.sentences_to_delete.append(f"{self.rid}/{field_key}/{split}")

        if replace_field:
            self.brain.sentences_to_delete.append(f"{self.rid}/{field_key}")

    def delete_vectors(self, field_key: str, vo: VectorObject):
        for subfield, vectors in vo.split_vectors.items():
            for vector in vectors.vectors:
                self.brain.sentences_to_delete.append(
                    f"{self.rid}/{field_key}/{subfield}/{vector.start}-{vector.end}"
                )

        for vector in vo.vectors.vectors:
            self.brain.sentences_to_delete.append(
                f"{self.rid}/{field_key}/{vector.start}-{vector.end}"
            )

    def set_processing_status(
        self, basic: Basic, previous_status: Optional[Metadata.Status.ValueType]
    ):
        # The value of brain.status will either be PROCESSED or PENDING
        status = basic.metadata.status
        if previous_status is not None and previous_status != Metadata.Status.PENDING:
            # Already processed once, so it stays as PROCESSED
            self.brain.status = PBBrainResource.PROCESSED
            return
        # previos_status is None or PENDING
        if status == Metadata.Status.PENDING:
            # Stays in pending
            self.brain.status = PBBrainResource.PENDING
        else:
            # Means it has just been processed
            self.brain.status = PBBrainResource.PROCESSED

    def get_processing_status_tag(self, metadata: Metadata) -> str:
        if not metadata.useful:
            return "EMPTY"
        return METADATA_STATUS_PB_TYPE_TO_NAME_MAP[metadata.status]

    def set_global_tags(self, basic: Basic, uuid: str, origin: Optional[Origin]):
        self.brain.metadata.created.CopyFrom(basic.created)
        self.brain.metadata.modified.CopyFrom(basic.modified)

        relationnodedocument = RelationNode(
            value=uuid, ntype=RelationNode.NodeType.RESOURCE
        )
        if origin is not None:
            if origin.source_id:
                self.tags["o"] = [origin.source_id]
            # origin tags
            for tag in origin.tags:
                self.tags["t"].append(tag)
            # origin source
            if origin.source_id != "":
                self.tags["u"].append(f"s/{origin.source_id}")

            # origin contributors
            for contrib in origin.colaborators:
                self.tags["u"].append(f"o/{contrib}")
                relationnodeuser = RelationNode(
                    value=contrib, ntype=RelationNode.NodeType.USER
                )
                self.brain.relations.append(
                    Relation(
                        relation=Relation.COLAB,
                        source=relationnodedocument,
                        to=relationnodeuser,
                    )
                )

        # icon
        self.tags["n"].append(f"i/{basic.icon}")

        # processing status
        status_tag = self.get_processing_status_tag(basic.metadata)
        self.tags["n"].append(f"s/{status_tag}")

        # main language
        if basic.metadata.language != "":
            self.tags["s"].append(f"p/{basic.metadata.language}")

        # all language
        for lang in basic.metadata.languages:
            self.tags["s"].append(f"s/{lang}")

        # labels
        for classification in basic.usermetadata.classifications:
            self.tags["l"].append(f"{classification.labelset}/{classification.label}")
            relation_node_label = RelationNode(
                value=f"{classification.labelset}/{classification.label}",
                ntype=RelationNode.NodeType.LABEL,
            )
            self.brain.relations.append(
                Relation(
                    relation=Relation.ABOUT,
                    source=relationnodedocument,
                    to=relation_node_label,
                )
            )

        # relations
        self.brain.relations.extend(basic.usermetadata.relations)

        self.compute_tags()

    def process_meta(
        self,
        field_key: str,
        metadata: FieldMetadata,
        tags: Dict[str, List[str]],
        relation_node_document: RelationNode,
        user_canceled_labels: List[str],
    ):
        for classification in metadata.classifications:
            label = f"{classification.labelset}/{classification.label}"
            if label not in user_canceled_labels:
                tags["l"].append(label)
                relation_node_label = RelationNode(
                    value=label,
                    ntype=RelationNode.NodeType.LABEL,
                )
                self.brain.relations.append(
                    Relation(
                        relation=Relation.ABOUT,
                        source=relation_node_document,
                        to=relation_node_label,
                    )
                )

        for klass_entity, _ in metadata.positions.items():
            tags["e"].append(klass_entity)
            entity_array = klass_entity.split("/")
            if len(entity_array) == 1:
                raise AttributeError(f"Entity should be with type {klass_entity}")
            elif len(entity_array) > 1:
                klass = entity_array[0]
                entity = "/".join(entity_array[1:])
            relation_node_entity = RelationNode(
                value=entity, ntype=RelationNode.NodeType.ENTITY, subtype=klass
            )
            rel = Relation(
                relation=Relation.ENTITY,
                source=relation_node_document,
                to=relation_node_entity,
            )
            self.brain.relations.append(rel)

    def process_keywordset_fields(self, field_key: str, field: FieldKeywordset):
        # all field keywords
        if field:
            for keyword in field.keywords:
                self.tags["f"].append(f"{field_key}/{keyword.value}")
                self.tags["fg"].append(keyword.value)

    def apply_field_tags_globally(
        self,
        field_key: str,
        metadata: Optional[FieldComputedMetadata],
        uuid: str,
        basic_user_metadata: Optional[UserMetadata] = None,
        basic_user_fieldmetadata: Optional[UserFieldMetadata] = None,
    ):
        if basic_user_metadata is not None:
            user_canceled_labels = [
                f"/l/{classification.labelset}/{classification.label}"
                for classification in basic_user_metadata.classifications
                if classification.cancelled_by_user
            ]
        else:
            user_canceled_labels = []

        relation_node_resource = RelationNode(
            value=uuid, ntype=RelationNode.NodeType.RESOURCE
        )
        tags: Dict[str, List[str]] = {"l": [], "e": []}
        if metadata is not None:
            for meta in metadata.split_metadata.values():
                self.process_meta(
                    field_key, meta, tags, relation_node_resource, user_canceled_labels
                )
            self.process_meta(
                field_key,
                metadata.metadata,
                tags,
                relation_node_resource,
                user_canceled_labels,
            )

        if basic_user_fieldmetadata is not None:
            for token in basic_user_fieldmetadata.token:
                if token.cancelled_by_user is False:
                    tags["e"].append(f"{token.klass}/{token.token}")
                    relation_node_entity = RelationNode(
                        value=token.token,
                        ntype=RelationNode.NodeType.ENTITY,
                        subtype=token.klass,
                    )
                    rel = Relation(
                        relation=Relation.ENTITY,
                        source=relation_node_resource,
                        to=relation_node_entity,
                    )
                    self.brain.relations.append(rel)
            for paragraph_annotation in basic_user_fieldmetadata.paragraphs:
                for classification in paragraph_annotation.classifications:
                    if not classification.cancelled_by_user:
                        self.brain.paragraphs[field_key].paragraphs[
                            paragraph_annotation.key
                        ].labels.append(
                            f"/l/{classification.labelset}/{classification.label}"
                        )
        self.brain.texts[field_key].labels.extend(flat_resource_tags(tags))

    def compute_tags(self):
        self.brain.labels.extend(flat_resource_tags(self.tags))


def get_paragraph_text(
    extracted_text: ExtractedText, start: int, end: int, split: Optional[str] = None
) -> str:
    if split is not None:
        text = extracted_text.split_text[split]
    else:
        text = extracted_text.text
    return text[start:end]


def is_paragraph_repeated_in_field(
    paragraph: Paragraph,
    extracted_text: Optional[ExtractedText],
    unique_paragraphs: Set[str],
    split: Optional[str] = None,
) -> bool:
    if extracted_text is None:
        return False

    paragraph_text = get_paragraph_text(
        extracted_text, start=paragraph.start, end=paragraph.end, split=split
    )
    if len(paragraph_text) == 0:
        return False

    if paragraph_text in unique_paragraphs:
        repeated_in_field = True
    else:
        repeated_in_field = False
        unique_paragraphs.add(paragraph_text)
    return repeated_in_field


def get_page_number(start_index: int, page_positions: FilePagePositions) -> int:
    page_number = 0
    for page_number, (page_start, page_end) in page_positions.items():
        if page_start <= start_index <= page_end:
            return int(page_number)
        if start_index <= page_end:
            logger.info("There is a wrong page start")
            return int(page_number)
    logger.error("Could not found a page")
    return int(page_number)
