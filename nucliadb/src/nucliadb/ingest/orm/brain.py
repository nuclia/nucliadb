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
import logging
from copy import deepcopy
from dataclasses import dataclass
from typing import Optional

from nucliadb.common import ids
from nucliadb.ingest import logger
from nucliadb.ingest.orm.utils import compute_paragraph_key
from nucliadb_models.labels import BASE_LABELS, flatten_resource_labels
from nucliadb_models.metadata import ResourceProcessingStatus
from nucliadb_protos import utils_pb2
from nucliadb_protos.noderesources_pb2 import IndexParagraph as BrainParagraph
from nucliadb_protos.noderesources_pb2 import ParagraphMetadata, Representation, ResourceID
from nucliadb_protos.noderesources_pb2 import Position as TextPosition
from nucliadb_protos.noderesources_pb2 import Resource as PBBrainResource
from nucliadb_protos.resources_pb2 import (
    Basic,
    ExtractedText,
    FieldComputedMetadata,
    FieldMetadata,
    Metadata,
    Origin,
    Paragraph,
    UserFieldMetadata,
    UserMetadata,
)
from nucliadb_protos.utils_pb2 import Relation, RelationNode, VectorObject

FilePagePositions = dict[int, tuple[int, int]]

METADATA_STATUS_PB_TYPE_TO_NAME_MAP = {
    Metadata.Status.ERROR: ResourceProcessingStatus.ERROR.name,
    Metadata.Status.PROCESSED: ResourceProcessingStatus.PROCESSED.name,
    Metadata.Status.PENDING: ResourceProcessingStatus.PENDING.name,
    Metadata.Status.BLOCKED: ResourceProcessingStatus.BLOCKED.name,
    Metadata.Status.EXPIRED: ResourceProcessingStatus.EXPIRED.name,
}


@dataclass
class ParagraphClassifications:
    valid: dict[str, list[str]]
    denied: dict[str, list[str]]


class ResourceBrain:
    def __init__(self, rid: str):
        self.rid = rid
        ridobj = ResourceID(uuid=rid)
        self.brain: PBBrainResource = PBBrainResource(resource=ridobj)
        self.labels: dict[str, list[str]] = deepcopy(BASE_LABELS)

    def apply_field_text(self, field_key: str, text: str):
        self.brain.texts[field_key].text = text

    def _get_paragraph_user_classifications(
        self, basic_user_field_metadata: Optional[UserFieldMetadata]
    ) -> ParagraphClassifications:
        pc = ParagraphClassifications(valid={}, denied={})
        if basic_user_field_metadata is None:
            return pc
        for annotated_paragraph in basic_user_field_metadata.paragraphs:
            for classification in annotated_paragraph.classifications:
                paragraph_key = compute_paragraph_key(self.rid, annotated_paragraph.key)
                classif_label = f"/l/{classification.labelset}/{classification.label}"
                if classification.cancelled_by_user:
                    pc.denied.setdefault(paragraph_key, []).append(classif_label)
                else:
                    pc.valid.setdefault(paragraph_key, []).append(classif_label)
        return pc

    def apply_field_metadata(
        self,
        field_key: str,
        metadata: FieldComputedMetadata,
        paragraphs_to_replace: list[str],
        replace_splits: dict[str, list[str]],
        page_positions: Optional[FilePagePositions],
        extracted_text: Optional[ExtractedText],
        basic_user_field_metadata: Optional[UserFieldMetadata] = None,
    ):
        # To check for duplicate paragraphs
        unique_paragraphs: set[str] = set()

        # Expose also user classifications
        paragraph_classifications = self._get_paragraph_user_classifications(basic_user_field_metadata)

        # We should set paragraphs and labels
        paragraph_pages = ParagraphPages(page_positions) if page_positions else None
        for subfield, metadata_split in metadata.split_metadata.items():
            # For each split of this field
            for index, paragraph in enumerate(metadata_split.paragraphs):
                key = f"{self.rid}/{field_key}/{subfield}/{paragraph.start}-{paragraph.end}"

                denied_classifications = paragraph_classifications.denied.get(key, [])
                position = TextPosition(
                    index=index,
                    start=paragraph.start,
                    end=paragraph.end,
                    start_seconds=paragraph.start_seconds,
                    end_seconds=paragraph.end_seconds,
                )
                page_with_visual = False
                if paragraph.HasField("page"):
                    position.page_number = paragraph.page.page
                    page_with_visual = paragraph.page.page_with_visual
                    position.in_page = True
                elif paragraph_pages:
                    position.page_number = paragraph_pages.get(paragraph.start)
                    position.in_page = True
                else:
                    position.in_page = False

                representation = Representation()
                if paragraph.HasField("representation"):
                    representation.file = paragraph.representation.reference_file
                    representation.is_a_table = paragraph.representation.is_a_table

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
                    metadata=ParagraphMetadata(
                        position=position,
                        page_with_visual=page_with_visual,
                        representation=representation,
                    ),
                )
                p.labels.append(f"/k/{Paragraph.TypeParagraph.Name(paragraph.kind).lower()}")
                for classification in paragraph.classifications:
                    label = f"/l/{classification.labelset}/{classification.label}"
                    if label not in denied_classifications:
                        p.labels.append(label)

                # Add user annotated labels to paragraphs
                extend_unique(p.labels, paragraph_classifications.valid.get(key, []))  # type: ignore

                self.brain.paragraphs[field_key].paragraphs[key].CopyFrom(p)

        for index, paragraph in enumerate(metadata.metadata.paragraphs):
            key = f"{self.rid}/{field_key}/{paragraph.start}-{paragraph.end}"
            denied_classifications = paragraph_classifications.denied.get(key, [])
            position = TextPosition(
                index=index,
                start=paragraph.start,
                end=paragraph.end,
                start_seconds=paragraph.start_seconds,
                end_seconds=paragraph.end_seconds,
            )
            page_with_visual = False
            if paragraph.HasField("page"):
                position.page_number = paragraph.page.page
                position.in_page = True
                page_with_visual = paragraph.page.page_with_visual
            elif paragraph_pages:
                position.page_number = paragraph_pages.get(paragraph.start)
                position.in_page = True
            else:
                position.in_page = False

            representation = Representation()
            if paragraph.HasField("representation"):
                representation.file = paragraph.representation.reference_file
                representation.is_a_table = paragraph.representation.is_a_table

            p = BrainParagraph(
                start=paragraph.start,
                end=paragraph.end,
                field=field_key,
                index=index,
                repeated_in_field=is_paragraph_repeated_in_field(
                    paragraph, extracted_text, unique_paragraphs
                ),
                metadata=ParagraphMetadata(
                    position=position,
                    page_with_visual=page_with_visual,
                    representation=representation,
                ),
            )
            p.labels.append(f"/k/{Paragraph.TypeParagraph.Name(paragraph.kind).lower()}")

            for classification in paragraph.classifications:
                label = f"/l/{classification.labelset}/{classification.label}"
                if label not in denied_classifications:
                    p.labels.append(label)

            # Add user annotated labels to paragraphs
            extend_unique(p.labels, paragraph_classifications.valid.get(key, []))  # type: ignore

            self.brain.paragraphs[field_key].paragraphs[key].CopyFrom(p)

        for relations in metadata.metadata.relations:
            for relation in relations.relations:
                self.brain.relations.append(relation)

        for split, sentences in replace_splits.items():
            for sentence in sentences:
                self.brain.paragraphs_to_delete.append(f"{self.rid}/{field_key}/{split}/{sentence}")

        for paragraph_to_delete in paragraphs_to_replace:
            self.brain.paragraphs_to_delete.append(f"{self.rid}/{field_key}/{paragraph_to_delete}")

    def delete_metadata(self, field_key: str, metadata: FieldComputedMetadata):
        for subfield, metadata_split in metadata.split_metadata.items():
            self.brain.paragraphs_to_delete.append(
                ids.FieldId(rid=self.rid, field_id=field_key, subfield_id=subfield).full()
            )
            # TODO: Bw/c, remove this when paragraph deletion by field_id gets
            # promoted
            for paragraph in metadata_split.paragraphs:
                self.brain.paragraphs_to_delete.append(
                    f"{self.rid}/{field_key}/{subfield}/{paragraph.start}-{paragraph.end}"
                )

        for paragraph in metadata.metadata.paragraphs:
            self.brain.paragraphs_to_delete.append(ids.FieldId(rid=self.rid, field_id=field_key).full())
            # TODO: Bw/c, remove this when paragraph deletion by field_id gets
            # promoted
            self.brain.paragraphs_to_delete.append(
                f"{self.rid}/{field_key}/{paragraph.start}-{paragraph.end}"
            )

    def apply_field_vectors(
        self,
        field_id: str,
        vo: utils_pb2.VectorObject,
        *,
        vectorset: Optional[str] = None,
        replace_field: bool = False,
        replace_splits: Optional[list[str]] = None,
        matryoshka_vector_dimension: Optional[int] = None,
    ):
        replace_splits = replace_splits or []

        for subfield, vectors in vo.split_vectors.items():
            _field_id = ids.FieldId(
                rid=self.rid,
                field_id=field_id,
                subfield_id=subfield,
            )
            # For each split of this field
            for index, vector in enumerate(vectors.vectors):
                paragraph_key = ids.ParagraphId(
                    field_id=_field_id,
                    paragraph_start=vector.start_paragraph,
                    paragraph_end=vector.end_paragraph,
                )
                sentence_key = ids.VectorId(
                    field_id=_field_id,
                    index=index,
                    vector_start=vector.start,
                    vector_end=vector.end,
                )
                self._apply_field_vector(
                    field_id,
                    paragraph_key,
                    sentence_key,
                    vector,
                    vectorset=vectorset,
                    matryoshka_vector_dimension=matryoshka_vector_dimension,
                )

        _field_id = ids.FieldId(
            rid=self.rid,
            field_id=field_id,
        )
        for index, vector in enumerate(vo.vectors.vectors):
            paragraph_key = ids.ParagraphId(
                field_id=_field_id,
                paragraph_start=vector.start_paragraph,
                paragraph_end=vector.end_paragraph,
            )
            sentence_key = ids.VectorId(
                field_id=_field_id,
                index=index,
                vector_start=vector.start,
                vector_end=vector.end,
            )
            self._apply_field_vector(
                field_id,
                paragraph_key,
                sentence_key,
                vector,
                vectorset=vectorset,
                matryoshka_vector_dimension=matryoshka_vector_dimension,
            )

        for split in replace_splits:
            self.brain.sentences_to_delete.append(
                ids.FieldId(rid=self.rid, field_id=field_id, subfield_id=split).full()
            )
            self.brain.paragraphs_to_delete.append(
                ids.FieldId(rid=self.rid, field_id=field_id, subfield_id=split).full()
            )

        if replace_field:
            self.brain.sentences_to_delete.append(ids.FieldId(rid=self.rid, field_id=field_id).full())
            self.brain.paragraphs_to_delete.append(ids.FieldId(rid=self.rid, field_id=field_id).full())

    def _apply_field_vector(
        self,
        field_id: str,
        paragraph_key: ids.ParagraphId,
        sentence_key: ids.VectorId,
        vector: utils_pb2.Vector,
        *,
        vectorset: Optional[str],
        matryoshka_vector_dimension: Optional[int] = None,
    ):
        paragraph_pb = self.brain.paragraphs[field_id].paragraphs[paragraph_key.full()]
        if vectorset:
            sentence_pb = paragraph_pb.vectorsets_sentences[vectorset].sentences[sentence_key.full()]
        else:
            sentence_pb = paragraph_pb.sentences[sentence_key.full()]

        sentence_pb.ClearField("vector")  # clear first to prevent duplicates

        # cut vectors if a specific dimension is specified
        if matryoshka_vector_dimension is not None:
            sentence_pb.vector.extend(vector.vector[:matryoshka_vector_dimension])
        else:
            sentence_pb.vector.extend(vector.vector)

        # we only care about start/stop position of the paragraph for a given sentence here
        # the key has the sentence position
        sentence_pb.metadata.position.start = vector.start_paragraph
        sentence_pb.metadata.position.end = vector.end_paragraph

        # does it make sense to copy forward paragraph values here?
        sentence_pb.metadata.position.page_number = paragraph_pb.metadata.position.page_number
        sentence_pb.metadata.position.in_page = paragraph_pb.metadata.position.in_page

        sentence_pb.metadata.page_with_visual = paragraph_pb.metadata.page_with_visual

        sentence_pb.metadata.representation.file = paragraph_pb.metadata.representation.file

        sentence_pb.metadata.representation.is_a_table = paragraph_pb.metadata.representation.is_a_table

        sentence_pb.metadata.position.index = paragraph_pb.metadata.position.index

    def delete_vectors(self, field_key: str, vo: VectorObject):
        for subfield, _ in vo.split_vectors.items():
            self.brain.sentences_to_delete.append(f"{self.rid}/{field_key}/{subfield}")

        self.brain.sentences_to_delete.append(f"{self.rid}/{field_key}")

    def set_processing_status(self, basic: Basic, previous_status: Optional[Metadata.Status.ValueType]):
        """
        We purposefully overwrite what we index as a status and DO NOT reflect
        actual status with what we index.

        This seems to be is on purpose so the frontend of the product can operate
        on 2 statuses only -- PENDING and PROCESSED.
        """
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

    def set_security(self, security: utils_pb2.Security):
        self.brain.security.CopyFrom(security)

    def get_processing_status_tag(self, metadata: Metadata) -> str:
        if not metadata.useful:
            return "EMPTY"
        return METADATA_STATUS_PB_TYPE_TO_NAME_MAP[metadata.status]

    def set_resource_metadata(self, basic: Basic, origin: Optional[Origin]):
        self._set_resource_dates(basic, origin)
        self._set_resource_labels(basic, origin)
        self._set_resource_relations(basic, origin)

    def _set_resource_dates(self, basic: Basic, origin: Optional[Origin]):
        if basic.created.seconds > 0:
            self.brain.metadata.created.CopyFrom(basic.created)
        else:
            logging.warning(f"Basic metadata has no created field for {self.rid}")
            self.brain.metadata.created.GetCurrentTime()
        if basic.modified.seconds > 0:
            self.brain.metadata.modified.CopyFrom(basic.modified)
        else:
            if basic.created.seconds > 0:
                self.brain.metadata.modified.CopyFrom(basic.created)
            else:
                self.brain.metadata.modified.GetCurrentTime()

        if origin is not None:
            # overwrite created/modified if provided on origin
            if origin.HasField("created") and origin.created.seconds > 0:
                self.brain.metadata.created.CopyFrom(origin.created)
            if origin.HasField("modified") and origin.modified.seconds > 0:
                self.brain.metadata.modified.CopyFrom(origin.modified)

    def _set_resource_relations(self, basic: Basic, origin: Optional[Origin]):
        relationnodedocument = RelationNode(value=self.rid, ntype=RelationNode.NodeType.RESOURCE)
        if origin is not None:
            # origin contributors
            for contrib in origin.colaborators:
                relationnodeuser = RelationNode(value=contrib, ntype=RelationNode.NodeType.USER)
                self.brain.relations.append(
                    Relation(
                        relation=Relation.COLAB,
                        source=relationnodedocument,
                        to=relationnodeuser,
                    )
                )

        # labels
        for classification in basic.usermetadata.classifications:
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

    def _set_resource_labels(self, basic: Basic, origin: Optional[Origin]):
        if origin is not None:
            if origin.source_id:
                self.labels["o"] = [origin.source_id]
            # origin tags
            for tag in origin.tags:
                self.labels["t"].append(tag)
            # origin source
            if origin.source_id != "":
                self.labels["u"].append(f"s/{origin.source_id}")

            if origin.path:
                self.labels["p"].append(origin.path.lstrip("/"))

            # origin contributors
            for contrib in origin.colaborators:
                self.labels["u"].append(f"o/{contrib}")

            for key, value in origin.metadata.items():
                self.labels["m"].append(f"{key[:255]}/{value[:255]}")

        # icon
        self.labels["n"].append(f"i/{basic.icon}")

        # processing status
        status_tag = self.get_processing_status_tag(basic.metadata)
        self.labels["n"].append(f"s/{status_tag}")

        # main language
        if basic.metadata.language:
            self.labels["s"].append(f"p/{basic.metadata.language}")

        # all language
        for lang in basic.metadata.languages:
            self.labels["s"].append(f"s/{lang}")

        # labels
        for classification in basic.usermetadata.classifications:
            self.labels["l"].append(f"{classification.labelset}/{classification.label}")

        self.compute_labels()

    def process_field_metadata(
        self,
        field_key: str,
        metadata: FieldMetadata,
        labels: dict[str, list[str]],
        relation_node_document: RelationNode,
        user_canceled_labels: list[str],
    ):
        for classification in metadata.classifications:
            label = f"{classification.labelset}/{classification.label}"
            if label not in user_canceled_labels:
                labels["l"].append(label)
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
            labels["e"].append(klass_entity)
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

    def apply_field_labels(
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

        relation_node_resource = RelationNode(value=uuid, ntype=RelationNode.NodeType.RESOURCE)
        labels: dict[str, list[str]] = {"l": [], "e": []}
        if metadata is not None:
            for meta in metadata.split_metadata.values():
                self.process_field_metadata(
                    field_key,
                    meta,
                    labels,
                    relation_node_resource,
                    user_canceled_labels,
                )
            self.process_field_metadata(
                field_key,
                metadata.metadata,
                labels,
                relation_node_resource,
                user_canceled_labels,
            )

        if basic_user_fieldmetadata is not None:
            for token in basic_user_fieldmetadata.token:
                if token.cancelled_by_user is False:
                    labels["e"].append(f"{token.klass}/{token.token}")
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
                        label = f"/l/{classification.labelset}/{classification.label}"
                        # FIXME: this condition avoid adding duplicate labels
                        # while importing a kb. We shouldn't add duplicates on
                        # the first place
                        if (
                            label
                            not in self.brain.paragraphs[field_key]
                            .paragraphs[paragraph_annotation.key]
                            .labels
                        ):
                            self.brain.paragraphs[field_key].paragraphs[
                                paragraph_annotation.key
                            ].labels.append(label)
        extend_unique(
            self.brain.texts[field_key].labels,  # type: ignore
            flatten_resource_labels(labels),
        )

    def compute_labels(self):
        extend_unique(self.brain.labels, flatten_resource_labels(self.labels))


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
    unique_paragraphs: set[str],
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


class ParagraphPages:
    """
    Class to get the page number for a given paragraph in an optimized way.
    """

    def __init__(self, positions: FilePagePositions):
        self.positions = positions
        self._materialized = self._materialize_page_numbers(positions)

    def _materialize_page_numbers(self, positions: FilePagePositions) -> list[int]:
        page_numbers_by_index = []
        for page_number, (page_start, page_end) in positions.items():
            page_numbers_by_index.extend([page_number] * (page_end - page_start + 1))
        return page_numbers_by_index

    def get(self, paragraph_start_index: int) -> int:
        try:
            return self._materialized[paragraph_start_index]
        except IndexError:
            logger.error(
                f"Could not find a page for the given index: {paragraph_start_index}. Page positions: {self.positions}"  # noqa
            )
            if len(self._materialized) > 0:
                return self._materialized[-1]
            return 0


def extend_unique(a: list, b: list):
    """
    Prevents extending with duplicate elements
    """
    for item in b:
        if item not in a:
            a.append(item)
