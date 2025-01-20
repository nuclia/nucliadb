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
from nucliadb_models.labels import BASE_LABELS, LABEL_HIDDEN, flatten_resource_labels
from nucliadb_models.metadata import ResourceProcessingStatus
from nucliadb_protos import utils_pb2
from nucliadb_protos.noderesources_pb2 import IndexParagraph as BrainParagraph
from nucliadb_protos.noderesources_pb2 import (
    ParagraphMetadata,
    Representation,
    ResourceID,
)
from nucliadb_protos.noderesources_pb2 import Position as TextPosition
from nucliadb_protos.noderesources_pb2 import Resource as PBBrainResource
from nucliadb_protos.resources_pb2 import (
    Basic,
    ExtractedText,
    FieldAuthor,
    FieldComputedMetadata,
    FieldMetadata,
    Metadata,
    Origin,
    Paragraph,
    UserFieldMetadata,
    UserMetadata,
)
from nucliadb_protos.utils_pb2 import Relation, RelationNode

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
        self.labels: dict[str, set[str]] = deepcopy(BASE_LABELS)

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
        page_positions: Optional[FilePagePositions],
        extracted_text: Optional[ExtractedText],
        basic_user_field_metadata: Optional[UserFieldMetadata] = None,
        *,
        replace_field: bool = False,
    ):
        # To check for duplicate paragraphs
        unique_paragraphs: set[str] = set()

        # Expose also user classifications
        user_paragraph_classifications = self._get_paragraph_user_classifications(
            basic_user_field_metadata
        )

        # We should set paragraphs and labels
        paragraph_pages = ParagraphPages(page_positions) if page_positions else None
        for subfield, metadata_split in metadata.split_metadata.items():
            extracted_text_str = extracted_text.split_text[subfield] if extracted_text else None

            # For each split of this field
            for index, paragraph in enumerate(metadata_split.paragraphs):
                key = f"{self.rid}/{field_key}/{subfield}/{paragraph.start}-{paragraph.end}"

                denied_classifications = set(user_paragraph_classifications.denied.get(key, []))
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
                        extracted_text_str,
                        unique_paragraphs,
                    ),
                    metadata=ParagraphMetadata(
                        position=position,
                        page_with_visual=page_with_visual,
                        representation=representation,
                    ),
                )
                paragraph_kind_label = f"/k/{Paragraph.TypeParagraph.Name(paragraph.kind).lower()}"
                paragraph_labels = {paragraph_kind_label}
                paragraph_labels.update(
                    f"/l/{classification.labelset}/{classification.label}"
                    for classification in paragraph.classifications
                )
                paragraph_labels.update(set(user_paragraph_classifications.valid.get(key, [])))
                paragraph_labels.difference_update(denied_classifications)
                p.labels.extend(list(paragraph_labels))

                self.brain.paragraphs[field_key].paragraphs[key].CopyFrom(p)

        extracted_text_str = extracted_text.text if extracted_text else None
        for index, paragraph in enumerate(metadata.metadata.paragraphs):
            key = f"{self.rid}/{field_key}/{paragraph.start}-{paragraph.end}"
            denied_classifications = set(user_paragraph_classifications.denied.get(key, []))
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
                    paragraph, extracted_text_str, unique_paragraphs
                ),
                metadata=ParagraphMetadata(
                    position=position,
                    page_with_visual=page_with_visual,
                    representation=representation,
                ),
            )
            paragraph_kind_label = f"/k/{Paragraph.TypeParagraph.Name(paragraph.kind).lower()}"
            paragraph_labels = {paragraph_kind_label}
            paragraph_labels.update(
                f"/l/{classification.labelset}/{classification.label}"
                for classification in paragraph.classifications
            )
            paragraph_labels.update(set(user_paragraph_classifications.valid.get(key, [])))
            paragraph_labels.difference_update(denied_classifications)
            p.labels.extend(list(paragraph_labels))

            self.brain.paragraphs[field_key].paragraphs[key].CopyFrom(p)

        if replace_field:
            field_type, field_name = field_key.split("/")
            full_field_id = ids.FieldId(rid=self.rid, type=field_type, key=field_name).full()
            self.brain.paragraphs_to_delete.append(full_field_id)

        for relations in metadata.metadata.relations:
            for relation in relations.relations:
                self.brain.relations.append(relation)

    def delete_field(self, field_key: str):
        ftype, fkey = field_key.split("/")
        full_field_id = ids.FieldId(rid=self.rid, type=ftype, key=fkey).full()
        self.brain.paragraphs_to_delete.append(full_field_id)
        self.brain.sentences_to_delete.append(full_field_id)

    def apply_field_vectors(
        self,
        field_id: str,
        vo: utils_pb2.VectorObject,
        *,
        vectorset: str,
        replace_field: bool = False,
        # cut to specific dimension if specified
        vector_dimension: Optional[int] = None,
    ):
        fid = ids.FieldId.from_string(f"{self.rid}/{field_id}")
        for subfield, vectors in vo.split_vectors.items():
            _field_id = ids.FieldId(
                rid=fid.rid,
                type=fid.type,
                key=fid.key,
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
                    vector_dimension=vector_dimension,
                )

        _field_id = ids.FieldId(
            rid=fid.rid,
            type=fid.type,
            key=fid.key,
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
                vector_dimension=vector_dimension,
            )

        if replace_field:
            full_field_id = ids.FieldId(rid=self.rid, type=fid.type, key=fid.key).full()
            self.brain.vector_prefixes_to_delete[vectorset].items.append(full_field_id)

    def _apply_field_vector(
        self,
        field_id: str,
        paragraph_key: ids.ParagraphId,
        sentence_key: ids.VectorId,
        vector: utils_pb2.Vector,
        *,
        vectorset: str,
        # cut vectors if a specific dimension is specified
        vector_dimension: Optional[int] = None,
    ):
        paragraph_pb = self.brain.paragraphs[field_id].paragraphs[paragraph_key.full()]
        sentence_pb = paragraph_pb.vectorsets_sentences[vectorset].sentences[sentence_key.full()]

        sentence_pb.ClearField("vector")  # clear first to prevent duplicates
        sentence_pb.vector.extend(vector.vector[:vector_dimension])

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
                self.labels["o"] = {origin.source_id}
            # origin tags
            for tag in origin.tags:
                self.labels["t"].add(tag)
            # origin source
            if origin.source_id != "":
                self.labels["u"].add(f"s/{origin.source_id}")

            if origin.path:
                self.labels["p"].add(origin.path.lstrip("/"))

            # origin contributors
            for contrib in origin.colaborators:
                self.labels["u"].add(f"o/{contrib}")

            for key, value in origin.metadata.items():
                self.labels["m"].add(f"{key[:255]}/{value[:255]}")

        # icon
        self.labels["n"].add(f"i/{basic.icon}")

        # processing status
        status_tag = self.get_processing_status_tag(basic.metadata)
        self.labels["n"].add(f"s/{status_tag}")

        # main language
        if basic.metadata.language:
            self.labels["s"].add(f"p/{basic.metadata.language}")

        # all language
        for lang in basic.metadata.languages:
            self.labels["s"].add(f"s/{lang}")

        # labels
        for classification in basic.usermetadata.classifications:
            self.labels["l"].add(f"{classification.labelset}/{classification.label}")

        # hidden
        if basic.hidden:
            _, p1, p2 = LABEL_HIDDEN.split("/")
            self.labels[p1].add(p2)

        self.brain.ClearField("labels")
        self.brain.labels.extend(flatten_resource_labels(self.labels))

    def process_field_metadata(
        self,
        field_key: str,
        metadata: FieldMetadata,
        labels: dict[str, set[str]],
        relation_node_document: RelationNode,
        user_canceled_labels: set[str],
    ):
        if metadata.mime_type != "":
            labels["mt"].add(metadata.mime_type)

        base_classification_relation = Relation(
            relation=Relation.ABOUT,
            source=relation_node_document,
            to=RelationNode(
                ntype=RelationNode.NodeType.LABEL,
            ),
        )
        for classification in metadata.classifications:
            label = f"{classification.labelset}/{classification.label}"
            if label not in user_canceled_labels:
                labels["l"].add(label)
                relation = Relation()
                relation.CopyFrom(base_classification_relation)
                relation.to.value = label
                self.brain.relations.append(relation)

        # Data Augmentation + Processor entities
        base_entity_relation = Relation(
            relation=Relation.ENTITY,
            source=relation_node_document,
            to=RelationNode(ntype=RelationNode.NodeType.ENTITY),
        )
        use_legacy_entities = True
        for data_augmentation_task_id, entities in metadata.entities.items():
            # If we recieved the entities from the processor here, we don't want to use the legacy entities
            # TODO: Remove this when processor doesn't use this anymore
            if data_augmentation_task_id == "processor":
                use_legacy_entities = False

            for ent in entities.entities:
                entity_text = ent.text
                entity_label = ent.label
                # Seems like we don't care about where the entity is in the text
                # entity_positions = entity.positions
                labels["e"].add(
                    f"{entity_label}/{entity_text}"
                )  # Add data_augmentation_task_id as a prefix?
                relation = Relation()
                relation.CopyFrom(base_entity_relation)
                relation.to.value = entity_text
                relation.to.subtype = entity_label
                self.brain.relations.append(relation)

        # Legacy processor entities
        # TODO: Remove once processor doesn't use this anymore and remove the positions and ner fields from the message
        def _parse_entity(klass_entity: str) -> tuple[str, str]:
            try:
                klass, entity = klass_entity.split("/", 1)
                return klass, entity
            except ValueError:
                raise AttributeError(f"Entity should be with type {klass_entity}")

        if use_legacy_entities:
            for klass_entity in metadata.positions.keys():
                labels["e"].add(klass_entity)
                klass, entity = _parse_entity(klass_entity)
                relation = Relation()
                relation.CopyFrom(base_entity_relation)
                relation.to.value = entity
                relation.to.subtype = klass
                self.brain.relations.append(relation)

    def apply_field_labels(
        self,
        field_key: str,
        metadata: Optional[FieldComputedMetadata],
        uuid: str,
        generated_by: FieldAuthor,
        basic_user_metadata: Optional[UserMetadata] = None,
        basic_user_fieldmetadata: Optional[UserFieldMetadata] = None,
    ):
        user_canceled_labels: set[str] = set()
        if basic_user_metadata is not None:
            user_canceled_labels.update(
                f"{classification.labelset}/{classification.label}"
                for classification in basic_user_metadata.classifications
                if classification.cancelled_by_user
            )
        relation_node_resource = RelationNode(value=uuid, ntype=RelationNode.NodeType.RESOURCE)
        labels: dict[str, set[str]] = {
            "l": set(),  # classification labels
            "e": set(),  # entities
            "mt": set(),  # mime type
            "g/da": set(),  # generated by
        }
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
                    labels["e"].add(f"{token.klass}/{token.token}")
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

        if generated_by.WhichOneof("author") == "data_augmentation":
            field_type, field_id = field_key.split("/")
            da_task_id = ids.extract_data_augmentation_id(field_id)
            if da_task_id is None:  # pragma: nocover
                logger.warning(
                    "Data augmentation field id has an unexpected format! Skipping label",
                    extra={
                        "rid": uuid,
                        "field_id": field_id,
                    },
                )
            else:
                labels["g/da"].add(da_task_id)

        self.brain.texts[field_key].labels.extend(flatten_resource_labels(labels))


def is_paragraph_repeated_in_field(
    paragraph: Paragraph,
    extracted_text: Optional[str],
    unique_paragraphs: set[str],
) -> bool:
    if extracted_text is None:
        return False

    paragraph_text = extracted_text[paragraph.start : paragraph.end]
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
