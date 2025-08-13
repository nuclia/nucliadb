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

from nidx_protos.noderesources_pb2 import IndexParagraph as BrainParagraph
from nidx_protos.noderesources_pb2 import (
    IndexRelation,
    ParagraphMetadata,
    Representation,
    ResourceID,
)
from nidx_protos.noderesources_pb2 import Position as TextPosition
from nidx_protos.noderesources_pb2 import Resource as PBBrainResource

from nucliadb.common import ids
from nucliadb.ingest import logger
from nucliadb.ingest.orm.metrics import brain_observer as observer
from nucliadb.ingest.orm.utils import compute_paragraph_key
from nucliadb_models.labels import BASE_LABELS, LABEL_HIDDEN, flatten_resource_labels
from nucliadb_models.metadata import ResourceProcessingStatus
from nucliadb_protos import utils_pb2
from nucliadb_protos.resources_pb2 import (
    Basic,
    ExtractedText,
    FieldAuthor,
    FieldComputedMetadata,
    Metadata,
    Origin,
    Paragraph,
    Relations,
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
        self.brain: PBBrainResource = PBBrainResource(resource=ResourceID(uuid=rid))
        self.labels: dict[str, set[str]] = deepcopy(BASE_LABELS)

    @observer.wrap({"type": "generate_resource_metadata"})
    def generate_resource_metadata(
        self,
        basic: Basic,
        user_relations: Relations,
        origin: Optional[Origin],
        previous_processing_status: Optional[Metadata.Status.ValueType],
        security: Optional[utils_pb2.Security],
    ) -> None:
        self._set_resource_status(basic, previous_processing_status)
        self._set_resource_dates(basic, origin)
        self._set_resource_labels(basic, origin)
        self._set_resource_relations(basic, origin, user_relations)
        if security is not None:
            self._set_resource_security(security)

    @observer.wrap({"type": "generate_texts"})
    def generate_texts(
        self,
        field_key: str,
        extracted_text: ExtractedText,
        field_computed_metadata: Optional[FieldComputedMetadata],
        basic_user_metadata: Optional[UserMetadata],
        field_author: Optional[FieldAuthor],
        replace_field: bool,
        skip_index: bool,
    ) -> None:
        self.apply_field_text(
            field_key,
            extracted_text,
            replace_field=replace_field,
            skip_texts=skip_index,
        )
        self.apply_field_labels(
            field_key,
            field_computed_metadata,
            field_author,
            basic_user_metadata,
        )

    @observer.wrap({"type": "apply_field_text"})
    def apply_field_text(
        self,
        field_key: str,
        extracted_text: ExtractedText,
        replace_field: bool,
        skip_texts: Optional[bool],
    ):
        if skip_texts is not None:
            self.brain.skip_texts = skip_texts
        field_text = extracted_text.text
        for _, split in extracted_text.split_text.items():
            field_text += f" {split} "
        self.brain.texts[field_key].text = field_text

        if replace_field:
            ftype, fkey = field_key.split("/")
            full_field_id = ids.FieldId(rid=self.rid, type=ftype, key=fkey).full()
            self.brain.texts_to_delete.append(full_field_id)

    @observer.wrap({"type": "apply_field_labels"})
    def apply_field_labels(
        self,
        field_key: str,
        field_computed_metadata: Optional[FieldComputedMetadata],
        field_author: Optional[FieldAuthor],
        basic_user_metadata: Optional[UserMetadata] = None,
    ):
        user_cancelled_labels: set[str] = (
            set(
                [
                    f"{classification.labelset}/{classification.label}"
                    for classification in basic_user_metadata.classifications
                    if classification.cancelled_by_user
                ]
            )
            if basic_user_metadata
            else set()
        )
        labels: dict[str, set[str]] = {
            "l": set(),  # classification labels
            "e": set(),  # entities
            "mt": set(),  # mime type
            "g/da": set(),  # generated by
        }
        if field_computed_metadata is not None:
            metadatas = list(field_computed_metadata.split_metadata.values())
            metadatas.append(field_computed_metadata.metadata)
            for metadata in metadatas:
                if metadata.mime_type != "":
                    labels["mt"].add(metadata.mime_type)
                for classification in metadata.classifications:
                    label = f"{classification.labelset}/{classification.label}"
                    if label not in user_cancelled_labels:
                        labels["l"].add(label)
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
                # Legacy processor entities
                if use_legacy_entities:
                    for klass_entity in metadata.positions.keys():
                        labels["e"].add(klass_entity)

        if field_author is not None and field_author.WhichOneof("author") == "data_augmentation":
            field_type, field_id = field_key.split("/")
            da_task_id = ids.extract_data_augmentation_id(field_id)
            if da_task_id is None:  # pragma: nocover
                logger.warning(
                    "Data augmentation field id has an unexpected format! Skipping label",
                    extra={
                        "field_id": field_id,
                        "field_type": field_type,
                    },
                )
            else:
                labels["g/da"].add(da_task_id)

        self.brain.texts[field_key].labels.extend(flatten_resource_labels(labels))

    @observer.wrap({"type": "generate_paragraphs"})
    def generate_paragraphs(
        self,
        field_key: str,
        field_computed_metadata: FieldComputedMetadata,
        extracted_text: ExtractedText,
        page_positions: Optional[FilePagePositions],
        user_field_metadata: Optional[UserFieldMetadata],
        replace_field: bool,
        skip_paragraphs_index: Optional[bool],
        skip_texts_index: Optional[bool],
    ) -> None:
        # We need to add the extracted text to the texts section of the Resource so that
        # the paragraphs can be indexed
        self.apply_field_text(
            field_key,
            extracted_text,
            replace_field=False,
            skip_texts=skip_texts_index,
        )
        self.apply_field_paragraphs(
            field_key,
            field_computed_metadata,
            extracted_text,
            page_positions,
            user_field_metadata,
            replace_field=replace_field,
            skip_paragraphs=skip_paragraphs_index,
        )

    @observer.wrap({"type": "apply_field_paragraphs"})
    def apply_field_paragraphs(
        self,
        field_key: str,
        field_computed_metadata: FieldComputedMetadata,
        extracted_text: ExtractedText,
        page_positions: Optional[FilePagePositions],
        user_field_metadata: Optional[UserFieldMetadata],
        replace_field: bool,
        skip_paragraphs: Optional[bool],
    ) -> None:
        if skip_paragraphs is not None:
            self.brain.skip_paragraphs = skip_paragraphs
        unique_paragraphs: set[str] = set()
        user_paragraph_classifications = self._get_paragraph_user_classifications(user_field_metadata)
        paragraph_pages = ParagraphPages(page_positions) if page_positions else None
        # Splits of the field
        for subfield, field_metadata in field_computed_metadata.split_metadata.items():
            extracted_text_str = extracted_text.split_text[subfield] if extracted_text else None
            for idx, paragraph in enumerate(field_metadata.paragraphs):
                key = f"{self.rid}/{field_key}/{subfield}/{paragraph.start}-{paragraph.end}"
                denied_classifications = set(user_paragraph_classifications.denied.get(key, []))
                position = TextPosition(
                    index=idx,
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
                    index=idx,
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

        # Main field
        extracted_text_str = extracted_text.text if extracted_text else None
        for idx, paragraph in enumerate(field_computed_metadata.metadata.paragraphs):
            key = f"{self.rid}/{field_key}/{paragraph.start}-{paragraph.end}"
            denied_classifications = set(user_paragraph_classifications.denied.get(key, []))
            position = TextPosition(
                index=idx,
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
                index=idx,
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

    @observer.wrap({"type": "generate_relations"})
    def generate_relations(
        self,
        field_key: str,
        field_computed_metadata: Optional[FieldComputedMetadata],
        basic_user_metadata: Optional[UserMetadata],
        replace_field: bool,
    ) -> None:
        user_cancelled_labels: set[str] = (
            set(
                [
                    f"{classification.labelset}/{classification.label}"
                    for classification in basic_user_metadata.classifications
                    if classification.cancelled_by_user
                ]
            )
            if basic_user_metadata
            else set()
        )

        field_relations = self.brain.field_relations[field_key].relations

        # Index relations that are computed by the processor
        if field_computed_metadata is not None:
            relation_node_document = RelationNode(
                value=self.brain.resource.uuid,
                ntype=RelationNode.NodeType.RESOURCE,
            )
            field_metadatas = list(field_computed_metadata.split_metadata.values())
            field_metadatas.append(field_computed_metadata.metadata)
            for field_metadata in field_metadatas:
                # Relations computed by the processor
                for relations in field_metadata.relations:
                    for relation in relations.relations:
                        index_relation = IndexRelation(relation=relation)
                        if relation.metadata.HasField("data_augmentation_task_id"):
                            index_relation.facets.append(
                                f"/g/da/{relation.metadata.data_augmentation_task_id}"
                            )
                        field_relations.append(index_relation)
                # Entities computed by the processor or ingestion agents
                base_entity_relation = Relation(
                    relation=Relation.ENTITY,
                    source=relation_node_document,
                    to=RelationNode(ntype=RelationNode.NodeType.ENTITY),
                )
                use_legacy_entities = True
                for data_augmentation_task_id, entities in field_metadata.entities.items():
                    # If we recieved the entities from the processor here, we don't want to use the legacy entities
                    # TODO: Remove this when processor doesn't use this anymore
                    if data_augmentation_task_id == "processor":
                        use_legacy_entities = False

                    for ent in entities.entities:
                        entity_text = ent.text
                        entity_label = ent.label
                        relation = Relation()
                        relation.CopyFrom(base_entity_relation)
                        relation.to.value = entity_text
                        relation.to.subtype = entity_label
                        field_relations.append(IndexRelation(relation=relation))

                # Legacy processor entities
                # TODO: Remove once processor doesn't use this anymore and remove the positions and ner fields from the message
                def _parse_entity(klass_entity: str) -> tuple[str, str]:
                    try:
                        klass, entity = klass_entity.split("/", 1)
                        return klass, entity
                    except ValueError:
                        raise AttributeError(f"Entity should be with type {klass_entity}")

                if use_legacy_entities:
                    for klass_entity in field_metadata.positions.keys():
                        klass, entity = _parse_entity(klass_entity)
                        relation = Relation()
                        relation.CopyFrom(base_entity_relation)
                        relation.to.value = entity
                        relation.to.subtype = klass
                        field_relations.append(IndexRelation(relation=relation))

                # Relations from field to classifications label
                base_classification_relation = Relation(
                    relation=Relation.ABOUT,
                    source=relation_node_document,
                    to=RelationNode(
                        ntype=RelationNode.NodeType.LABEL,
                    ),
                )
                for classification in field_metadata.classifications:
                    label = f"{classification.labelset}/{classification.label}"
                    if label in user_cancelled_labels:
                        continue
                    relation = Relation()
                    relation.CopyFrom(base_classification_relation)
                    relation.to.value = label
                    field_relations.append(IndexRelation(relation=relation))
        if replace_field:
            self.brain.relation_fields_to_delete.append(field_key)

    def delete_field(self, field_key: str):
        ftype, fkey = field_key.split("/")
        full_field_id = ids.FieldId(rid=self.rid, type=ftype, key=fkey).full()
        self.brain.texts_to_delete.append(full_field_id)
        self.brain.paragraphs_to_delete.append(full_field_id)
        self.brain.sentences_to_delete.append(full_field_id)
        self.brain.relation_fields_to_delete.append(field_key)

    @observer.wrap({"type": "generate_vectors"})
    def generate_vectors(
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

    @observer.wrap({"type": "apply_field_vector"})
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

    def _set_resource_status(self, basic: Basic, previous_status: Optional[Metadata.Status.ValueType]):
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

    def _set_resource_security(self, security: utils_pb2.Security):
        self.brain.security.CopyFrom(security)

    def get_processing_status_tag(self, metadata: Metadata) -> str:
        if not metadata.useful:
            return "EMPTY"
        return METADATA_STATUS_PB_TYPE_TO_NAME_MAP[metadata.status]

    def _set_resource_dates(self, basic: Basic, origin: Optional[Origin]):
        """
        Adds the user-defined dates to the brain object. This is at resource level and applies to
        all fields of the resource.
        """
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

    def _set_resource_relations(self, basic: Basic, origin: Optional[Origin], user_relations: Relations):
        """
        Adds the relations to the brain object corresponding to the user-defined metadata at the resource level:
        - Contributors of the document
        - Classificatin labels
        - Relations
        """
        relationnodedocument = RelationNode(value=self.rid, ntype=RelationNode.NodeType.RESOURCE)
        if origin is not None:
            # origin contributors
            for contrib in origin.colaborators:
                relationnodeuser = RelationNode(value=contrib, ntype=RelationNode.NodeType.USER)
                relation = Relation(
                    relation=Relation.COLAB,
                    source=relationnodedocument,
                    to=relationnodeuser,
                )
                self.brain.field_relations["a/metadata"].relations.append(
                    IndexRelation(relation=relation)
                )

        # labels
        for classification in basic.usermetadata.classifications:
            if classification.cancelled_by_user:
                continue
            relation_node_label = RelationNode(
                value=f"{classification.labelset}/{classification.label}",
                ntype=RelationNode.NodeType.LABEL,
            )
            relation = Relation(
                relation=Relation.ABOUT,
                source=relationnodedocument,
                to=relation_node_label,
            )
            self.brain.field_relations["a/metadata"].relations.append(IndexRelation(relation=relation))

        # relations
        for relation in user_relations.relations:
            self.brain.field_relations["a/metadata"].relations.append(
                IndexRelation(relation=relation, facets=["/g/u"])
            )

        self.brain.relation_fields_to_delete.append("a/metadata")

    def _set_resource_labels(self, basic: Basic, origin: Optional[Origin]):
        """
        Adds the resource-level labels to the brain object.
        These levels are user-defined in basic or origin metadata.
        """
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
            if classification.cancelled_by_user:
                continue
            self.labels["l"].add(f"{classification.labelset}/{classification.label}")

        # hidden
        if basic.hidden:
            _, p1, p2 = LABEL_HIDDEN.split("/")
            self.labels[p1].add(p2)

        self.brain.ClearField("labels")
        self.brain.labels.extend(flatten_resource_labels(self.labels))


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

    @observer.wrap({"type": "materialize_page_numbers"})
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
