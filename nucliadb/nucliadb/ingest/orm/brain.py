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
import hashlib
from copy import deepcopy
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Union

from nucliadb_protos.noderesources_pb2 import IndexParagraph as BrainParagraph
from nucliadb_protos.noderesources_pb2 import ParagraphMetadata, ParagraphPosition
from nucliadb_protos.noderesources_pb2 import Resource as PBBrainResource
from nucliadb_protos.noderesources_pb2 import ResourceID
from nucliadb_protos.resources_pb2 import (
    Basic,
    FieldComputedMetadata,
    FieldKeywordset,
    FieldMetadata,
    Metadata,
    Origin,
    Paragraph,
)
from nucliadb_protos.utils_pb2 import Relation, RelationNode, VectorObject

from nucliadb.ingest.orm.labels import BASE_TAGS, flat_resource_tags

if TYPE_CHECKING:
    StatusValue = Union[Metadata.Status.V, int]
else:
    StatusValue = int

FilePagePositions = Dict[int, Tuple[int, int]]


class DuplicateParagraphsChecker:
    def __init__(self):
        self.seen = set()

    def check(self, paragraph: Paragraph) -> bool:
        """Returns whether paragraph has already been checked"""
        par_md5 = hashlib.md5(paragraph.text.encode()).hexdigest()
        if par_md5 in self.seen:
            return True
        else:
            self.seen.add(par_md5)
            return False


class ResourceBrain:
    def __init__(self, rid: str):
        self.rid = rid
        ridobj = ResourceID(uuid=rid)
        self.brain: PBBrainResource = PBBrainResource(resource=ridobj)
        self.tags: Dict[str, List[str]] = deepcopy(BASE_TAGS)

    def apply_field_text(self, field_key: str, text: str):
        self.brain.texts[field_key].text = text

    def get_paragraph_page_number(
        self, paragraph: Paragraph, page_positions: FilePagePositions
    ) -> int:
        for page_number, (page_start, page_end) in page_positions.items():
            if page_start <= paragraph.start <= page_end:
                return int(page_number)
        raise ValueError("Could not find paragraph page number!")

    def apply_field_metadata(
        self,
        field_key: str,
        metadata: FieldComputedMetadata,
        replace_field: List[str],
        replace_splits: Dict[str, List[str]],
        page_positions: Optional[FilePagePositions] = None,
    ):
        dups = DuplicateParagraphsChecker()

        # We should set paragraphs and labels
        for subfield, metadata_split in metadata.split_metadata.items():
            # For each split of this field
            for index, paragraph in enumerate(metadata_split.paragraphs):
                key = f"{self.rid}/{field_key}/{subfield}/{paragraph.start}-{paragraph.end}"
                position = ParagraphPosition(
                    index=index,
                    start=paragraph.start,
                    end=paragraph.end,
                    start_seconds=paragraph.start_seconds,
                    end_seconds=paragraph.end_seconds,
                )
                if page_positions:
                    position.page_number = self.get_paragraph_page_number(
                        paragraph, page_positions
                    )
                p = BrainParagraph(
                    start=paragraph.start,
                    end=paragraph.end,
                    field=field_key,
                    split=subfield,
                    index=index,
                    repeated_in_field=dups.check(paragraph),
                    metadata=ParagraphMetadata(position=position),
                )
                for classification in paragraph.classifications:
                    p.labels.append(
                        f"l/{classification.labelset}/{classification.label}"
                    )

                self.brain.paragraphs[field_key].paragraphs[key].CopyFrom(p)

        for index, paragraph in enumerate(metadata.metadata.paragraphs):
            key = f"{self.rid}/{field_key}/{paragraph.start}-{paragraph.end}"
            position = ParagraphPosition(
                index=index,
                start=paragraph.start,
                end=paragraph.end,
                start_seconds=paragraph.start_seconds,
                end_seconds=paragraph.end_seconds,
            )
            if page_positions:
                position.page_number = self.get_paragraph_page_number(
                    paragraph, page_positions
                )
            p = BrainParagraph(
                start=paragraph.start,
                end=paragraph.end,
                field=field_key,
                index=index,
                repeated_in_field=dups.check(paragraph),
                metadata=ParagraphMetadata(position=position),
            )
            for classification in paragraph.classifications:
                p.labels.append(f"l/{classification.labelset}/{classification.label}")

            self.brain.paragraphs[field_key].paragraphs[key].CopyFrom(p)

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
                self.brain.paragraphs[field_key].paragraphs[
                    f"{self.rid}/{field_key}/{subfield}/{vector.start_paragraph}-{vector.end_paragraph}"
                ].sentences[
                    f"{self.rid}/{field_key}/{subfield}/{index}/{vector.start}-{vector.end}"
                ].vector.extend(
                    vector.vector
                )

        for index, vector in enumerate(vo.vectors.vectors):
            self.brain.paragraphs[field_key].paragraphs[
                f"{self.rid}/{field_key}/{vector.start_paragraph}-{vector.end_paragraph}"
            ].sentences[
                f"{self.rid}/{field_key}/{index}/{vector.start}-{vector.end}"
            ].vector.extend(
                vector.vector
            )

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

    def set_status(self, status: StatusValue, useful: Optional[bool]):
        if status == Metadata.Status.ERROR:
            self.brain.status = PBBrainResource.ERROR
        elif useful is False:
            self.brain.status = PBBrainResource.EMPTY
        elif status == Metadata.Status.PROCESSED:
            self.brain.status = PBBrainResource.PROCESSED
        elif status == Metadata.Status.PENDING:
            self.brain.status = PBBrainResource.PENDING

    def set_global_tags(self, basic: Basic, uuid: str, origin: Optional[Origin]):

        self.brain.metadata.created.CopyFrom(basic.created)
        self.brain.metadata.modified.CopyFrom(basic.modified)

        self.set_status(basic.metadata.status, basic.metadata.useful)

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

        # main language
        if basic.metadata.language != "":
            self.tags["s"].append(f"p/{basic.metadata.language}")

        # all language
        for lang in basic.metadata.languages:
            self.tags["s"].append(f"s/{lang}")

        # labels
        for classification in basic.usermetadata.classifications:
            self.tags["l"].append(f"{classification.labelset}/{classification.label}")
            relationnodelabel = RelationNode(
                value=f"{classification.labelset}/{classification.label}",
                ntype=RelationNode.NodeType.LABEL,
            )
            self.brain.relations.append(
                Relation(
                    relation=Relation.ABOUT,
                    source=relationnodedocument,
                    to=relationnodelabel,
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
        relationnodedocument: RelationNode,
    ):
        for classification in metadata.classifications:
            tags["l"].append(f"{classification.labelset}/{classification.label}")
            relationnodelabel = RelationNode(
                value=f"{classification.labelset}/{classification.label}",
                ntype=RelationNode.NodeType.LABEL,
            )
            self.brain.relations.append(
                Relation(
                    relation=Relation.ABOUT,
                    source=relationnodedocument,
                    to=relationnodelabel,
                )
            )

        for entity, klass in metadata.ner.items():
            tags["e"].append(f"{klass}/{entity}")
            relationnodeentity = RelationNode(
                value=entity, ntype=RelationNode.NodeType.ENTITY, subtype=klass
            )
            rel = Relation(
                relation=Relation.ENTITY,
                source=relationnodedocument,
                to=relationnodeentity,
            )
            self.brain.relations.append(rel)

    def process_keywordset_fields(self, field_key: str, field: FieldKeywordset):
        # all field keywords
        if field:
            for keyword in field.keywords:
                self.tags["f"].append(f"{field_key}/{keyword.value}")
                self.tags["fg"].append(keyword.value)

    def apply_field_tags_globally(
        self, field_key: str, metadata: FieldComputedMetadata, uuid: str
    ):
        relationnodedocument = RelationNode(
            value=uuid, ntype=RelationNode.NodeType.RESOURCE
        )
        tags: Dict[str, List[str]] = {"l": [], "e": []}
        for meta in metadata.split_metadata.values():
            self.process_meta(field_key, meta, tags, relationnodedocument)
        self.process_meta(field_key, metadata.metadata, tags, relationnodedocument)
        self.brain.texts[field_key].labels.extend(flat_resource_tags(tags))

    def compute_tags(self):
        self.brain.labels.extend(flat_resource_tags(self.tags))
