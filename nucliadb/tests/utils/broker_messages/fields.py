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

import dataclasses
from datetime import datetime
from typing import Iterator, Optional

from nucliadb.common.ids import FIELD_TYPE_PB_TO_STR, FieldId, ParagraphId
from nucliadb_protos import resources_pb2 as rpb
from nucliadb_protos import utils_pb2

from .helpers import labels_to_classifications


@dataclasses.dataclass
class FieldUser:
    metadata: Optional[rpb.UserFieldMetadata] = None


@dataclasses.dataclass
class FieldExtracted:
    metadata: Optional[rpb.FieldComputedMetadataWrapper] = None
    text: Optional[rpb.ExtractedTextWrapper] = None
    vectors: Optional[list[rpb.ExtractedVectorsWrapper]] = None
    question_answers: Optional[rpb.FieldQuestionAnswerWrapper] = None


@dataclasses.dataclass
class Field:
    id: rpb.FieldID
    user: FieldUser = dataclasses.field(default_factory=FieldUser)
    extracted: FieldExtracted = dataclasses.field(default_factory=FieldExtracted)


class FieldBuilder:
    def __init__(self, rid: str, field: str, field_type: rpb.FieldType.ValueType):
        self._rid = rid
        self._field_id = rpb.FieldID(field=field, field_type=field_type)
        self.__extracted_metadata: Optional[rpb.FieldComputedMetadataWrapper] = None
        self.__extracted_text: Optional[rpb.ExtractedTextWrapper] = None
        self.__extracted_vectors: Optional[dict[str, rpb.ExtractedVectorsWrapper]] = None
        self.__user_metadata: Optional[rpb.UserFieldMetadata] = None
        self.__question_answers: Optional[rpb.FieldQuestionAnswerWrapper] = None

    @property
    def id(self) -> rpb.FieldID:
        return self._field_id

    # properties to generate a default value per pb

    @property
    def _extracted_metadata(self) -> rpb.FieldComputedMetadataWrapper:
        if self.__extracted_metadata is None:
            now = datetime.now()
            self.__extracted_metadata = rpb.FieldComputedMetadataWrapper(
                field=self._field_id,
            )
            self.__extracted_metadata.metadata.metadata.last_index.FromDatetime(now)
            self.__extracted_metadata.metadata.metadata.last_understanding.FromDatetime(now)
            self.__extracted_metadata.metadata.metadata.last_extract.FromDatetime(now)
            self.__extracted_metadata.metadata.metadata.last_processing_start.FromDatetime(now)
        return self.__extracted_metadata

    @property
    def _extracted_text(self) -> rpb.ExtractedTextWrapper:
        if self.__extracted_text is None:
            self.__extracted_text = rpb.ExtractedTextWrapper(field=self._field_id)
        return self.__extracted_text

    def _extracted_vectors(self, vectorset: str) -> rpb.ExtractedVectorsWrapper:
        if self.__extracted_vectors is None:
            self.__extracted_vectors = dict()
        return self.__extracted_vectors.setdefault(
            vectorset, rpb.ExtractedVectorsWrapper(field=self._field_id, vectorset_id=vectorset)
        )

    @property
    def _question_answers(self) -> rpb.FieldQuestionAnswerWrapper:
        if self.__question_answers is None:
            self.__question_answers = rpb.FieldQuestionAnswerWrapper(field=self._field_id)
        return self.__question_answers

    @property
    def _user_metadata(self) -> rpb.UserFieldMetadata:
        if self.__user_metadata is None:
            self.__user_metadata = rpb.UserFieldMetadata(field=self._field_id)
        return self.__user_metadata

    def build(self) -> Field:
        field = Field(id=self._field_id)

        if self.__extracted_metadata is not None:
            field.extracted.metadata = rpb.FieldComputedMetadataWrapper()
            field.extracted.metadata.CopyFrom(self.__extracted_metadata)

        if self.__extracted_text is not None:
            field.extracted.text = rpb.ExtractedTextWrapper()
            field.extracted.text.CopyFrom(self.__extracted_text)

        if self.__extracted_vectors is not None:
            field.extracted.vectors = list(self.__extracted_vectors.values())

        if self.__question_answers is not None:
            field.extracted.question_answers = rpb.FieldQuestionAnswerWrapper()
            field.extracted.question_answers.CopyFrom(self.__question_answers)

        if self.__user_metadata is not None:
            field.user.metadata = rpb.UserFieldMetadata()
            field.user.metadata.CopyFrom(self.__user_metadata)

        return field

    def with_extracted_labels(self, labelset: str, labels: list[str]):
        classifications = labels_to_classifications(labelset, labels)
        self._extracted_metadata.metadata.metadata.classifications.extend(classifications)

    def with_extracted_text(self, text: str, split: Optional[str] = None):
        if split is None:
            self._extracted_text.body.text = text
        else:
            self._extracted_text.body.split_text[split] = text

    def with_extracted_vectors(
        self, vectors: list[utils_pb2.Vector], vectorset: str, split: Optional[str] = None
    ):
        if split is None:
            self._extracted_vectors(vectorset).vectors.vectors.vectors.extend(vectors)
        else:
            self._extracted_vectors(vectorset).vectors.split_vectors[split].vectors.extend(vectors)

    def with_extracted_paragraph_metadata(
        self, paragraph: rpb.Paragraph, split: Optional[str] = None
    ) -> rpb.Paragraph:
        if split is None:
            self._extracted_metadata.metadata.metadata.paragraphs.append(paragraph)
            return self._extracted_metadata.metadata.metadata.paragraphs[-1]
        else:
            self._extracted_metadata.metadata.split_metadata[split].paragraphs.append(paragraph)
            return self._extracted_metadata.metadata.split_metadata[split].paragraphs[-1]

    def with_extracted_entity(
        self,
        klass: str,
        name: str,
        *,
        positions: list[rpb.Position],
        data_augmentation_task_id: str = "processor",
    ):
        self._extracted_metadata.metadata.metadata.entities[data_augmentation_task_id].entities.append(
            rpb.FieldEntity(text=name, label=klass, positions=positions)
        )

    def with_user_paragraph_labels(
        self, key: str, labelset: str, labels: list[str], split: Optional[str] = None
    ):
        classifications = labels_to_classifications(labelset, labels, split)
        pa = rpb.ParagraphAnnotation()
        pa.key = key
        pa.classifications.extend(classifications)
        self._user_metadata.paragraphs.append(pa)

    def add_paragraph(
        self,
        text: str,
        split: Optional[str] = None,
        kind: rpb.Paragraph.TypeParagraph.ValueType = rpb.Paragraph.TypeParagraph.TEXT,
        labels: Optional[list[tuple[str, list[str]]]] = None,
        user_labels: Optional[list[tuple[str, list[str]]]] = None,
        vectors: Optional[dict[str, list[float]]] = None,
    ) -> rpb.Paragraph:
        """Add a single paragraph to the field.

        This adds the paragraph at the end of the extracted text. Repeated calls
        to this will create a broker message with paragraphs on each part of the
        extracted text. However, there's nothing restricting the processor to
        skip parts of the text in their paragraph extraction. In fact, some
        legacy testing broker messages do this.

        Despite that, we've chosen a simpler behavior for this function.

        """

        if split is None:
            start = len(self._extracted_text.body.text)
            self._extracted_text.body.text += text
        else:
            start = len(self._extracted_text.body.split_text[split])
            self._extracted_text.body.split_text[split] += text

        end = start + len(text)

        paragraph = rpb.Paragraph()
        paragraph.key = ParagraphId(
            field_id=FieldId(
                rid=self._rid,
                key=self.id.field,
                type=FIELD_TYPE_PB_TO_STR[self.id.field_type],
                subfield_id=split,
            ),
            paragraph_start=start,
            paragraph_end=end,
        ).full()
        paragraph.start = start
        paragraph.end = end
        paragraph.text = text
        paragraph.kind = kind

        if labels is not None:
            for labelset, labels_ in labels:
                classifications = labels_to_classifications(labelset, labels_, split)
                paragraph.classifications.extend(classifications)

        if user_labels is not None:
            for labelset, labels_ in user_labels:
                self.with_user_paragraph_labels(paragraph.key, labelset, labels_, split)

        if vectors is not None:
            for vectorset, vector in vectors.items():
                vector_pb = utils_pb2.Vector(
                    start=start, end=end, start_paragraph=start, end_paragraph=end, vector=vector
                )
                self.with_extracted_vectors([vector_pb], vectorset, split)

        return self.with_extracted_paragraph_metadata(paragraph, split)

    def add_question_answer(
        self,
        question: str,
        answer: str,
        question_lang: str = "en",
        question_paragraph_ids: list[str] = [],
        answer_lang: str = "en",
        answer_paragraph_ids: list[str] = [],
    ):
        question_pb = rpb.Question(
            text=question,
            language=question_lang,
            ids_paragraphs=question_paragraph_ids,
        )
        answer_pb = rpb.Answers(
            text=answer,
            language=answer_lang,
            ids_paragraphs=answer_paragraph_ids,
        )

        # check if is another answer for an already added question
        for question_answer in self._question_answers.question_answers.question_answers.question_answer:
            if question_answer.question == question_pb:
                question_answer.answers.append(answer_pb)
                return

        question_answer = rpb.QuestionAnswer(question=question_pb)
        question_answer.answers.append(answer_pb)
        self._question_answers.question_answers.question_answers.question_answer.append(question_answer)

    def iter_paragraphs(self, split: Optional[str] = None) -> Iterator[rpb.Paragraph]:
        if split is None:
            paragraphs = self._extracted_metadata.metadata.metadata.paragraphs
        else:
            paragraphs = self._extracted_metadata.metadata.split_metadata[split].paragraphs

        for paragraph in paragraphs:
            yield paragraph
