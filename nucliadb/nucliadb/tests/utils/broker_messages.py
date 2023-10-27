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
from typing import Optional
from uuid import uuid4

from nucliadb_protos.resources_pb2 import (
    Classification,
    ExtractedTextWrapper,
    FieldComputedMetadataWrapper,
    FieldID,
    FieldType,
    Metadata,
    Origin,
    Paragraph,
    ParagraphAnnotation,
    UserFieldMetadata,
)
from nucliadb_protos.writer_pb2 import BrokerMessage


@dataclasses.dataclass
class FieldUser:
    metadata: Optional[UserFieldMetadata] = None


@dataclasses.dataclass
class FieldExtracted:
    metadata: Optional[FieldComputedMetadataWrapper] = None
    text: Optional[ExtractedTextWrapper] = None


@dataclasses.dataclass
class Field:
    id: FieldID
    user: FieldUser = dataclasses.field(default_factory=FieldUser)
    extracted: FieldExtracted = dataclasses.field(default_factory=FieldExtracted)


class FieldBuilder:
    def __init__(self, field: str, field_type: FieldType.ValueType):
        self._field_id = FieldID(field=field, field_type=field_type)
        self.__extracted_metadata: Optional[FieldComputedMetadataWrapper] = None
        self.__extracted_text: Optional[ExtractedTextWrapper] = None
        self.__user_metadata: Optional[UserFieldMetadata] = None

    @property
    def _extracted_metadata(self) -> FieldComputedMetadataWrapper:
        if self.__extracted_metadata is None:
            now = datetime.now()
            self.__extracted_metadata = FieldComputedMetadataWrapper(
                field=self._field_id,
            )
            self.__extracted_metadata.metadata.metadata.last_index.FromDatetime(now)
            self.__extracted_metadata.metadata.metadata.last_understanding.FromDatetime(
                now
            )
            self.__extracted_metadata.metadata.metadata.last_extract.FromDatetime(now)
        return self.__extracted_metadata

    @property
    def _extracted_text(self) -> ExtractedTextWrapper:
        if self.__extracted_text is None:
            self.__extracted_text = ExtractedTextWrapper(field=self._field_id)
        return self.__extracted_text

    @property
    def _user_metadata(self) -> UserFieldMetadata:
        if self.__user_metadata is None:
            self.__user_metadata = UserFieldMetadata(field=self._field_id)
        return self.__user_metadata

    def build(self) -> Field:
        field = Field(id=self._field_id)

        if self.__extracted_metadata is not None:
            field.extracted.metadata = FieldComputedMetadataWrapper()
            field.extracted.metadata.CopyFrom(self.__extracted_metadata)

        if self.__extracted_text is not None:
            field.extracted.text = ExtractedTextWrapper()
            field.extracted.text.CopyFrom(self.__extracted_text)

        if self.__user_metadata is not None:
            field.user.metadata = UserFieldMetadata()
            field.user.metadata.CopyFrom(self.__user_metadata)

        return field

    def with_extracted_labels(self, labelset: str, labels: list[str]):
        classifications = labels_to_classifications(labelset, labels)
        self._extracted_metadata.metadata.metadata.classifications.extend(
            classifications
        )

    def with_extracted_text(self, text: str):
        self._extracted_text.body.text = text

    def with_extracted_paragraph_metadata(self, paragraph: Paragraph):
        self._extracted_metadata.metadata.metadata.paragraphs.append(paragraph)

    def with_user_paragraph_labels(self, key: str, labelset: str, labels: list[str]):
        classifications = labels_to_classifications(labelset, labels)
        pa = ParagraphAnnotation()
        pa.key = key
        pa.classifications.extend(classifications)
        self._user_metadata.paragraphs.append(pa)


class BrokerMessageBuilder:
    """Helper to deal with broker message creation. It allows customized
    creation of broker messages with sensible defaults and default title and
    summary.

    """

    def __init__(
        self,
        *,
        kbid: str,
        rid: Optional[str] = None,
        slug: Optional[str] = None,
    ):
        self.bm = BrokerMessage()
        self.bm.kbid = kbid
        self.bm.type = BrokerMessage.AUTOCOMMIT

        # if first BM comes from PROCESSOR, it'll be ignored as it's out of order
        self.bm.source = BrokerMessage.MessageSource.WRITER

        if rid is None:
            rid = str(uuid4())
        self.bm.uuid = rid

        if slug is None:
            slug = f"{rid}-slug"
        self.bm.slug = slug

        self._default_basic()
        self._default_origin()

    def build(self) -> BrokerMessage:
        return self.bm

    def add_field(self, field: Field):
        def replace_if_exists(mut_iterable, field_id: FieldID, item):
            for obj in mut_iterable:
                if obj.field == field_id:
                    obj.Clear()
                    obj.CopyFrom(item)
                    break
            else:
                mut_iterable.append(item)

        if field.user.metadata is not None:
            replace_if_exists(
                self.bm.basic.fieldmetadata, field.id, field.user.metadata
            )

        if field.extracted.metadata is not None:
            replace_if_exists(
                self.bm.field_metadata, field.id, field.extracted.metadata
            )

        if field.extracted.text is not None:
            replace_if_exists(self.bm.extracted_text, field.id, field.extracted.text)

    def with_title(self, title: str):
        title_field = FieldBuilder("title", FieldType.GENERIC)
        title_field.with_extracted_text(title)
        # we do this to writer BMs in write resource API endpoint
        title_field.with_extracted_paragraph_metadata(
            Paragraph(
                start=0,
                end=len(title),
                kind=Paragraph.TypeParagraph.TITLE,
            )
        )
        self.bm.basic.title = title
        self.add_field(title_field.build())

    def with_summary(self, summary: str):
        summary_field = FieldBuilder("summary", FieldType.GENERIC)
        summary_field.with_extracted_text(summary)
        # we do this to writer BMs in write resource API endpoint
        summary_field.with_extracted_paragraph_metadata(
            Paragraph(
                start=0,
                end=len(summary),
                kind=Paragraph.TypeParagraph.DESCRIPTION,
            )
        )
        self.bm.basic.summary = summary
        self.add_field(summary_field.build())

    def with_resource_labels(self, labelset: str, labels: list[str]):
        classifications = labels_to_classifications(labelset, labels)
        self.bm.basic.usermetadata.classifications.extend(classifications)

    def _default_basic(self):
        self.bm.basic.icon = "text/plain"
        self.bm.basic.thumbnail = "doc"
        self.bm.basic.layout = "default"
        self.bm.basic.metadata.useful = True
        self.bm.basic.metadata.language = "en"
        self.bm.basic.metadata.status = Metadata.Status.PROCESSED
        self.bm.basic.metadata.metadata["key"] = "value"
        self.bm.basic.created.FromDatetime(datetime.now())
        self.bm.basic.modified.FromDatetime(datetime.now())

        self.with_title("Default test resource title")
        self.with_summary("Default test resource summary")

    def _default_origin(self):
        self.bm.origin.source = Origin.Source.API
        self.bm.origin.source_id = "My Source"
        self.bm.origin.created.FromDatetime(datetime.now())
        self.bm.origin.modified.FromDatetime(datetime.now())


def labels_to_classifications(labelset: str, labels: list[str]) -> list[Classification]:
    classifications = [
        Classification(
            labelset=labelset,
            label=label,
            cancelled_by_user=False,
        )
        for label in labels
    ]
    return classifications
