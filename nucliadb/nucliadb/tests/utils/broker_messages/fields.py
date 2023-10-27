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

from nucliadb_protos.resources_pb2 import (
    ExtractedTextWrapper,
    FieldComputedMetadataWrapper,
    FieldID,
    FieldType,
    Paragraph,
    ParagraphAnnotation,
    UserFieldMetadata,
)

from .helpers import labels_to_classifications


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
