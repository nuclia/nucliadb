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
from typing import Optional
from uuid import uuid4

from nucliadb_protos import resources_pb2 as rpb
from nucliadb_protos import writer_pb2 as wpb

from .fields import FieldBuilder
from .helpers import labels_to_classifications


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
        self.bm = wpb.BrokerMessage()
        self.fields: dict[tuple[str, rpb.FieldType.ValueType], FieldBuilder] = {}

        self.bm.kbid = kbid
        self.bm.type = wpb.BrokerMessage.AUTOCOMMIT

        # if first BM comes from PROCESSOR, it'll be ignored as it's out of order
        self.bm.source = wpb.BrokerMessage.MessageSource.WRITER

        if rid is None:
            rid = str(uuid4())
        self.bm.uuid = rid

        if slug is None:
            slug = f"{rid}-slug"
        self.bm.slug = slug

        self._default_basic()
        self._default_origin()

    def build(self) -> wpb.BrokerMessage:
        self._apply_fields()
        return self.bm

    def add_field_builder(self, field: FieldBuilder):
        self.fields[(field.id.field, field.id.field_type)] = field

    def field_builder(
        self, field_id: str, field_type: rpb.FieldType.ValueType
    ) -> FieldBuilder:
        return self.fields[(field_id, field_type)]

    def with_title(self, title: str):
        title_builder = FieldBuilder("title", rpb.FieldType.GENERIC)
        title_builder.with_extracted_text(title)
        # we do this to writer BMs in write resource API endpoint
        title_builder.with_extracted_paragraph_metadata(
            rpb.Paragraph(
                start=0,
                end=len(title),
                kind=rpb.Paragraph.TypeParagraph.TITLE,
            )
        )
        self.bm.basic.title = title
        self.add_field_builder(title_builder)

    def with_summary(self, summary: str):
        summary_builder = FieldBuilder("summary", rpb.FieldType.GENERIC)
        summary_builder.with_extracted_text(summary)
        # we do this to writer BMs in write resource API endpoint
        summary_builder.with_extracted_paragraph_metadata(
            rpb.Paragraph(
                start=0,
                end=len(summary),
                kind=rpb.Paragraph.TypeParagraph.DESCRIPTION,
            )
        )
        self.bm.basic.summary = summary
        self.add_field_builder(summary_builder)

    def with_resource_labels(self, labelset: str, labels: list[str]):
        classifications = labels_to_classifications(labelset, labels)
        self.bm.basic.usermetadata.classifications.extend(classifications)

    def _default_basic(self):
        self.bm.basic.icon = "text/plain"
        self.bm.basic.thumbnail = "doc"
        self.bm.basic.layout = "default"
        self.bm.basic.metadata.useful = True
        self.bm.basic.metadata.language = "en"
        self.bm.basic.metadata.status = rpb.Metadata.Status.PROCESSED
        self.bm.basic.metadata.metadata["key"] = "value"
        self.bm.basic.created.FromDatetime(datetime.now())
        self.bm.basic.modified.FromDatetime(datetime.now())

        self.with_title("Default test resource title")
        self.with_summary("Default test resource summary")

    def _default_origin(self):
        self.bm.origin.source = rpb.Origin.Source.API
        self.bm.origin.source_id = "My Source"
        self.bm.origin.created.FromDatetime(datetime.now())
        self.bm.origin.modified.FromDatetime(datetime.now())

    def _apply_fields(self):
        def replace_if_exists(mut_iterable, field_id: rpb.FieldID, item):
            for obj in mut_iterable:
                if obj.field == field_id:
                    obj.Clear()
                    obj.CopyFrom(item)
                    break
            else:
                mut_iterable.append(item)

        for field_builder in self.fields.values():
            field = field_builder.build()

            if field.id.field_type == rpb.FieldType.GENERIC:
                pass
            elif field.id.field_type == rpb.FieldType.FILE:
                file_field = self.bm.files[field.id.field]
                file_field.added.FromDatetime(datetime.now())
                file_field.file.source = rpb.CloudFile.Source.EXTERNAL
            else:
                raise Exception("Unsupported field type")

            if field.user.metadata is not None:
                replace_if_exists(
                    self.bm.basic.fieldmetadata, field.id, field.user.metadata
                )
            if field.extracted.metadata is not None:
                replace_if_exists(
                    self.bm.field_metadata, field.id, field.extracted.metadata
                )
            if field.extracted.text is not None:
                replace_if_exists(
                    self.bm.extracted_text, field.id, field.extracted.text
                )
            if field.extracted.question_answers is not None:
                replace_if_exists(
                    self.bm.question_answers, field.id, field.extracted.question_answers
                )
