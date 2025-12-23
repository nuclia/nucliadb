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
        rid: str | None = None,
        slug: str | None = None,
        source: wpb.BrokerMessage.MessageSource.ValueType | None = None,
    ):
        self.bm = wpb.BrokerMessage()
        self.fields: dict[tuple[str, rpb.FieldType.ValueType], FieldBuilder] = {}

        self.bm.kbid = kbid
        self.bm.type = wpb.BrokerMessage.AUTOCOMMIT

        if source is not None:
            self.bm.source = source
        else:
            # if first BM comes from PROCESSOR, it'll be ignored as it's out of order
            self.bm.source = wpb.BrokerMessage.MessageSource.WRITER

        if rid is None:
            rid = str(uuid4())
        self.rid = rid
        self.bm.uuid = rid

        if slug is None:
            slug = f"{rid}-slug"
        self.bm.slug = slug

        self._default_basic()
        self._default_origin()

    def build(self) -> wpb.BrokerMessage:
        self._apply_fields()
        return self.bm

    def field_builder(self, field_id: str, field_type: rpb.FieldType.ValueType) -> FieldBuilder:
        """Get a field builder for a specific field id and type. If the builder
        didn't have this field builder, add it and return it.

        """
        if (field_id, field_type) not in self.fields:
            field_builder = FieldBuilder(self.bm.kbid, self.rid, field_id, field_type)
            self.fields[(field_id, field_type)] = field_builder
        return self.fields[(field_id, field_type)]

    def with_title(self, title: str) -> FieldBuilder:
        title_builder = self.field_builder("title", rpb.FieldType.GENERIC)
        title_builder.add_paragraph(
            text=title,
            # we do this to writer BMs in write resource API endpoint
            kind=rpb.Paragraph.TypeParagraph.TITLE,
        )
        self.bm.basic.title = title
        return title_builder

    def with_summary(self, summary: str) -> FieldBuilder:
        summary_builder = self.field_builder("summary", rpb.FieldType.GENERIC)
        summary_builder.add_paragraph(
            text=summary,
            # we do this to writer BMs in write resource API endpoint
            kind=rpb.Paragraph.TypeParagraph.DESCRIPTION,
        )
        self.bm.basic.summary = summary
        return summary_builder

    def with_resource_labels(self, labelset: str, labels: list[str]):
        classifications = labels_to_classifications(labelset, labels)
        self.bm.basic.usermetadata.classifications.extend(classifications)

    def _default_basic(self):
        self.bm.basic.icon = "text/plain"
        self.bm.basic.thumbnail = "doc"
        self.bm.basic.metadata.useful = True
        self.bm.basic.metadata.language = "en"

        if self.bm.source == wpb.BrokerMessage.MessageSource.WRITER:
            self.bm.basic.metadata.status = rpb.Metadata.Status.PENDING
        elif self.bm.source == wpb.BrokerMessage.MessageSource.PROCESSOR:
            self.bm.basic.metadata.status = rpb.Metadata.Status.PROCESSED
        else:
            raise ValueError(f"Unknown broker message source: {self.bm.source}")

        self.bm.basic.metadata.metadata["key"] = "value"
        self.bm.basic.created.FromDatetime(datetime.now())
        self.bm.basic.modified.FromDatetime(datetime.now())

    def _default_origin(self) -> None:
        self.bm.origin.source = rpb.Origin.Source.API
        self.bm.origin.source_id = "My Source"
        self.bm.origin.created.FromDatetime(datetime.now())
        self.bm.origin.modified.FromDatetime(datetime.now())

    def _apply_fields(self) -> None:
        # clear broker message fields before adding data
        self.bm.basic.ClearField("fieldmetadata")
        self.bm.ClearField("field_metadata")
        self.bm.ClearField("extracted_text")
        self.bm.ClearField("field_vectors")
        self.bm.ClearField("question_answers")

        for field_builder in self.fields.values():
            field = field_builder.build()

            if field.id.field_type == rpb.FieldType.GENERIC:
                # we don't need to do anything else
                pass

            elif field.id.field_type in (rpb.FieldType.TEXT, rpb.FieldType.CONVERSATION):
                assert field.extracted.text is not None, (
                    "only text fields with extracted data are supported nowadays"
                )

            elif field.id.field_type == rpb.FieldType.FILE:
                file_field = self.bm.files[field.id.field]
                file_field.added.FromDatetime(datetime.now())
                file_field.file.source = rpb.CloudFile.Source.EXTERNAL

                if field.extracted.file is not None:
                    self.bm.file_extracted_data.append(field.extracted.file)

            elif (
                field.id.field_type == rpb.FieldType.LINK
                and self.bm.source == wpb.BrokerMessage.MessageSource.PROCESSOR
            ):
                if field.extracted.link is not None:
                    self.bm.link_extracted_data.append(field.extracted.link)

            else:  # pragma: no cover
                raise Exception(f"Unsupported field type: {field.id.field_type}")

            if field.user.metadata is not None:
                self.bm.basic.fieldmetadata.append(field.user.metadata)
            if field.extracted.metadata is not None:
                self.bm.field_metadata.append(field.extracted.metadata)
            if field.extracted.text is not None:
                self.bm.extracted_text.append(field.extracted.text)
            if field.extracted.vectors is not None:
                self.bm.field_vectors.extend(field.extracted.vectors)
            if field.extracted.question_answers is not None:
                self.bm.question_answers.append(field.extracted.question_answers)
