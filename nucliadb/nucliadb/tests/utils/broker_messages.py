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

from nucliadb_protos.resources_pb2 import (
    Classification,
    ExtractedTextWrapper,
    FieldType,
    Origin,
)
from nucliadb_protos.writer_pb2 import BrokerMessage


class BrokerMessageBuilder:
    """Helper to deal with broker message creation. It allows customized
    creation of broker messages with sensible defaults and default title

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

    def build(self) -> BrokerMessage:
        return self.bm

    def with_title(self, title: str):
        if title is not None:
            self.bm.basic.title = title
        self._add_extracted_text("title", FieldType.GENERIC, title)

    def with_summary(self, summary: str):
        if summary is not None:
            self.bm.basic.summary = summary
        self._add_extracted_text("summary", FieldType.GENERIC, summary)

    def with_message_source(self, source: BrokerMessage.MessageSource.ValueType):
        self.bm.source = source

    def with_resource_labels(self, labelset: str, labels: list[str]):
        classifications = [
            Classification(
                labelset=labelset,
                label=label,
                cancelled_by_user=False,
            )
            for label in labels
        ]
        self.bm.basic.usermetadata.classifications.extend(classifications)

    def _default_basic(self):
        self.bm.basic.icon = "text/plain"
        self.bm.basic.thumbnail = "doc"
        self.bm.basic.layout = "default"
        self.bm.basic.metadata.useful = True
        self.bm.basic.metadata.language = "en"
        self.bm.basic.created.FromDatetime(datetime.now())
        self.bm.basic.modified.FromDatetime(datetime.now())

        self.with_title("Default test resource title")

    def _default_origin(self):
        self.bm.origin.source = Origin.Source.WEB

    def _add_extracted_text(
        self, field: str, field_type: FieldType.ValueType, text: str
    ):
        etw = ExtractedTextWrapper()
        etw.field.field = field
        etw.field.field_type = field_type
        etw.body.text = text
