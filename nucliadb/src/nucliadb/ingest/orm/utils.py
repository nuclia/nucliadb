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

import urllib.parse
from typing import Sequence

from nucliadb.ingest.processing import PushPayload
from nucliadb_models.text import PushTextFormat, Text
from nucliadb_protos.resources_pb2 import (
    ExtractedTextWrapper,
    FieldComputedMetadataWrapper,
    FieldType,
    Metadata,
    Paragraph,
)
from nucliadb_protos.writer_pb2 import BrokerMessage


def set_title(writer: BrokerMessage, toprocess: PushPayload, title: str):
    title = urllib.parse.unquote(title)
    writer.basic.title = title
    etw = ExtractedTextWrapper()
    etw.field.field = "title"
    etw.field.field_type = FieldType.GENERIC
    etw.body.text = title
    writer.extracted_text.append(etw)
    fmw = FieldComputedMetadataWrapper()
    paragraph = Paragraph(start=0, end=len(title), kind=Paragraph.TypeParagraph.TITLE)
    fmw.metadata.metadata.paragraphs.append(paragraph)
    fmw.field.field = "title"
    fmw.field.field_type = FieldType.GENERIC
    writer.field_metadata.append(fmw)
    writer.basic.metadata.useful = True
    writer.basic.metadata.status = Metadata.Status.PENDING

    toprocess.genericfield["title"] = Text(body=title, format=PushTextFormat.PLAIN)


def compute_paragraph_key(rid: str, paragraph_key: str) -> str:
    return paragraph_key.replace("N_RID", rid)


def choose_matryoshka_dimension(dimensions: Sequence[int]) -> int:
    """Given a list of matryoshka embedding available dimensions, choose one to
    set the vector dimension.
    """
    if len(dimensions) == 0:
        raise ValueError("Can't choose matryoshka dimension from an empty list")

    threshold = 2000
    previous = None
    for dimension in sorted(dimensions):
        if dimension > threshold:
            break
        previous = dimension

    if dimension > threshold:
        if previous is None:
            return dimension
        else:
            return previous
    return dimension
