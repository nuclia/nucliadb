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
from nucliadb_protos.resources_pb2 import FieldComputedMetadata, Paragraph
from nucliadb_protos.utils_pb2 import ExtractedText

from nucliadb.search.api.v1.resource.ask import (
    get_field_blocks,
    get_field_blocks_split_by_paragraphs,
)


def test_get_field_blocks():
    etxt = ExtractedText(text="Hello World")
    assert get_field_blocks(etxt) == ["Hello World"]

    # split text
    etxt = ExtractedText()
    etxt.split_text["foo"] = "Hello World"
    etxt.split_text["bar"] = "I am here"
    assert get_field_blocks(etxt) == ["I am here", "Hello World"]


def test_get_field_blocks_split_by_paragraphs():
    etxt = ExtractedText(text="Hello World")
    fcm = FieldComputedMetadata()
    p1 = Paragraph(start=0, end=5)
    p2 = Paragraph(start=6, end=11)
    fcm.metadata.paragraphs.append(p1)
    fcm.metadata.paragraphs.append(p2)
    assert get_field_blocks_split_by_paragraphs(etxt, fcm) == ["Hello", "World"]

    # split text
    etxt = ExtractedText()
    etxt.split_text["foo"] = "Hello World"
    etxt.split_text["bar"] = "I am here"
    fcm = FieldComputedMetadata()
    p1 = Paragraph(start=0, end=5)
    p2 = Paragraph(start=6, end=11)
    p3 = Paragraph(start=0, end=4)
    p4 = Paragraph(start=5, end=9)
    fcm.split_metadata["foo"].paragraphs.append(p1)
    fcm.split_metadata["foo"].paragraphs.append(p2)
    fcm.split_metadata["bar"].paragraphs.append(p3)
    fcm.split_metadata["bar"].paragraphs.append(p4)
    assert get_field_blocks_split_by_paragraphs(etxt, fcm) == [
        "I am",
        "here",
        "Hello",
        "World",
    ]
