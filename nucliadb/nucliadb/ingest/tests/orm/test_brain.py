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
from uuid import uuid4

import pytest
from nucliadb_protos.resources_pb2 import (
    FieldComputedMetadataWrapper,
    FieldID,
    FieldType,
    Paragraph,
    Sentence,
)

from nucliadb.ingest.orm.brain import ResourceBrain


@pytest.mark.asyncio
async def test_apply_field_metadata_marks_duplicated_paragraphs():
    br = ResourceBrain(rid=str(uuid4()))
    field_key = "text1"

    fcmw = FieldComputedMetadataWrapper()
    fcmw.field.CopyFrom(FieldID(field_type=FieldType.TEXT, field=field_key))

    # Simulate a field with two paragraphs that contain the same text
    some_text = "Some text here. And another sentence here"
    p1 = Paragraph(start=0, end=20, text=some_text)
    p1.sentences.append(Sentence(start=0, end=10, key="test"))
    p1.sentences.append(Sentence(start=11, end=20, key="test"))
    fcmw.metadata.metadata.paragraphs.append(p1)

    p2 = Paragraph(start=40, end=60, text=some_text)
    p2.sentences.append(Sentence(start=0, end=10, key="test"))
    p2.sentences.append(Sentence(start=11, end=20, key="test"))
    fcmw.metadata.metadata.paragraphs.append(p2)

    # Add it to split too
    fcmw.metadata.split_metadata["subfield"].paragraphs.append(p1)
    fcmw.metadata.split_metadata["subfield"].paragraphs.append(p2)

    br.apply_field_metadata(field_key, fcmw.metadata, [], {})

    assert len(br.brain.paragraphs[field_key].paragraphs) == 4
    for key, paragraph in br.brain.paragraphs[field_key].paragraphs.items():
        if "subfield" in key and "0-20" in key:
            # Only the first time that a paragraph is found should be set to false
            assert paragraph.repeated_in_field is False
        else:
            assert paragraph.repeated_in_field is True
