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
from nucliadb_protos.noderesources_pb2 import Resource as PBResource
from nucliadb_protos.resources_pb2 import (
    Basic,
    ExtractedText,
    FieldComputedMetadataWrapper,
    FieldID,
    FieldType,
    Metadata,
    Paragraph,
    Sentence,
)

from nucliadb.ingest.orm.brain import ResourceBrain, get_page_number
from nucliadb_protos import resources_pb2


def test_apply_field_metadata_marks_duplicated_paragraphs():
    # Simulate a field with two paragraphs that contain the same text
    br = ResourceBrain(rid=str(uuid4()))
    field_key = "text1"
    fcmw = FieldComputedMetadataWrapper()
    fcmw.field.CopyFrom(FieldID(field_type=FieldType.TEXT, field=field_key))
    paragraph = "Some paragraph here. "
    text_1 = f"{paragraph}{paragraph}"
    first_occurrence = [0, len(paragraph)]
    second_occurrence = [len(paragraph), len(paragraph) * 2]

    et = ExtractedText(text=text_1)
    p1 = Paragraph(start=first_occurrence[0], end=first_occurrence[1])
    p1.sentences.append(
        Sentence(start=first_occurrence[0], end=first_occurrence[1], key="test")
    )
    p2 = Paragraph(start=second_occurrence[0], end=second_occurrence[1])
    p2.sentences.append(
        Sentence(start=second_occurrence[0], end=second_occurrence[1], key="test")
    )
    fcmw.metadata.metadata.paragraphs.append(p1)
    fcmw.metadata.metadata.paragraphs.append(p2)

    br.apply_field_metadata(
        field_key,
        fcmw.metadata,
        replace_field=[],
        replace_splits={},
        page_positions={},
        extracted_text=et,
    )

    assert len(br.brain.paragraphs[field_key].paragraphs) == 2
    for key, paragraph in br.brain.paragraphs[field_key].paragraphs.items():
        if f"{first_occurrence[0]}-{first_occurrence[1]}" in key:
            # Only the first time that a paragraph is found should be set to false
            assert paragraph.repeated_in_field is False
        else:
            assert paragraph.repeated_in_field is True


def test_apply_field_metadata_marks_duplicated_paragraphs_on_split_metadata():
    # # Test now the split text path
    br = ResourceBrain(rid=str(uuid4()))
    field_key = "text1"
    split_key = "subfield"
    fcmw = FieldComputedMetadataWrapper()
    fcmw.field.CopyFrom(FieldID(field_type=FieldType.TEXT, field=field_key))
    paragraph = "Some paragraph here. "
    text_1 = f"{paragraph}{paragraph}"
    first_occurrence = [0, len(paragraph)]
    second_occurrence = [len(paragraph), len(paragraph) * 2]

    et = ExtractedText()
    et.split_text[split_key] = text_1
    p1 = Paragraph(start=first_occurrence[0], end=first_occurrence[1])
    p1.sentences.append(
        Sentence(start=first_occurrence[0], end=first_occurrence[1], key="test")
    )
    p2 = Paragraph(start=second_occurrence[0], end=second_occurrence[1])
    p2.sentences.append(
        Sentence(start=second_occurrence[0], end=second_occurrence[1], key="test")
    )
    fcmw.metadata.split_metadata[split_key].paragraphs.append(p1)
    fcmw.metadata.split_metadata[split_key].paragraphs.append(p2)

    br.apply_field_metadata(
        field_key,
        fcmw.metadata,
        replace_field=[],
        replace_splits={},
        page_positions={},
        extracted_text=et,
    )

    assert len(br.brain.paragraphs[field_key].paragraphs) == 2
    for key, paragraph in br.brain.paragraphs[field_key].paragraphs.items():
        if f"{first_occurrence[0]}-{first_occurrence[1]}" in key:
            # Only the first time that a paragraph is found should be set to false
            assert paragraph.repeated_in_field is False
        else:
            assert paragraph.repeated_in_field is True


def test_get_page_number():
    page_positions = {
        0: (0, 99),
        1: (100, 199),
        2: (200, 299),
    }
    assert get_page_number(10, page_positions) == 0
    assert get_page_number(100, page_positions) == 1
    assert get_page_number(500, page_positions) == 2


@pytest.mark.parametrize(
    "new_status,previous_status,expected_brain_status",
    [
        # No previous_status
        (Metadata.Status.PENDING, None, PBResource.PENDING),
        (Metadata.Status.PROCESSED, None, PBResource.PROCESSED),
        (Metadata.Status.ERROR, None, PBResource.PROCESSED),
        (Metadata.Status.BLOCKED, None, PBResource.PROCESSED),
        (Metadata.Status.EXPIRED, None, PBResource.PROCESSED),
        # previous_status = PENDING
        (Metadata.Status.PENDING, Metadata.Status.PENDING, PBResource.PENDING),
        (Metadata.Status.PROCESSED, Metadata.Status.PENDING, PBResource.PROCESSED),
        (Metadata.Status.ERROR, Metadata.Status.PENDING, PBResource.PROCESSED),
        (Metadata.Status.BLOCKED, Metadata.Status.PENDING, PBResource.PROCESSED),
        (Metadata.Status.EXPIRED, Metadata.Status.PENDING, PBResource.PROCESSED),
        # previous_status = PROCESSED
        (Metadata.Status.PROCESSED, Metadata.Status.PROCESSED, PBResource.PROCESSED),
        (Metadata.Status.ERROR, Metadata.Status.PROCESSED, PBResource.PROCESSED),
        (Metadata.Status.BLOCKED, Metadata.Status.PROCESSED, PBResource.PROCESSED),
        (Metadata.Status.PENDING, Metadata.Status.PROCESSED, PBResource.PROCESSED),
        (Metadata.Status.EXPIRED, Metadata.Status.PROCESSED, PBResource.PROCESSED),
        # previous_status = ERROR
        (Metadata.Status.PENDING, Metadata.Status.ERROR, PBResource.PROCESSED),
        (Metadata.Status.PROCESSED, Metadata.Status.ERROR, PBResource.PROCESSED),
        (Metadata.Status.ERROR, Metadata.Status.ERROR, PBResource.PROCESSED),
        (Metadata.Status.BLOCKED, Metadata.Status.ERROR, PBResource.PROCESSED),
        (Metadata.Status.EXPIRED, Metadata.Status.ERROR, PBResource.PROCESSED),
        # previous_status = BLOCKED
        (Metadata.Status.PENDING, Metadata.Status.BLOCKED, PBResource.PROCESSED),
        (Metadata.Status.PROCESSED, Metadata.Status.BLOCKED, PBResource.PROCESSED),
        (Metadata.Status.ERROR, Metadata.Status.BLOCKED, PBResource.PROCESSED),
        (Metadata.Status.BLOCKED, Metadata.Status.BLOCKED, PBResource.PROCESSED),
        (Metadata.Status.EXPIRED, Metadata.Status.BLOCKED, PBResource.PROCESSED),
        # previous_status = EXPIRED
        (Metadata.Status.PENDING, Metadata.Status.EXPIRED, PBResource.PROCESSED),
        (Metadata.Status.PROCESSED, Metadata.Status.EXPIRED, PBResource.PROCESSED),
        (Metadata.Status.ERROR, Metadata.Status.EXPIRED, PBResource.PROCESSED),
        (Metadata.Status.BLOCKED, Metadata.Status.EXPIRED, PBResource.PROCESSED),
        (Metadata.Status.EXPIRED, Metadata.Status.EXPIRED, PBResource.PROCESSED),
    ],
)
def test_set_processing_status(new_status, previous_status, expected_brain_status):
    br = ResourceBrain(rid="foo")
    basic = Basic()
    basic.metadata.status = new_status
    br.set_processing_status(basic, previous_status)
    assert br.brain.status == expected_brain_status


def test_apply_field_metadata_populates_page_number():
    br = ResourceBrain(rid="foo")
    field_key = "text1"

    fcmw = FieldComputedMetadataWrapper()
    fcmw.field.CopyFrom(FieldID(field_type=FieldType.TEXT, field=field_key))

    p1 = Paragraph(
        start=40, end=54, start_seconds=[0], end_seconds=[10], text="Some text here"
    )
    p1.sentences.append(Sentence(start=40, end=54, key="test"))
    fcmw.metadata.metadata.paragraphs.append(p1)

    # Add it to the split too
    fcmw.metadata.split_metadata["subfield"].paragraphs.append(p1)

    page_positions = {
        0: (0, 20),
        1: (21, 39),
        2: (40, 100),
    }
    br.apply_field_metadata(
        field_key,
        fcmw.metadata,
        replace_field=[],
        replace_splits={},
        page_positions=page_positions,
        extracted_text=None,
    )

    assert len(br.brain.paragraphs[field_key].paragraphs) == 2
    for paragraph in br.brain.paragraphs[field_key].paragraphs.values():
        assert paragraph.metadata.position.page_number == 2
        assert paragraph.metadata.position.start == 40
        assert paragraph.metadata.position.end == 54
        assert paragraph.metadata.position.start_seconds == [0]
        assert paragraph.metadata.position.end_seconds == [10]


def test_set_resource_metadata_promotes_origin_dates():
    resource_brain = ResourceBrain("rid")
    basic = Basic()
    basic.created.seconds = 1
    basic.modified.seconds = 2
    origin = resources_pb2.Origin()
    origin.created.seconds = 3
    origin.modified.seconds = 4

    resource_brain.set_resource_metadata(basic, origin)

    assert resource_brain.brain.metadata.created.seconds == 3
    assert resource_brain.brain.metadata.modified.seconds == 4


def test_set_resource_metadata_handles_timestamp_not_present():
    resource_brain = ResourceBrain("rid")
    basic = Basic()
    resource_brain.set_resource_metadata(basic, None)
    created = resource_brain.brain.metadata.created.seconds
    modified = resource_brain.brain.metadata.modified.seconds
    assert created > 0
    assert modified > 0
    assert modified >= created
