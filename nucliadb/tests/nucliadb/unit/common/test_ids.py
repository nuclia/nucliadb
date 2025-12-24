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


import pytest

from nucliadb.common.ids import (
    FIELD_TYPE_PB_TO_STR,
    FieldId,
    ParagraphId,
    VectorId,
    extract_data_augmentation_id,
)
from nucliadb_protos.resources_pb2 import FieldType


def test_field_ids():
    invalids = [
        "foobar",
    ]
    for invalid in invalids:
        with pytest.raises(ValueError):
            FieldId.from_string(invalid)

    field_id = FieldId.from_string("rid/u/field_id")
    assert field_id.rid == "rid"
    assert field_id.type == "u"
    assert field_id.key == "field_id"
    assert field_id.subfield_id is None
    assert field_id.full() == "rid/u/field_id"

    field_id = FieldId.from_string("rid/u/field_id/subfield_id")
    assert field_id.rid == "rid"
    assert field_id.type == "u"
    assert field_id.key == "field_id"
    assert field_id.subfield_id == "subfield_id"
    assert field_id.full() == "rid/u/field_id/subfield_id"
    assert field_id.type == "u"
    assert field_id.pb_type == FieldType.LINK

    field_id = FieldId.from_pb("rid", FieldType.LINK, "field_id", subfield_id="subfield_id")
    assert field_id.rid == "rid"
    assert field_id.type == "u"
    assert field_id.key == "field_id"
    assert field_id.subfield_id == "subfield_id"
    assert field_id.full() == "rid/u/field_id/subfield_id"


def test_field_ids_int_field_type():
    # Test that we can use integers as field types
    for value in FieldType.values():
        field_id = FieldId.from_string(f"rid/{value}/field_id/subfield_id")
        assert field_id.rid == "rid"
        assert field_id.type == FIELD_TYPE_PB_TO_STR[value]
        assert field_id.key == "field_id"
        assert field_id.subfield_id == "subfield_id"
        assert field_id.full() == f"rid/{FIELD_TYPE_PB_TO_STR[value]}/field_id/subfield_id"
        assert field_id.pb_type == value


def test_paragraph_ids():
    invalids = [
        "foobar",
    ]
    for invalid in invalids:
        with pytest.raises(ValueError):
            ParagraphId.from_string(invalid)

    paragraph_id = ParagraphId.from_string("rid/u/field_id/0-10")
    assert paragraph_id.field_id.full() == "rid/u/field_id"
    assert paragraph_id.paragraph_start == 0
    assert paragraph_id.paragraph_end == 10

    paragraph_id = ParagraphId.from_string("rid/u/field_id/subfield_id/0-10")
    assert paragraph_id.field_id.full() == "rid/u/field_id/subfield_id"
    assert paragraph_id.paragraph_start == 0
    assert paragraph_id.paragraph_end == 10

    vid = VectorId.from_string("rid/u/field_id/0/0-10")
    paragraph_id = ParagraphId.from_vector_id(vid)
    assert paragraph_id.field_id.full() == "rid/u/field_id"
    assert paragraph_id.paragraph_start == 0
    assert paragraph_id.paragraph_end == 10
    assert paragraph_id.full() == "rid/u/field_id/0-10"


def test_vector_ids():
    invalids = [
        "foobar",
    ]
    for invalid in invalids:
        with pytest.raises(ValueError):
            VectorId.from_string(invalid)

    vector_id = VectorId.from_string("rid/u/field_id/0/0-10")
    assert vector_id.field_id.full() == "rid/u/field_id"
    assert vector_id.index == 0
    assert vector_id.vector_start == 0
    assert vector_id.vector_end == 10

    vector_id = VectorId.from_string("rid/u/field_id/subfield_id/1/10-20")
    assert vector_id.field_id.full() == "rid/u/field_id/subfield_id"
    assert vector_id.index == 1
    assert vector_id.vector_start == 10
    assert vector_id.vector_end == 20


@pytest.mark.parametrize(
    "task_id,field_type,field_id,split",
    [
        ("mytask", "t", "mytext", None),
        ("mytask", "t", "mytext", "mysplit"),
        ("mytask", "t", "my-text", "my-split"),  # dashes in field ids are valid
    ],
)
def test_valid_data_augmentation_id_extraction(
    task_id: str,
    field_type: str,
    field_id: str,
    split: str | None,
):
    if split is not None:
        gen_field_id = f"da-{task_id}-{field_type}-{field_id}-{split}"
    else:
        gen_field_id = f"da-{task_id}-{field_type}-{field_id}"

    assert extract_data_augmentation_id(gen_field_id) == task_id


@pytest.mark.parametrize(
    "task_id,field_type,field_id,split",
    [
        ("my-task", "t", "mytext", None),  # task_id can't contain `-`
        ("", "t", "mytext", None),  # empty task_id
    ],
)
def test_invalid_data_augmentation_id_extraction(
    task_id: str,
    field_type: str,
    field_id: str,
    split: str | None,
):
    if split is not None:
        gen_field_id = f"da-{task_id}-{field_type}-{field_id}-{split}"
    else:
        gen_field_id = f"da-{task_id}-{field_type}-{field_id}"

    assert (
        extract_data_augmentation_id(gen_field_id) is None
        or extract_data_augmentation_id(gen_field_id) != task_id
    )


@pytest.mark.parametrize(
    "gen_field_id",
    [
        "not-starting-with-da",
        "da-butnomoredashes",
    ],
)
def test_invalid_data_augmentation_id_extraction_2(
    gen_field_id: str,
):
    assert extract_data_augmentation_id(gen_field_id) is None
