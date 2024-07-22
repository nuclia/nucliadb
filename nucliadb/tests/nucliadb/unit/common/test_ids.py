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

from nucliadb.common.ids import FieldId, ParagraphId, VectorId


def test_field_ids():
    invalids = [
        "foobar",
    ]
    for invalid in invalids:
        with pytest.raises(ValueError):
            FieldId.from_string(invalid)

    field_id = FieldId.from_string("rid/u/field_id")
    assert field_id.rid == "rid"
    assert field_id.field_id == "u/field_id"
    assert field_id.subfield_id is None
    assert field_id.full() == "rid/u/field_id"

    field_id = FieldId.from_string("rid/u/field_id/subfield_id")
    assert field_id.rid == "rid"
    assert field_id.field_id == "u/field_id"
    assert field_id.subfield_id == "subfield_id"
    assert field_id.full() == "rid/u/field_id/subfield_id"
    assert field_id.field_type == "u"


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
