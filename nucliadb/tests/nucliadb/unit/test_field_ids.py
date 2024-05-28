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
from pydantic import BaseModel, ValidationError

from nucliadb_models.utils import FieldIdString


class DummyFieldIdModel(BaseModel):
    field_id: FieldIdString


def test_field_ids():
    """Test some examples of valid fields and exhaustively test invalid
    fields.

    """
    valid_field_ids = [
        "foo",
        "foo_bar",
        "foo-bar_123",
    ]
    for valid in valid_field_ids:
        DummyFieldIdModel(field_id=valid)

    invalid_field_ids = [
        "",
        "foo/bar",
    ]
    for invalid in invalid_field_ids:
        with pytest.raises(ValidationError):
            DummyFieldIdModel(field_id=invalid)
