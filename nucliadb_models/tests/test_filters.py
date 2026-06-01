# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from datetime import datetime, timedelta

import pytest
from pydantic_core import ValidationError

from nucliadb_models import filters
from nucliadb_models.common import FieldTypeName


@pytest.mark.parametrize(
    "gte,lte",
    [
        # must select a filter
        (None, None),
        # combine int/float with dates
        (10, datetime.now()),
        (3.5, datetime.now()),
        (datetime.now(), 10),
        (datetime.now(), 3.5),
        # lte < gte
        (20, 10),
        (20.0, 10),
        (20, 10.0),
        (20.0, 10.0),
        (datetime.now(), datetime.now() - timedelta(days=1)),
    ],
)
def test_key_value_inequality_filter_bounds_check(
    gte: int | float | datetime | None,
    lte: int | float | datetime | None,
):
    with pytest.raises(ValidationError):
        filters.Inequalities(schema_id="schema", key="k", gte=gte, lte=lte)


def test_resource_field_prefix_filter_resource_id_or_slug_check():
    with pytest.raises(ValidationError):
        filters.ResourceFieldPrefix(
            resource_id=None,
            resource_slug=None,
            field_type=FieldTypeName.CONVERSATION,
            field_name_prefix="foo",
        )
