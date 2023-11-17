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
from unittest import mock
from unittest.mock import Mock, call

import pytest

from nucliadb.search.search.filters import (
    record_filters_counter,
    split_filters_by_label_type,
)


def test_record_filters_counter():
    counter = Mock()

    record_filters_counter(["", "/l/ls/l1", "/e/ORG/Nuclia"], counter)

    counter.inc.assert_has_calls(
        [
            call({"type": "filters"}),
            call({"type": "filters_entities"}),
            call({"type": "filters_labels"}),
        ]
    )


@pytest.fixture(scope="function")
def is_paragraph_labelset_kind_mock():
    with mock.patch(
        "nucliadb.search.search.filters.is_paragraph_labelset_kind"
    ) as mocked:
        yield mocked


async def test_split_filters_by_label_type(is_paragraph_labelset_kind_mock):
    is_paragraph_labelset_kind_mock.return_value = False
    field_labels, paragraph_labels = await split_filters_by_label_type(
        "kbid", ["/e/foo/bar", "/l/bar/baa", "foo/"]
    )
    assert field_labels == ["/e/foo/bar", "/l/bar/baa"]
    assert paragraph_labels == []

    is_paragraph_labelset_kind_mock.return_value = True
    field_labels, paragraph_labels = await split_filters_by_label_type(
        "kbid", ["/e/foo/bar", "/l/bar/baa"]
    )
    assert field_labels == ["/e/foo/bar"]
    assert paragraph_labels == ["/l/bar/baa"]
