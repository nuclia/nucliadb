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

import pytest

from nucliadb.search.search.exceptions import InvalidQueryError
from nucliadb.search.search.filters import (
    has_classification_label_filters,
    iter_labels,
    translate_expression_labels,
    translate_label,
)
from nucliadb_models.filtering import AND


@pytest.fixture(scope="function", autouse=True)
def is_paragraph_labelset_kind_mock():
    with mock.patch(
        "nucliadb.search.search.filters.is_paragraph_labelset_kind"
    ) as mocked:
        yield mocked


def test_has_classification_label_filters():
    assert not has_classification_label_filters(["foo", "bar"])
    assert has_classification_label_filters(["foo", "/l/labelset/label"])
    assert not has_classification_label_filters({"and": ["foo", "bar"]})
    assert has_classification_label_filters(AND(AND("foo", "/l/labelset/label"), "baz"))


def test_iter_labels():
    assert list(iter_labels("foo")) == ["foo"]
    assert list(iter_labels({"and": ["foo", "bar"]})) == ["foo", "bar"]
    assert list(iter_labels({"and": ["foo", {"not": {"or": ["ba", "blanca"]}}]})) == [
        "foo",
        "ba",
        "blanca",
    ]


def test_translate_expression_labels():
    expression = {
        "and": [
            "/classification.labels/labelset/label",
            {"or": ["/entities/GPE/Barcelona", "/icon/pdf"]},
        ],
        "or": [
            "/classification.labels/labelset/label",
            {"or": ["/entities/GPE/Barcelona", "/icon/pdf"]},
        ],
        "not": [
            "/classification.labels/labelset/label",
            {"or": ["/entities/GPE/Barcelona", "/icon/pdf"]},
        ],
    }
    assert translate_expression_labels(expression) == {
        "and": [
            "/l/labelset/label",
            {"or": ["/e/GPE/Barcelona", "/n/i/pdf"]},
        ],
        "or": [
            "/l/labelset/label",
            {"or": ["/e/GPE/Barcelona", "/n/i/pdf"]},
        ],
        "not": [
            "/l/labelset/label",
            {"or": ["/e/GPE/Barcelona", "/n/i/pdf"]},
        ],
    }


def test_translate_label():
    assert (
        translate_label("/classification.labels/labelset/label") == "/l/labelset/label"
    )

    for invalid_filter in [
        "",
        "something_without_initial_slash",
    ]:
        with pytest.raises(InvalidQueryError):
            translate_label(invalid_filter)
