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

import jsonschema  # type: ignore
import pytest

from nucliadb.search.search.filters import (
    INDEX_NODE_FILTERS_SCHEMA,
    convert_filter_to_node_schema,
    convert_to_node_filters,
    iter_filter_labels_expression,
    translate_label_filters,
)
from nucliadb_models.search import Filter


@pytest.fixture(scope="function")
def is_paragraph_labelset_kind_mock():
    with mock.patch(
        "nucliadb.search.search.filters.is_paragraph_labelset_kind"
    ) as mocked:
        yield mocked


@pytest.mark.parametrize(
    "original,converted",
    [
        ("foo", {"literal": "foo"}),
        (Filter(all=["foo"]), {"literal": "foo"}),
        (Filter(all=["foo", "bar"]), {"and": [{"literal": "foo"}, {"literal": "bar"}]}),
        (Filter(any=["foo"]), {"literal": "foo"}),
        (Filter(any=["foo", "bar"]), {"or": [{"literal": "foo"}, {"literal": "bar"}]}),
        (Filter(none=["foo"]), {"not": {"literal": "foo"}}),
        (
            Filter(none=["foo", "bar"]),
            {"not": {"or": [{"literal": "foo"}, {"literal": "bar"}]}},
        ),
        (Filter(not_all=["foo"]), {"not": {"literal": "foo"}}),
        (
            Filter(not_all=["foo", "bar"]),
            {"not": {"and": [{"literal": "foo"}, {"literal": "bar"}]}},
        ),
    ],
)
def test_convert_filter_to_node_schema(original, converted):
    assert convert_filter_to_node_schema(original) == converted
    jsonschema.validate(converted, INDEX_NODE_FILTERS_SCHEMA)


def test_convert_to_node_filters():
    assert convert_to_node_filters([]) == {}
    assert convert_to_node_filters(["foo"]) == {"literal": "foo"}
    assert convert_to_node_filters(["foo", "bar"]) == {
        "and": [{"literal": "foo"}, {"literal": "bar"}]
    }
    assert convert_to_node_filters([Filter(all=["foo"])]) == {"literal": "foo"}
    assert convert_to_node_filters([Filter(all=["foo"]), Filter(any=["bar"])]) == {
        "and": [{"literal": "foo"}, {"literal": "bar"}]
    }


def test_translate_label_filters():
    literal = {"literal": "/classification.labels/foo/bar"}
    translated = {"literal": "/l/foo/bar"}

    assert translate_label_filters(literal) == translated
    assert translate_label_filters({"not": literal}) == {"not": translated}
    assert translate_label_filters({"and": [literal, literal]}) == {
        "and": [translated, translated]
    }
    assert translate_label_filters({"or": [literal, literal]}) == {
        "or": [translated, translated]
    }
    assert translate_label_filters(
        {"and": [{"or": [literal, literal]}, {"not": literal}]}
    ) == {
        "and": [
            {"or": [translated, translated]},
            {"not": translated},
        ]
    }


def test_iter_filter_labels_expression():
    literal = {"literal": "foo"}
    assert list(iter_filter_labels_expression(literal)) == ["foo"]
    assert list(iter_filter_labels_expression({"and": [literal, literal]})) == [
        "foo",
        "foo",
    ]
    assert list(iter_filter_labels_expression({"or": [literal, literal]})) == [
        "foo",
        "foo",
    ]
    assert list(
        iter_filter_labels_expression({"not": {"and": [literal, literal]}})
    ) == ["foo", "foo"]


def test_filters_model():
    f = Filter(all=["foo", "bar"], any=None)
    assert f.all == ["foo", "bar"]
    assert f.any is None
    assert f.none is None
    assert f.not_all is None

    with pytest.raises(ValueError):
        Filter(all=["foo"], any=["bar"])
