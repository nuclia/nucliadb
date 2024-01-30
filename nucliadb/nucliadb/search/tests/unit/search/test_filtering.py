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

import pytest

from nucliadb_models.filtering import AND, NOT, OR, convert_to_v2, validate


def test_helper_and():
    assert AND("foo") == "foo"
    assert AND(["foo", "bar"]) == {"and": ["foo", "bar"]}
    assert AND("foo", "bar") == {"and": ["foo", "bar"]}
    assert AND(AND("foo", "bar"), "baz") == {"and": [{"and": ["foo", "bar"]}, "baz"]}
    with pytest.raises(ValueError):
        AND()


def test_helper_or():
    assert OR("foo") == "foo"
    assert OR(["foo", "bar"]) == {"or": ["foo", "bar"]}
    assert OR("foo", "bar") == {"or": ["foo", "bar"]}
    assert OR(OR("foo", "bar"), "baz") == {"or": [{"or": ["foo", "bar"]}, "baz"]}
    with pytest.raises(ValueError):
        OR()


def test_helper_not():
    assert NOT("foo") == {"not": "foo"}
    assert NOT(["foo"]) == {"not": "foo"}
    assert NOT(["foo", "bar"]) == {"not": ["foo", "bar"]}
    assert NOT("foo", "bar") == {"not": ["foo", "bar"]}
    assert NOT(NOT("foo", "bar")) == {"not": {"not": ["foo", "bar"]}}
    with pytest.raises(ValueError):
        NOT()


def test_helpers_combined():
    assert AND(OR("foo", "bar"), NOT("baz")) == {
        "and": [{"or": ["foo", "bar"]}, {"not": "baz"}]
    }


def test_convert_to_v2():
    convert_to_v2(["foo", "bar"]) == {"and": ["foo", "bar"]}
    convert_to_v2(["foo"]) == "foo"
    convert_to_v2([]) == []


@pytest.mark.parametrize(
    "expression",
    [
        {"and": ["foo", "bar"]},
        {"and": ["foo", {"and": ["baz", "baa"]}]},
        {"or": ["foo", "bar"]},
        {"or": ["foo", {"or": ["baz", "baa"]}]},
        {"not": "foo"},
        {"not": ["foo", "bar"]},
        {"not": ["foo", {"and": ["baz", "baa"]}]},
    ],
)
def test_validate_ok(expression):
    validate(expression)


@pytest.mark.parametrize(
    "expression",
    [
        {"and": ["foo"]},
        {"and": [1, 2]},
        {"or": ["foo"]},
        {"or": [1, 2]},
        {"not": 1},
        {"not": ["foo"]},
    ],
)
def test_validate_invalid(expression):
    with pytest.raises(ValueError):
        validate(expression)
