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

from nucliadb.search.search.fetch import highlight_paragraph as highlight


def test_highlight():

    res = highlight(
        "Query whatever you want my to make it work my query with this",
        ["this", "is", "my", "query"],
    )
    assert (
        res
        == "<mark>Query</mark> whatever you want <mark>my</mark> to make it work <mark>my</mark> <mark>query</mark> with <mark>this</mark>"  # noqa
    )

    res = highlight(
        "Query whatever you want to make it work my query with this",
        ["this", "is"],
        ["my query"],
    )

    assert (
        res
        == "Query whatever you want to make it work <mark>my query</mark> with <mark>this</mark>"
    )

    res = highlight(
        "Query whatever you redis want to make it work my query with this",
        ["this", "is"],
        ["my query"],
    )

    assert (
        res
        == "Query whatever you redis want to make it work <mark>my query</mark> with <mark>this</mark>"
    )

    res = highlight(
        "Plone offers superior security controls, often without cost, of course!",
        ["use", "cases", "of", "plone"],
    )

    assert (
        res
        == "<mark>Plone</mark> offers superior security controls, often without cost, <mark>of</mark> course!"
    )

    res = highlight(
        "In contrast, traditional companies often make it impossible",
        ["of", "market"],
        ["of", "market"],
    )
    assert res == "In contrast, traditional companies often make it impossible"
