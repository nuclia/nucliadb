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

from nucliadb_search.search.fetch import highlight, split_text


def test_highlight():

    res = highlight(
        "Query whatever you want my to make it work my query with this",
        "this is my query",
        True,
    )
    assert (
        res[0]
        == "Query whatever you want my to make it work my <mark>query</mark> with <mark>this</mark>"
    )

    res = highlight(
        "Query whatever you want to make it work my query with this",
        'this is "my query"',
        True,
    )

    assert (
        res[0]
        == "Query whatever you want to make it work <mark>my query</mark> with <mark>this</mark>"
    )

    res = highlight(
        "Query whatever you redis want to make it work my query with this",
        'this is "my query"',
        True,
    )

    assert (
        res[0]
        == "Query whatever you redis want to make it work <mark>my query</mark> with <mark>this</mark>"
    )


TEXT1 = """
Explanation: the methods based on + (including the implied use in sum) are, of necessity,
O(T**2) when there are T sublists -- as the intermediate result list keeps getting longer,
at each step a new intermediate result list object gets allocated,
and all the items in the previous intermediate result must be copied over
(as well as a few new ones added at the end).
So, for simplicity and without actual loss of generality, say you have T sublists of k items each:
the first k items are copied back and forth T-1 times, the second k items T-2 times, and so on;
total number of copies is k times the sum of x for x from 1 to T excluded, i.e., k * (T**2)/2.

The list comprehension just generates one list, once, and copies each item over
(from its original place of residence to the result list) also exactly once
"""


def test_split_text():

    res = split_text(
        TEXT1,
        "including copies",
        True,
    )
    assert (
        res[0]
        == " …methods based on + (<mark>including</mark> the implied use in … …on;\ntotal number of <mark>copies</mark> is k times the sum … …one list, once, and <mark>copies</mark> each item over\n(fro…"  # noqa
    )

    res = split_text(
        TEXT1,
        'this is "each item over"',
        True,
    )
    assert (
        res[0]
        == " …t, once, and copies <mark>each item over</mark>\n(from its original …"
    )
