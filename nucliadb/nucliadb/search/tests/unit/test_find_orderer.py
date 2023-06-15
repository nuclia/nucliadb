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

import random

from nucliadb.search.search.find_merge import Orderer


def test_orderer():
    orderer = Orderer()

    items = {}
    for i in range(30):
        key = str(i)
        score = random.random() * 25
        items[key] = score

    boosted = {4, 10, 28}

    boosted_items = []
    regular_items = []

    for i, (key, score) in enumerate(items.items()):
        if i in boosted:
            boosted_items.append(key)
            orderer.add_boosted(key)
        else:
            regular_items.append(key)
            orderer.add(key)

    sorted_items = list(orderer.sorted_by_insertion())
    assert sorted_items == boosted_items + regular_items


def test_orderer_handles_duplicate_insertions():
    orderer = Orderer()
    orderer.add_boosted("a")
    orderer.add_boosted("b")
    orderer.add_boosted("a")
    orderer.add_boosted("c")
    orderer.add("a")
    assert list(orderer.sorted_by_insertion()) == ["a", "b", "c"]
