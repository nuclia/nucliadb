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

from nucliadb_sdk.entities import Entity
from nucliadb_sdk.file import File
from nucliadb_sdk.knowledgebox import KnowledgeBox
from nucliadb_sdk.vectors import Vector


def test_dict_resource(knowledgebox: KnowledgeBox):
    assert knowledgebox.get("mykey1") is None

    knowledgebox.new_vectorset("base", 2)

    resource_id = knowledgebox.upload(
        key="mykey1",
        binary=File(data=b"asd", filename="data"),
        text="I'm Ramon",
        labels=["labelset/positive"],
        entities=[Entity(type="NAME", value="Ramon", positions=[(5, 9)])],
        vectors=[Vector(value=[1.0, 0.2], vectorset="base")],
    )
    resource2 = knowledgebox[resource_id]

    knowledgebox["mykey2"] = resource2

    assert len(knowledgebox) == 2

    del knowledgebox["mykey1"]

    assert len(knowledgebox) == 1

    del knowledgebox["mykey2"]

    assert len(knowledgebox) == 0
