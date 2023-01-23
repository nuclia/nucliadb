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

from nucliadb_sdk.entities import Entity
from nucliadb_sdk.file import File
from nucliadb_sdk.knowledgebox import KnowledgeBox
from nucliadb_sdk.vectors import Vector


def test_create_resource(knowledgebox: KnowledgeBox):
    assert knowledgebox.get("mykey1") is None

    resource_id = knowledgebox.upload(
        key="mykey1",
        binary=File(data=b"asd", filename="data"),
        text="I'm Ramon",
        labels=["labelset/positive"],
        entities=[Entity(type="NAME", value="Ramon", positions=[(5, 9)])],
        vectors=[Vector(value=[1.0, 0.2], vectorset="base")],
    )
    resource2 = knowledgebox[resource_id]
    assert (
        resource2.data is not None
        and resource2.data.texts is not None
        and "text" in resource2.data.texts
    )
    assert (
        resource2.data is not None
        and resource2.data.files is not None
        and "file" in resource2.data.files
    )

    resource3 = knowledgebox["mykey1"]
    assert resource3.id == resource2.id

    assert len(knowledgebox) == 1

    del knowledgebox["mykey1"]

    assert len(knowledgebox) == 0


def test_create_resource_dict(knowledgebox: KnowledgeBox):
    assert knowledgebox.get("mykey1") is None

    resource_id = knowledgebox.upload(
        key="mykey1",
        binary=File(data=b"asd", filename="data"),
        text="I'm Ramon",
        labels=["labelset/positive"],
        entities=[Entity(type="NAME", value="Ramon", positions=[(5, 9)])],
        vectors={"base": [1.0, 2.0]},
    )
    resource2 = knowledgebox[resource_id]
    assert (
        resource2.data is not None
        and resource2.data.texts is not None
        and "text" in resource2.data.texts
    )
    assert (
        resource2.data is not None
        and resource2.data.files is not None
        and "file" in resource2.data.files
    )

    resource3 = knowledgebox["mykey1"]
    assert resource3.id == resource2.id

    assert len(knowledgebox) == 1

    del knowledgebox["mykey1"]

    assert len(knowledgebox) == 0


@pytest.mark.asyncio
async def test_create_resource_async(knowledgebox: KnowledgeBox):
    assert await knowledgebox.async_get("mykey1") is None

    resource_id = await knowledgebox.async_upload(
        key="mykey1",
        binary=File(data=b"asd", filename="data"),
        text="asd",
        labels=["labelset/positive"],
        entities=[Entity(type="NAME", value="Ramon", positions=[(5, 9)])],
        vectors=[Vector(value=[1.0, 0.2], vectorset="base")],
    )

    resource2 = await knowledgebox.async_get(resource_id)
    assert (
        resource2.data is not None
        and resource2.data.texts is not None
        and "text" in resource2.data.texts
    )
    assert (
        resource2.data is not None
        and resource2.data.files is not None
        and "file" in resource2.data.files
    )

    resource3 = await knowledgebox.async_get("mykey1")
    assert resource3.id == resource2.id

    assert await knowledgebox.async_len() == 1

    await knowledgebox.async_del("mykey1")

    assert await knowledgebox.async_len() == 0
