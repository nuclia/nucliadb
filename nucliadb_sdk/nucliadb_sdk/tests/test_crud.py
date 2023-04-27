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

from nucliadb_models.metadata import (
    FieldID,
    TokenSplit,
    UserClassification,
    UserFieldMetadata,
    UserMetadata,
)
from nucliadb_models.resource import FieldText, TextFieldData
from nucliadb_models.text import TextFormat
from nucliadb_sdk.entities import Entity
from nucliadb_sdk.file import File
from nucliadb_sdk.knowledgebox import KnowledgeBox
from nucliadb_sdk.vectors import Vector


def test_create_resource(knowledgebox: KnowledgeBox):
    assert knowledgebox.get("mykey1") is None

    resource_id = knowledgebox.create_resource(
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


def test_iter_resources(knowledgebox: KnowledgeBox):
    assert knowledgebox.get("mykey1") is None

    for i in range(50):
        knowledgebox.create_resource(text="Test resource")

    res = []
    for resource in knowledgebox:  # type: ignore
        res.append(resource)

    assert len(res) == 50


def test_create_resource_dict(knowledgebox: KnowledgeBox):
    assert knowledgebox.get("mykey1") is None

    resource_id = knowledgebox.create_resource(
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


def test_dict_resource(knowledgebox: KnowledgeBox):
    assert knowledgebox.get("mykey1") is None

    knowledgebox.new_vectorset("base", 2)

    resource_id = knowledgebox.create_resource(
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


def test_update_existing_resource(knowledgebox: KnowledgeBox):
    assert knowledgebox.get("mykey1") is None

    knowledgebox.new_vectorset("base", 2)

    def _validate(res1, res2):
        assert res1.id == res2.id
        assert res1.title != res2.title
        assert res1.summary != res2.summary
        assert res1.data.texts["text"] != res2.data.texts["text"]
        assert res1.usermetadata != res2.usermetadata
        assert res1.fieldmetadata != res2.fieldmetadata

        assert res2.title == "common man"
        assert res2.summary == "I'm not Ramon"
        assert res2.data.texts["text"] == TextFieldData(
            value=FieldText(
                body="Really, I'm not Ramon",
                format=TextFormat.PLAIN,
                md5="62ef6847d003b438c597cb82d70982d0",
            ),
            extracted=None,
            error=None,
        )
        assert res2.usermetadata == UserMetadata(
            classifications=[
                UserClassification(
                    labelset="labelset", label="negative", cancelled_by_user=False
                )
            ],
            relations=[],
        )
        assert res2.fieldmetadata == [
            UserFieldMetadata(
                token=[
                    TokenSplit(
                        token="Ferran",
                        klass="NAME",
                        start=5,
                        end=9,
                        cancelled_by_user=False,
                    )
                ],
                paragraphs=[],
                field=FieldID(field_type=FieldID.FieldType.TEXT, field="text"),
            )
        ]

    # Validate upload updates fields
    resource_id = knowledgebox.create_resource(
        key="mykey1",
        binary=File(data=b"asd", filename="data"),
        text="I'm Ramon",
        labels=["labelset/positive"],
        entities=[Entity(type="NAME", value="Ramon", positions=[(5, 9)])],
        vectors=[Vector(value=[1.0, 0.2], vectorset="base")],
    )
    resource1 = knowledgebox[resource_id]

    knowledgebox.update_resource(
        resource1,
        title="common man",
        summary="I'm not Ramon",
        text="Really, I'm not Ramon",
        labels=["labelset/negative"],
        entities=[Entity(type="NAME", value="Ferran", positions=[(5, 9)])],
        vectors=[Vector(value=[1.0, 0.2], vectorset="base")],
    )
    resource2 = knowledgebox[resource_id]
    _validate(resource1, resource2)

    # Validate setattr updates fields
    resource_id = knowledgebox.create_resource(
        key="mykey2",
        binary=File(data=b"asd", filename="data"),
        text="I'm Ramon",
        labels=["labelset/positive"],
        entities=[Entity(type="NAME", value="Ramon", positions=[(5, 9)])],
        vectors=[Vector(value=[1.0, 0.2], vectorset="base")],
    )
    resource1 = knowledgebox[resource_id]
    resource1_updated = knowledgebox[resource_id]
    resource1_updated.title = "common man"
    resource1_updated.summary = "I'm not Ramon"
    resource1_updated.data.texts["text"] = TextFieldData(  # type: ignore
        value=FieldText(
            body="Really, I'm not Ramon",
            format=TextFormat.PLAIN,
            md5="62ef6847d003b438c597cb82d70982d0",
        ),
        extracted=None,
        error=None,
    )
    resource1_updated.usermetadata.classifications = [  # type: ignore
        UserClassification(
            labelset="labelset", label="negative", cancelled_by_user=False
        )
    ]
    resource1_updated.fieldmetadata = [
        UserFieldMetadata(
            token=[
                TokenSplit(
                    token="Ferran",
                    klass="NAME",
                    start=5,
                    end=9,
                    cancelled_by_user=False,
                )
            ],
            paragraphs=[],
            field=FieldID(field_type=FieldID.FieldType.TEXT, field="text"),
        )
    ]
    knowledgebox[resource_id] = resource1_updated
    # pull again to validate
    resource2 = knowledgebox[resource_id]
    _validate(resource1, resource2)
