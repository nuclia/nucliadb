from nucliadb_models.text import FieldText
import pytest

from nucliadb_sdk.entities import Entity
from nucliadb_sdk.file import File
from nucliadb_sdk.knowledgebox import KnowledgeBox
from nucliadb_sdk.vectors import Vector
from nucliadb_models.resource import Resource


def test_dict_resource(knowledgebox: KnowledgeBox):
    assert knowledgebox.get("mykey1") is None

    knowledgebox.new_vectorset("base", 2)

    resource_id = knowledgebox.upload(
        key="mykey1",
        binary=File(data=b"asd", filename="data"),
        text="I'm Ramon",
        labels=["positive"],
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
