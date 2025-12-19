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

import pytest
from httpx import AsyncClient

from nucliadb.ingest.orm.resource import Resource
from nucliadb.reader.api.v1.router import KB_PREFIX, RESOURCE_PREFIX, RSLUG_PREFIX

ID = ("id",)
BASIC = (
    "slug",
    "title",
    "summary",
    "icon",
    "thumbnail",
    "metadata",
    "usermetadata",
    "computedmetadata",
    "created",
    "modified",
    "fieldmetadata",
    "last_seqid",
    "last_account_seq",
    "queue",
    "hidden",
)
RELATIONS = ("relations",)
ORIGIN = ("origin",)
DATA = ("data",)
VALUES = ("values",)
EXTRACTED = ("extracted",)


@pytest.mark.deploy_modes("component")
async def test_get_resource_inexistent(nucliadb_reader: AsyncClient, knowledgebox: str) -> None:
    kbid = knowledgebox
    rid = "000000000000001"

    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}",
    )
    assert resp.status_code == 404


@pytest.mark.deploy_modes("component")
async def test_get_resource_default_options(
    nucliadb_reader: AsyncClient, test_resource: Resource
) -> None:
    rsc = test_resource
    kbid = rsc.kbid
    rid = rsc.uuid

    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}",
    )
    assert resp.status_code == 200

    resource = resp.json()

    expected_root_fields = set(ID + BASIC)
    assert set(resource.keys()) == expected_root_fields
    assert "data" not in resource


@pytest.mark.deploy_modes("component")
async def test_get_resource_sequence_ids_are_set_on_resource(
    nucliadb_reader: AsyncClient, test_resource: Resource
) -> None:
    rsc = test_resource
    kbid = rsc.kbid
    rid = rsc.uuid

    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}",
    )
    assert resp.status_code == 200

    resource = resp.json()

    expected_root_fields = set(ID + BASIC)
    assert set(resource.keys()) == expected_root_fields

    assert "data" not in resource
    assert test_resource.basic is not None
    assert resource["last_seqid"] == test_resource.basic.last_seqid
    assert resource["last_account_seq"] == test_resource.basic.last_account_seq
    assert resource["queue"] == "private"


@pytest.mark.deploy_modes("component")
async def test_get_resource_all(
    nucliadb_reader: AsyncClient,
    test_resource: Resource,
) -> None:
    rsc = test_resource
    kbid = rsc.kbid
    rid = rsc.uuid

    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}",
        params={
            "show": ["basic", "origin", "relations", "values", "extracted"],
            "field_type": [
                "text",
                "link",
                "file",
                "conversation",
            ],
            "extracted": [
                "metadata",
                "vectors",
                "large_metadata",
                "text",
                "link",
                "file",
            ],
        },
    )
    assert resp.status_code == 200

    resource = resp.json()

    expected_root_fields = set(ID + BASIC + ORIGIN + DATA)
    assert set(resource.keys()) == expected_root_fields
    assert "relations" in resource["usermetadata"].keys()

    data = resource["data"]
    assert set(data.keys()) == {
        "files",
        "texts",
        "links",
        "conversations",
    }
    texts = data["texts"]
    assert set(texts.keys()) == {"text1"}
    assert set(texts["text1"]["extracted"].keys()) == {
        "metadata",
        "vectors",
        "large_metadata",
        "text",
    }
    links = data["links"]
    assert set(links.keys()) == {"link1"}
    assert set(links["link1"]["extracted"].keys()) == {
        "metadata",
        "vectors",
        "large_metadata",
        "text",
        "link",
    }


@pytest.mark.deploy_modes("component")
async def test_get_resource_filter_root_fields(nucliadb_reader: AsyncClient, test_resource):
    rsc = test_resource
    kbid = rsc.kbid
    rid = rsc.uuid

    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}",
        params={"show": ["basic", "values"]},
    )
    assert resp.status_code == 200

    resource = resp.json()

    expected_root_fields = set(ID + BASIC + DATA)
    assert set(resource.keys()) == expected_root_fields

    data = resource["data"]

    assert set(data.keys()) == {
        "files",
        "texts",
        "links",
        "conversations",
        "generics",
    }

    assert set(data["files"]["file1"].keys()) == {"value"}
    assert set(data["texts"]["text1"].keys()) == {"value"}
    assert set(data["links"]["link1"].keys()) == {"value"}
    assert set(data["conversations"]["conv1"].keys()) == {"value"}


@pytest.mark.deploy_modes("component")
async def test_get_resource_filter_field_types(nucliadb_reader: AsyncClient, test_resource):
    rsc = test_resource
    kbid = rsc.kbid
    rid = rsc.uuid

    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}",
        params={"show": ["values", "extracted"], "field_type": ["text", "link"]},
    )
    assert resp.status_code == 200

    resource = resp.json()

    expected_root_fields = set(ID + DATA)
    assert set(resource.keys()) == expected_root_fields

    data = resource["data"]

    assert set(data.keys()) == {"texts", "links"}
    assert set(data["texts"]["text1"].keys()) == {"value", "extracted"}
    assert set(data["links"]["link1"].keys()) == {"value", "extracted"}


@pytest.mark.deploy_modes("component")
async def test_get_resource_filter_field_types_and_extracted(
    nucliadb_reader: AsyncClient, test_resource
):
    rsc = test_resource
    kbid = rsc.kbid
    rid = rsc.uuid

    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}",
        params={
            "show": ["extracted"],
            "field_type": ["text"],
            "extracted": ["metadata", "vectors"],
        },
    )

    assert resp.status_code == 200
    resource = resp.json()

    expected_root_fields = set(ID + DATA)
    assert set(resource.keys()) == expected_root_fields

    data = resource["data"]

    assert set(data.keys()) == {"texts"}
    assert set(data["texts"]["text1"].keys()) == {"extracted"}
    assert set(data["texts"]["text1"]["extracted"].keys()) == {
        "metadata",
        "vectors",
    }


@pytest.mark.deploy_modes("component")
async def test_resource_endpoints_by_slug(nucliadb_reader: AsyncClient, test_resource):
    rsc = test_resource
    kbid = rsc.kbid
    rslug = rsc.basic.slug

    non_existent_slug = "foobar"

    # Regular GET

    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{kbid}/{RSLUG_PREFIX}/{rslug}",
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{kbid}/{RSLUG_PREFIX}/{non_existent_slug}",
    )
    assert resp.status_code == 404

    # Field endpoint

    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{kbid}/{RSLUG_PREFIX}/{rslug}/text/text1",
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{kbid}/{RSLUG_PREFIX}/{non_existent_slug}/text/text1",
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Resource does not exist"


@pytest.mark.deploy_modes("component")
async def test_get_resource_extracted_metadata(nucliadb_reader: AsyncClient, test_resource: Resource):
    rsc = test_resource
    kbid = rsc.kbid
    rid = rsc.uuid

    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}",
        params={
            "show": ["extracted"],
            "extracted": [
                "metadata",
            ],
        },
    )
    assert resp.status_code == 200

    resource = resp.json()
    metadata = resource["data"]["texts"]["text1"]["extracted"]["metadata"]["metadata"]

    # Check that the processor entity is in the legacy metadata
    # TODO: Remove once deprecated fields are removed
    assert metadata["positions"]["ENTITY/document"]["entity"] == "document"
    # Check that we recieved entities in the new fields
    assert metadata["entities"]["processor"]["entities"][0]["text"] == "document"
    assert metadata["entities"]["processor"]["entities"][0]["label"] == "ENTITY"
    assert len(metadata["entities"]["processor"]["entities"][0]["positions"]) == 2

    assert metadata["entities"]["my-task-id"]["entities"][0]["text"] == "document"
    assert metadata["entities"]["my-task-id"]["entities"][0]["label"] == "NOUN"
    assert len(metadata["entities"]["my-task-id"]["entities"][0]["positions"]) == 2


@pytest.mark.deploy_modes("component")
async def test_head_resource(nucliadb_reader: AsyncClient, test_resource: Resource):
    kbid = test_resource.kbid
    slug = test_resource.basic.slug  # type: ignore
    uuid = test_resource.uuid

    # By UUID
    resp = await nucliadb_reader.head(
        f"/kb/{kbid}/resource/{uuid}",
    )
    assert resp.status_code == 200
    resp = await nucliadb_reader.head(
        f"/kb/{kbid}/resource/non-existent-uuid",
    )
    assert resp.status_code == 404

    # By slug
    resp = await nucliadb_reader.head(
        f"/kb/{kbid}/slug/{slug}",
    )
    assert resp.status_code == 200
    resp = await nucliadb_reader.head(
        f"/kb/{kbid}/slug/non-existent-slug",
    )
    assert resp.status_code == 404
