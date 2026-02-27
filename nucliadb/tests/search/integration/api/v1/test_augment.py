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

from nucliadb.search.api.v1.router import KB_PREFIX
from nucliadb_models.augment import AugmentedFileField, AugmentResponse
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.ndbfixtures.resources import cookie_tale_resource, smb_wonder_resource


@pytest.mark.deploy_modes("standalone")
async def test_augment_api(
    nucliadb_search: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    knowledgebox: str,
) -> None:
    kbid = knowledgebox
    rid = await smb_wonder_resource(kbid, nucliadb_writer, nucliadb_ingest_grpc)

    resp = await nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/augment",
        json={
            "resources": [
                {
                    "given": [rid],
                    "basic": True,
                }
            ],
            "paragraphs": [
                {
                    "given": [
                        {"id": f"{rid}/f/smb-wonder/145-234"},
                    ],
                    "text": True,
                }
            ],
        },
    )
    assert resp.status_code == 200

    body = AugmentResponse.model_validate(resp.json())

    assert body.resources[rid].slug == "smb-wonder"
    assert body.resources[rid].title == "Super Mario Bros. Wonder"
    assert (
        body.paragraphs[f"{rid}/f/smb-wonder/145-234"].text
        == "As one of eight player characters, the player completes levels across the Flower Kingdom."
    )


@pytest.mark.deploy_modes("standalone")
async def test_augment_api_resource_fields(
    nucliadb_search: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    knowledgebox: str,
) -> None:
    kbid = knowledgebox
    rid = await smb_wonder_resource(kbid, nucliadb_writer, nucliadb_ingest_grpc)

    # Validate how the resource fields text is returned depending on the filters

    resp = await nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/augment",
        json={
            "resources": [
                {
                    "given": [rid],
                    "fields": {
                        "text": True,
                        # no filters means all resource fields
                        "filters": [],
                    },
                }
            ],
        },
    )
    assert resp.status_code == 200

    body = AugmentResponse.model_validate(resp.json())
    field = body.fields[f"{rid}/f/smb-wonder"]
    assert field.text is not None and len(field.text) == 234

    resp = await nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/augment",
        json={
            "resources": [
                {
                    "given": [rid],
                    "fields": {
                        "text": True,
                        "filters": [
                            # only text fields
                            {"prop": "field", "type": "text"}
                        ],
                    },
                }
            ],
        },
    )
    assert resp.status_code == 200

    body = AugmentResponse.model_validate(resp.json())
    field = body.fields[f"{rid}/f/smb-wonder"]
    assert field.text is not None and len(field.text) == 234

    resp = await nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/augment",
        json={
            "resources": [
                {
                    "given": [rid],
                    "fields": {
                        "text": True,
                        "filters": [
                            # try with all other field types
                            {"prop": "field", "type": t}
                            for t in ["file", "link", "generic", "conversation"]
                        ],
                    },
                }
            ],
        },
    )
    assert resp.status_code == 200

    body = AugmentResponse.model_validate(resp.json())
    # no field returned, as the resource only has a text field
    assert len(body.fields) == 0


@pytest.mark.deploy_modes("standalone")
async def test_augment_api_images(
    nucliadb_search: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    knowledgebox: str,
) -> None:
    kbid = knowledgebox
    rid = await cookie_tale_resource(kbid, nucliadb_writer, nucliadb_ingest_grpc)

    resp = await nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/augment",
        json={
            "paragraphs": [
                {
                    "given": [
                        {"id": f"{rid}/f/cookie-recipie/0-29"},
                    ],
                    "source_image": True,
                }
            ],
        },
    )
    assert resp.status_code == 200
    body = AugmentResponse.model_validate(resp.json())
    assert body.paragraphs[f"{rid}/f/cookie-recipie/0-29"].source_image == "generated/cookies.png"

    resp = await nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/augment",
        json={
            "paragraphs": [
                {
                    "given": [
                        {"id": f"{rid}/f/cookie-recipie/29-75"},
                    ],
                    "table_image": True,
                }
            ],
        },
    )
    assert resp.status_code == 200
    body = AugmentResponse.model_validate(resp.json())
    assert (
        body.paragraphs[f"{rid}/f/cookie-recipie/29-75"].table_image == "generated/ingredients_table.png"
    )

    resp = await nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/augment",
        json={
            "paragraphs": [
                {
                    "given": [
                        {"id": f"{rid}/f/cookie-recipie/29-75"},
                    ],
                    "table_image": True,
                    "table_prefers_page_preview": True,
                }
            ],
        },
    )
    assert resp.status_code == 200
    body = AugmentResponse.model_validate(resp.json())
    assert (
        body.paragraphs[f"{rid}/f/cookie-recipie/29-75"].table_image
        == "generated/extracted_images_1.png"
    )

    resp = await nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/augment",
        json={
            "paragraphs": [
                {
                    "given": [
                        {"id": f"{rid}/f/cookie-recipie/29-75"},
                    ],
                    "page_preview_image": True,
                }
            ],
        },
    )
    assert resp.status_code == 200
    body = AugmentResponse.model_validate(resp.json())
    assert (
        body.paragraphs[f"{rid}/f/cookie-recipie/29-75"].page_preview_image
        == "generated/extracted_images_1.png"
    )


@pytest.mark.deploy_modes("standalone")
async def test_augment_api_file_thumbnails(
    nucliadb_search: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
    knowledgebox: str,
) -> None:
    kbid = knowledgebox
    rid = await cookie_tale_resource(kbid, nucliadb_writer, nucliadb_ingest_grpc)

    resp = await nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/augment",
        json={
            "fields": [
                {
                    "given": [f"{rid}/f/cookie-recipie"],
                    "file_thumbnail": True,
                }
            ],
        },
    )
    assert resp.status_code == 200

    body: AugmentResponse = AugmentResponse.model_validate(resp.json())
    assert f"{rid}/f/cookie-recipie" in body.fields
    field = body.fields[f"{rid}/f/cookie-recipie"]
    assert isinstance(field, AugmentedFileField)
    assert field.thumbnail_image == "file_thumbnail"

    # the path returned can be used to download the thumbnail
    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{kbid}/resource/{rid}/file/cookie-recipie/download/extracted/{field.thumbnail_image}"
    )
    assert resp.status_code == 200
    assert resp.content == b"cookie recipie (file) thumbnail"
