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
from nucliadb_models.augment import AugmentResponse
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.ndbfixtures.resources import smb_wonder_resource


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
            "resources": {
                "given": [rid],
                "select": ["basic"],
            },
            "paragraphs": {
                "given": [
                    {"id": f"{rid}/f/smb-wonder/145-234"},
                ],
                "text": True,
            },
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
