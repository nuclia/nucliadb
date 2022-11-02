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

import base64
from io import StringIO
from typing import Optional

import pytest
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb.models.text import TextField
from nucliadb.models.utils import FieldIdString
from nucliadb.models.writer import CreateResourcePayload
from nucliadb_client.client import NucliaDBClient
from nucliadb_client.exceptions import ConflictError
from nucliadb_client.knowledgebox import KnowledgeBox


@pytest.mark.asyncio
async def test_export_import(nucliadb_client: NucliaDBClient):
    export = []
    nucliadb_client.init_async_grpc()

    exists = nucliadb_client.get_kb(slug="mykb")
    if exists:
        exists.delete()

    try:
        kb: Optional[KnowledgeBox] = nucliadb_client.create_kb(
            title="My KB", description="Its a new KB", slug="mykb"
        )
    except ConflictError:
        kb = nucliadb_client.get_kb(slug="mykb")
    if kb is None:
        raise Exception("Not found KB")

    payload = CreateResourcePayload()
    payload.icon = "plain/text"
    payload.title = "My Resource"
    payload.summary = "My long summary of the resource"
    payload.slug = "myresource"  # type: ignore
    payload.texts[FieldIdString("text1")] = TextField(body="My text")
    kb.create_resource(payload)

    async for line in kb.generator():
        export.append(line)

    data = StringIO("\n".join(export))

    exists = nucliadb_client.get_kb(slug="mykb2")
    if exists:
        exists.delete()

    await nucliadb_client.import_kb(slug="mykb2", location=data)

    kb = nucliadb_client.get_kb(slug="mykb2")
    if kb is None:
        raise AttributeError("Could not found KB")
    resources = kb.list_resources()

    bm = BrokerMessage()
    bm.ParseFromString(base64.b64decode(export[0][4:]))
    assert bm.basic.title == resources[0].get().title

    for resource in kb.iter_resources(page_size=1):
        resource.get().title == bm.basic.title
