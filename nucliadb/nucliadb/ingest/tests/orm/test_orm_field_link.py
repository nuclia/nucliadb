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
from datetime import datetime
from uuid import uuid4

import pytest
from nucliadb_protos.resources_pb2 import FieldLink as PBFieldLink
from nucliadb_protos.resources_pb2 import FieldType

from nucliadb.ingest.fields.link import Link
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb_utils.storages.storage import Storage


@pytest.mark.asyncio
async def test_create_resource_orm_field_link(
    gcs_storage: Storage, txn, cache, fake_node, knowledgebox: str
):
    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, gcs_storage, cache, kbid=knowledgebox)
    r = await kb_obj.add_resource(uuid=uuid, slug="slug")
    assert r is not None

    t2 = PBFieldLink(
        uri="htts://nuclia.cloud",
        language="es",
    )
    t2.added.FromDatetime(datetime.now())
    t2.headers["AUTHORIZATION"] = "Bearer xxxxx"

    await r.set_field(FieldType.LINK, "link1", t2)

    linkfield: Link = await r.get_field("link1", FieldType.LINK, load=True)
    assert linkfield.value.uri == t2.uri

    await r.delete_field(FieldType.LINK, "link1")
    with pytest.raises(KeyError):
        linkfield = await r.get_field("link1", FieldType.LINK, load=True)
