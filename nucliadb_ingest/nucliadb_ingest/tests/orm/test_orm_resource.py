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
from typing import Optional
from uuid import uuid4

import pytest
from nucliadb_protos.resources_pb2 import Basic as PBBasic
from nucliadb_protos.resources_pb2 import Classification as PBClassification
from nucliadb_protos.resources_pb2 import FieldID as PBFieldID
from nucliadb_protos.resources_pb2 import FieldType
from nucliadb_protos.resources_pb2 import Metadata as PBMetadata
from nucliadb_protos.resources_pb2 import Origin as PBOrigin
from nucliadb_protos.resources_pb2 import TokenSplit as PBTokenSplit
from nucliadb_protos.resources_pb2 import UserFieldMetadata as PBUserFieldMetadata
from nucliadb_protos.utils_pb2 import Relation as PBRelation
from nucliadb_protos.utils_pb2 import RelationNode

from nucliadb_ingest.orm.knowledgebox import KnowledgeBox


@pytest.mark.asyncio
async def test_create_resource_orm_with_basic(
    gcs_storage, txn, cache, fake_node, knowledgebox: str
):
    basic = PBBasic(
        icon="text/plain",
        title="My title",
        summary="My summary",
        thumbnail="/file",
        layout="basic",
    )
    basic.metadata.metadata["key"] = "value"
    basic.metadata.language = "ca"
    basic.metadata.useful = True
    basic.metadata.status = PBMetadata.Status.PROCESSED

    cl1 = PBClassification(labelset="labelset1", label="label")
    basic.usermetadata.classifications.append(cl1)

    r1 = PBRelation(
        relation=PBRelation.CHILD,
        source=RelationNode(value="000000", ntype=RelationNode.NodeType.RESOURCE),
        to=RelationNode(value="000001", ntype=RelationNode.NodeType.RESOURCE),
    )

    basic.usermetadata.relations.append(r1)

    ufm1 = PBUserFieldMetadata(
        token=[PBTokenSplit(token="My home", klass="Location")],
        field=PBFieldID(field_type=FieldType.TEXT, field="title"),
    )

    basic.fieldmetadata.append(ufm1)
    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, gcs_storage, cache, kbid=knowledgebox)
    r = await kb_obj.add_resource(uuid=uuid, slug="slug", basic=basic)
    assert r is not None

    b2: Optional[PBBasic] = await r.get_basic()
    assert b2 is not None
    assert b2.icon == "text/plain"

    o2: Optional[PBOrigin] = await r.get_origin()
    assert o2 is None

    o2 = PBOrigin()
    assert o2 is not None
    o2.source = PBOrigin.Source.API
    o2.source_id = "My Surce"
    o2.created.FromDatetime(datetime.now())

    await r.set_origin(o2)
    o2 = await r.get_origin()
    assert o2 is not None
    assert o2.source_id == "My Surce"
