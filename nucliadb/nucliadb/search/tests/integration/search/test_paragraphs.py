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

import uuid

from nucliadb_protos.resources_pb2 import Basic, ExtractedTextWrapper
from nucliadb_protos.utils_pb2 import ExtractedText

from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.utils import set_basic
from nucliadb.search.search import paragraphs


async def test_get_paragraph_text(
    storage, cache, txn, fake_node, processor, knowledgebox_ingest
):
    kbid = knowledgebox_ingest
    uid = uuid.uuid4().hex
    basic = Basic(slug="slug", uuid=uid)
    await set_basic(txn, kbid, uid, basic)
    kb = KnowledgeBox(txn, storage, kbid)
    orm_resource = await kb.get(uid)
    field_obj = await orm_resource.get_field("field", 4, load=False)
    await field_obj.set_extracted_text(
        ExtractedTextWrapper(body=ExtractedText(text="Hello World!"))
    )

    # make sure to reset the cache
    field_obj.extracted_text = None

    text1 = await paragraphs.get_paragraph_text(
        kbid=kbid,
        rid=uid,
        field="/t/field",
        start=0,
        end=5,
        orm_resource=orm_resource,
    )
    assert text1 == "Hello"
