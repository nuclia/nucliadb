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

from nucliadb.common import datamanagers
from nucliadb.common.ids import ParagraphId
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.search.search import paragraphs
from nucliadb_protos.resources_pb2 import Basic, ExtractedTextWrapper
from nucliadb_protos.utils_pb2 import ExtractedText


async def test_get_paragraph_text(storage, cache, txn, dummy_nidx_utility, processor, knowledgebox):
    kbid = knowledgebox
    rid = uuid.uuid4().hex
    basic = Basic(slug="slug", uuid=rid)
    await datamanagers.resources.set_basic(txn, kbid=kbid, rid=rid, basic=basic)
    kb = KnowledgeBox(txn, storage, kbid)
    orm_resource = await kb.get(rid)
    field_obj = await orm_resource.get_field("field", 4, load=False)
    await field_obj.set_extracted_text(ExtractedTextWrapper(body=ExtractedText(text="Hello World!")))

    # make sure to reset the cache
    field_obj.extracted_text = None

    text1 = await paragraphs.get_paragraph_text(
        kbid=kbid,
        paragraph_id=ParagraphId.from_string(f"{rid}/t/field/0-5"),
        orm_resource=orm_resource,
    )
    assert text1 == "Hello"
