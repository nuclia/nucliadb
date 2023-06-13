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
from typing import List, Optional

from nucliadb_protos.knowledgebox_pb2 import Synonyms as PBSynonyms
from nucliadb_protos.nodereader_pb2 import SearchRequest

from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.orm.synonyms import Synonyms


async def apply_synonyms_to_request(request: SearchRequest, kbid: str) -> None:
    if not request.body:
        # Nothing to do
        return

    synonyms = await get_kb_synonyms(kbid)
    if synonyms is None:
        # No synonyms found
        return

    synonyms_found: List[str] = []
    advanced_query = []
    for term in request.body.split(" "):
        advanced_query.append(term)
        term_synonyms = synonyms.terms.get(term)
        if term_synonyms is None or len(term_synonyms.synonyms) == 0:
            # No synonyms found for this term
            continue
        synonyms_found.extend(term_synonyms.synonyms)

    if len(synonyms_found):
        request.advanced_query = " OR ".join(advanced_query + synonyms_found)
        request.ClearField("body")


async def get_kb_synonyms(kbid: str) -> Optional[PBSynonyms]:
    driver = get_driver()
    async with driver.transaction() as txn:
        return await Synonyms(txn, kbid).get()
