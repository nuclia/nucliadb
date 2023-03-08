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

from nucliadb.ingest.orm.synonyms import Synonyms
from nucliadb.ingest.utils import get_driver
from nucliadb.search import logger


async def apply_synonyms_to_request(request: SearchRequest, kbid: str) -> None:
    if not request.body:
        # Nothing to do
        return

    if request.advanced_query:
        # TODO: ask if this is a use-case?
        logger.warning(
            "Applying synonyms is not applicable when an advanced query is present"
        )
        return

    driver = await get_driver()
    async with driver.transaction() as txn:
        synonyms: Optional[PBSynonyms] = await Synonyms(txn, kbid).get()
        if synonyms is None:
            # No synonyms found
            return

    parts: List[str] = []
    for term in request.body.split(" "):
        for sterm in synonyms.terms.keys():
            if not synonym_match(term, sterm):
                continue
            parts.append(term)
            parts.extend(synonyms.terms[term].synonyms)  # type: ignore
            break

    if len(parts):
        advanced_query = " ".join(parts)
        request.advanced_query = advanced_query
        request.ClearField("body")


def synonym_match(term: str, another_term: str) -> bool:
    # TODO: Think if we should somehow pre-process the stored synonyms too...
    return term == another_term
