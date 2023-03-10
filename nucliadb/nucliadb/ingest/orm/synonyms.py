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
from typing import Optional

from nucliadb_protos.knowledgebox_pb2 import Synonyms as PBSynonyms

from nucliadb.ingest.maindb.driver import Transaction

KB_SYNONYMS = "/kbs/{kbid}/synonyms"


class Synonyms:
    def __init__(self, txn: Transaction, kbid: str):
        self.txn = txn
        self.kbid = kbid

    @property
    def key(self) -> str:
        return KB_SYNONYMS.format(kbid=self.kbid)

    async def set(self, synonyms: PBSynonyms):
        body = synonyms.SerializeToString()
        await self.txn.set(self.key, body)

    async def get(self) -> Optional[PBSynonyms]:
        try:
            payload = await self.txn.get(self.key)
        except KeyError:
            return None
        if payload is None:
            return None
        body = PBSynonyms()
        body.ParseFromString(payload)
        return body

    async def clear(self):
        await self.txn.delete(self.key)
