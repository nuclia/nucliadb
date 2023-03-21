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
from nucliadb_protos.knowledgebox_pb2 import Synonyms as PBSynonyms

from nucliadb.ingest.orm.synonyms import Synonyms


@pytest.mark.asyncio
async def test_set_get(txn, gcs_storage, fake_node, knowledgebox_ingest):
    synonyms = Synonyms(txn, knowledgebox_ingest)

    assert await synonyms.get() is None

    pbs = PBSynonyms()
    pbs.terms["planet"].synonyms.extend(["globe", "earth"])
    await synonyms.set(pbs)

    pbs2 = await synonyms.get()
    assert pbs2.terms["planet"].synonyms == ["globe", "earth"]


@pytest.mark.asyncio
async def test_clear(txn, gcs_storage, fake_node, knowledgebox_ingest):
    synonyms = Synonyms(txn, knowledgebox_ingest)

    await synonyms.clear()

    pbs = PBSynonyms()
    pbs.terms["planet"].synonyms.extend(["globe", "earth"])
    await synonyms.set(pbs)

    await synonyms.clear()

    assert await synonyms.get() is None
