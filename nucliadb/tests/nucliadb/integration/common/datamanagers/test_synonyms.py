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


from nucliadb.common import datamanagers
from nucliadb.common.maindb.driver import Driver
from nucliadb_protos import knowledgebox_pb2


async def test_kb_synonyms(maindb_driver: Driver):
    kbid = "kbid"

    # Check initially there are no synonyms
    async with maindb_driver.ro_transaction() as txn:
        assert await datamanagers.synonyms.get(txn, kbid=kbid) is None

    # Set and validate storage
    synonyms = knowledgebox_pb2.Synonyms()
    synonyms.terms["planet"].synonyms.extend(["globe", "earth"])

    async with maindb_driver.rw_transaction() as txn:
        await datamanagers.synonyms.set(txn, kbid=kbid, synonyms=synonyms)
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        stored = await datamanagers.synonyms.get(txn, kbid=kbid)
    assert stored == synonyms

    # Delete and validate
    async with maindb_driver.rw_transaction() as txn:
        await datamanagers.synonyms.delete(txn, kbid=kbid)
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        assert await datamanagers.synonyms.get(txn, kbid=kbid) is None
