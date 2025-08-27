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


from nucliadb.common.datamanagers import vectorsets
from nucliadb.common.maindb.driver import Driver
from nucliadb_protos import knowledgebox_pb2


async def test_kb_vectorsets(maindb_driver: Driver):
    kbid = "kbid"
    vectorset_id_1 = "gecko"
    vectoset_config_1 = knowledgebox_pb2.VectorSetConfig(vectorset_id=vectorset_id_1)
    vectorset_id_2 = "openai-matryoshka"
    vectorset_config_2 = knowledgebox_pb2.VectorSetConfig(vectorset_id=vectorset_id_2)

    # Check initially there are no vectorsets
    async with maindb_driver.ro_transaction() as txn:
        assert await vectorsets.get(txn, kbid=kbid, vectorset_id=vectorset_id_1) is None
        assert await vectorsets.exists(txn, kbid=kbid, vectorset_id=vectorset_id_1) is False
        assert await vectorsets.get(txn, kbid=kbid, vectorset_id=vectorset_id_2) is None
        assert await vectorsets.exists(txn, kbid=kbid, vectorset_id=vectorset_id_2) is False

    # Set two vectorsets
    async with maindb_driver.rw_transaction() as txn:
        await vectorsets.set(txn, kbid=kbid, config=vectoset_config_1)
        await vectorsets.set(txn, kbid=kbid, config=vectorset_config_2)
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        assert await vectorsets.exists(txn, kbid=kbid, vectorset_id=vectorset_id_1) is True
        assert await vectorsets.exists(txn, kbid=kbid, vectorset_id=vectorset_id_2) is True

        stored_vectorset_1 = await vectorsets.get(txn, kbid=kbid, vectorset_id=vectorset_id_1)
    assert stored_vectorset_1 == vectoset_config_1

    # Delete one vectorset an validate the second one is still there
    async with maindb_driver.rw_transaction() as txn:
        await vectorsets.delete(txn, kbid=kbid, vectorset_id=vectorset_id_1)
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        await vectorsets.get(txn, kbid=kbid, vectorset_id=vectorset_id_1) is None
        assert await vectorsets.exists(txn, kbid=kbid, vectorset_id=vectorset_id_2) is True

        stored_vectorset_2 = await vectorsets.get(txn, kbid=kbid, vectorset_id=vectorset_id_2)
    assert stored_vectorset_2 == vectorset_config_2
