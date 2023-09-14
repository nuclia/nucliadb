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

from nucliadb.common.maindb.driver import Driver, Transaction
from nucliadb_protos import knowledgebox_pb2 as kb_pb2

KB_LABELS = "/kbs/{kbid}/labels"
KB_LABELSET = "/kbs/{kbid}/labels/{id}"


class LabelsDataManager:
    def __init__(self, driver: Driver):
        self.driver = driver

    async def get_labels(self, kbid: str) -> kb_pb2.Labels:
        """
        Get all labels for a knowledge box (from multiple labelsets)
        """
        async with self.driver.transaction() as txn:
            return await LabelsDataManager.inner_get_labels(kbid, txn)

    @classmethod
    async def inner_get_labels(cls, kbid: str, txn: Transaction) -> kb_pb2.Labels:
        labels = kb_pb2.Labels()
        labels_key = KB_LABELS.format(kbid=kbid)
        async for key in txn.keys(labels_key, count=-1):
            labelset = await txn.get(key)
            id = key.split("/")[-1]
            if labelset:
                ls = kb_pb2.LabelSet()
                ls.ParseFromString(labelset)
                labels.labelset[id].CopyFrom(ls)
        return labels

    @classmethod
    async def inner_get_labelset(
        cls, kbid: str, labelset_id: str, txn: Transaction
    ) -> Optional[kb_pb2.LabelSet]:
        labelset_key = KB_LABELSET.format(kbid=kbid, id=labelset_id)
        payload = await txn.get(labelset_key)
        if payload:
            ls = kb_pb2.LabelSet()
            ls.ParseFromString(payload)
            return ls
        return None

    async def set_labels(self, kbid: str, labels: kb_pb2.Labels) -> None:
        """
        Set all labels for a knowledge box (may include multiple labelsets)
        """
        async with self.driver.transaction() as txn:
            for ls_id, ls_labels in labels.labelset.items():
                await LabelsDataManager.set_labelset(kbid, ls_id, ls_labels, txn)
            await txn.commit()

    @classmethod
    async def set_labelset(
        cls, kbid: str, labelset_id: str, labelset: kb_pb2.LabelSet, txn: Transaction
    ) -> None:
        labelset_key = KB_LABELSET.format(kbid=kbid, id=labelset_id)
        await txn.set(labelset_key, labelset.SerializeToString())
