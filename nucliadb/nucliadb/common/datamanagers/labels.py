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

import orjson

from nucliadb.common.maindb.driver import Driver, Transaction
from nucliadb_protos import knowledgebox_pb2 as kb_pb2

KB_LABELS = "/kbs/{kbid}/labels"
KB_LABELSET = "/kbs/{kbid}/labels/{id}"
KB_LABELSET_IDS = "/kbs/{kbid}/ids-labels"


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
        labelset_ids = await cls._get_labelset_ids_bw_compat(kbid, txn)
        for labelset_id in labelset_ids:
            labelset = await txn.get(KB_LABELSET.format(kbid=kbid, id=labelset_id))
            if not labelset:
                continue
            ls = kb_pb2.LabelSet()
            ls.ParseFromString(labelset)
            labels.labelset[labelset_id].CopyFrom(ls)
        return labels

    @classmethod
    async def _get_labelset_ids_bw_compat(
        cls, kbid: str, txn: Transaction
    ) -> list[str]:
        labelsets = await cls._get_labelset_ids(kbid, txn)
        if labelsets is not None:
            return labelsets
        # TODO: Remove this after migration #11
        return await cls._deprecated_scan_labelset_ids(kbid, txn)

    @classmethod
    async def _deprecated_scan_labelset_ids(
        cls, kbid: str, txn: Transaction
    ) -> list[str]:
        labelsets = []
        labels_key = KB_LABELS.format(kbid=kbid)
        async for key in txn.keys(labels_key, count=-1, include_start=False):
            lsid = key.split("/")[-1]
            labelsets.append(lsid)
        return labelsets

    @classmethod
    async def _get_labelset_ids(
        cls, kbid: str, txn: Transaction
    ) -> Optional[list[str]]:
        key = KB_LABELSET_IDS.format(kbid=kbid)
        data = await txn.get(key)
        if not data:
            return None
        return orjson.loads(data)

    @classmethod
    async def _add_to_labelset_ids(
        cls, kbid: str, txn: Transaction, labelsets: list[str]
    ) -> None:
        previous = await cls._get_labelset_ids(kbid, txn)
        needs_set = False
        if previous is None:
            # TODO: Remove this after migration #11
            needs_set = True
            previous = await LabelsDataManager._deprecated_scan_labelset_ids(kbid, txn)
        for labelset in labelsets:
            if labelset not in previous:
                needs_set = True
                previous.append(labelset)
        if needs_set:
            await cls._set_labelset_ids(kbid, txn, previous)

    @classmethod
    async def _delete_from_labelset_ids(
        cls, kbid: str, txn: Transaction, labelsets: list[str]
    ) -> None:
        needs_set = False
        previous = await cls._get_labelset_ids(kbid, txn)
        if previous is None:
            # TODO: Remove this after migration #11
            needs_set = True
            previous = await LabelsDataManager._deprecated_scan_labelset_ids(kbid, txn)
        for labelset in labelsets:
            if labelset in previous:
                needs_set = True
                previous.remove(labelset)
        if needs_set:
            await cls._set_labelset_ids(kbid, txn, previous)

    @classmethod
    async def _set_labelset_ids(
        cls, kbid: str, txn: Transaction, labelsets: list[str]
    ) -> None:
        key = KB_LABELSET_IDS.format(kbid=kbid)
        data = orjson.dumps(labelsets)
        await txn.set(key, data)

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
            labelset_ids = list(labels.labelset.keys())
            await LabelsDataManager._set_labelset_ids(kbid, txn, labelset_ids)
            for ls_id, ls_labels in labels.labelset.items():
                await LabelsDataManager._set_labelset(kbid, ls_id, ls_labels, txn)
            await txn.commit()

    @classmethod
    async def set_labelset(
        cls, kbid: str, labelset_id: str, labelset: kb_pb2.LabelSet, txn: Transaction
    ) -> None:
        await cls._add_to_labelset_ids(kbid, txn, [labelset_id])
        await cls._set_labelset(kbid, labelset_id, labelset, txn)

    @classmethod
    async def _set_labelset(
        cls, kbid: str, labelset_id: str, labelset: kb_pb2.LabelSet, txn: Transaction
    ) -> None:
        labelset_key = KB_LABELSET.format(kbid=kbid, id=labelset_id)
        await txn.set(labelset_key, labelset.SerializeToString())

    @classmethod
    async def delete_labelset(
        cls, kbid: str, labelset_id: str, txn: Transaction
    ) -> None:
        await cls._delete_from_labelset_ids(kbid, txn, [labelset_id])
        labelset_key = KB_LABELSET.format(kbid=kbid, id=labelset_id)
        await txn.delete(labelset_key)
