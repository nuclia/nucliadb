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
import logging
from typing import Optional

import orjson

from nucliadb.common.maindb.driver import Transaction
from nucliadb_protos import knowledgebox_pb2 as kb_pb2

logger = logging.getLogger(__name__)

KB_LABELS = "/kbs/{kbid}/labels"
KB_LABELSET = "/kbs/{kbid}/labels/{id}"
KB_LABELSET_IDS = "/kbs/{kbid}/ids-labels"


async def get_labels(txn: Transaction, *, kbid: str) -> kb_pb2.Labels:
    """
    Get all labels for a knowledge box (from multiple labelsets)
    """
    labels = kb_pb2.Labels()
    labelset_ids = await _get_labelset_ids(txn, kbid=kbid)
    if labelset_ids is None:
        return labels
    for labelset_id in labelset_ids:
        labelset = await txn.get(KB_LABELSET.format(kbid=kbid, id=labelset_id))
        if not labelset:
            continue
        ls = kb_pb2.LabelSet()
        ls.ParseFromString(labelset)
        labels.labelset[labelset_id].CopyFrom(ls)
    return labels


async def _get_labelset_ids(txn: Transaction, *, kbid: str) -> Optional[list[str]]:
    key = KB_LABELSET_IDS.format(kbid=kbid)
    data = await txn.get(key, for_update=True)
    if not data:
        return None
    return orjson.loads(data)


async def _add_to_labelset_ids(txn: Transaction, *, kbid: str, labelsets: list[str]) -> None:
    updated = set(labelsets)
    previous = await _get_labelset_ids(txn, kbid=kbid)
    if previous is not None:
        updated.update(previous)
    if previous is None or previous != updated:
        await _set_labelset_ids(txn, kbid=kbid, labelsets=list(updated))


async def _delete_from_labelset_ids(txn: Transaction, *, kbid: str, labelsets: list[str]) -> None:
    previous = await _get_labelset_ids(txn, kbid=kbid)
    if previous is None:
        # Nothing to delete
        return
    previous_set = set(previous)
    updated = previous_set - set(labelsets)
    if previous_set != updated:
        await _set_labelset_ids(txn, kbid=kbid, labelsets=list(updated))


async def _set_labelset_ids(txn: Transaction, *, kbid: str, labelsets: list[str]) -> None:
    key = KB_LABELSET_IDS.format(kbid=kbid)
    data = orjson.dumps(labelsets)
    await txn.set(key, data)


async def get_labelset(txn: Transaction, *, kbid: str, labelset_id: str) -> Optional[kb_pb2.LabelSet]:
    labelset_key = KB_LABELSET.format(kbid=kbid, id=labelset_id)
    payload = await txn.get(labelset_key)
    if payload:
        ls = kb_pb2.LabelSet()
        ls.ParseFromString(payload)
        return ls
    return None


async def set_labels(txn: Transaction, *, kbid: str, labels: kb_pb2.Labels) -> None:
    """
    Set all labels for a knowledge box (may include multiple labelsets)
    """
    labelset_ids = list(labels.labelset.keys())
    await _set_labelset_ids(txn, kbid=kbid, labelsets=labelset_ids)
    for ls_id, ls_labels in labels.labelset.items():
        await _set_labelset(txn, kbid=kbid, labelset_id=ls_id, labelset=ls_labels)


async def set_labelset(
    txn: Transaction, *, kbid: str, labelset_id: str, labelset: kb_pb2.LabelSet
) -> None:
    await _add_to_labelset_ids(txn, kbid=kbid, labelsets=[labelset_id])
    await _set_labelset(txn, kbid=kbid, labelset_id=labelset_id, labelset=labelset)


async def _set_labelset(
    txn: Transaction, *, kbid: str, labelset_id: str, labelset: kb_pb2.LabelSet
) -> None:
    labelset_key = KB_LABELSET.format(kbid=kbid, id=labelset_id)
    await txn.set(labelset_key, labelset.SerializeToString())


async def delete_labelset(txn: Transaction, *, kbid: str, labelset_id: str) -> None:
    await _delete_from_labelset_ids(txn, kbid=kbid, labelsets=[labelset_id])
    labelset_key = KB_LABELSET.format(kbid=kbid, id=labelset_id)
    await txn.delete(labelset_key)
