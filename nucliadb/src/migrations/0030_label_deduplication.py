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

"""Migration #30

We want to support labels with the same title anymore. Run a deduplication for
all labelsets

"""

import logging

from nucliadb.common import datamanagers
from nucliadb.migrator.context import ExecutionContext

logger = logging.getLogger(__name__)


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    async with datamanagers.with_rw_transaction() as txn:
        kb_labels = await datamanagers.labels.get_labels(txn, kbid=kbid)
        changed = False

        for labelset in kb_labels.labelset.values():
            current_labels = labelset.labels
            labelset.ClearField("labels")
            deduplicator = set()

            for label in current_labels:
                label_id = label.title.lower()
                if label_id not in deduplicator:
                    deduplicator.add(label_id)
                    labelset.labels.append(label)

            changed = changed or (len(labelset.labels) < len(current_labels))

        if changed:
            await datamanagers.labels.set_labels(txn, kbid=kbid, labels=kb_labels)

        await txn.commit()
