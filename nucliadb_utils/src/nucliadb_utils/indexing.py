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
from typing import List, Optional, Tuple

from nucliadb_protos.nodewriter_pb2 import IndexMessage
from nucliadb_utils import const, logger
from nucliadb_utils.nats import NatsConnectionManager


class IndexingUtility:
    def __init__(
        self,
        nats_servers: List[str],
        nats_creds: Optional[str] = None,
        dummy: bool = False,
    ):
        self.dummy = dummy
        if dummy:
            self._calls: List[Tuple[str, IndexMessage]] = []
        else:
            self.nats_connection_manager = NatsConnectionManager(
                service_name="IndexingUtility",
                nats_servers=nats_servers,
                nats_creds=nats_creds,
            )

    async def initialize(self, service_name: Optional[str] = None):
        if self.dummy:
            return
        await self.nats_connection_manager.initialize()

    async def finalize(self):
        if self.dummy:
            return
        await self.nats_connection_manager.finalize()

    async def index(self, writer: IndexMessage, node: str) -> int:
        if self.dummy:
            self._calls.append((node, writer))
            return 0

        subject = const.Streams.INDEX.subject.format(node=node)
        res = await self.nats_connection_manager.js.publish(subject, writer.SerializeToString())
        logger.info(
            f" - Pushed message to index {subject}.  shard: {writer.shard}, txid: {writer.txid}  seqid: {res.seq}"  # noqa
        )
        return res.seq
