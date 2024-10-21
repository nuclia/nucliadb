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

from nucliadb_protos.nodewriter_pb2 import IndexMessage
from nucliadb_utils import logger
from nucliadb_utils.nats import NatsConnectionManager
from nucliadb_utils.settings import indexing_settings
from nucliadb_utils.utilities import Utility, clean_utility, get_utility, set_utility


class NidxIndexer:
    def __init__(
        self,
        subject: str,
        nats_servers: list[str],
        nats_creds: Optional[str] = None,
    ):
        self.nats_connection_manager = NatsConnectionManager(
            service_name="NidxIndexer",
            nats_servers=nats_servers,
            nats_creds=nats_creds,
        )
        self.subject = subject

    async def initialize(self):
        await self.nats_connection_manager.initialize()

    async def finalize(self):
        await self.nats_connection_manager.finalize()

    async def index(self, writer: IndexMessage) -> int:
        res = await self.nats_connection_manager.js.publish(self.subject, writer.SerializeToString())
        logger.info(
            f" = Pushed message to nidx shard: {writer.shard}, txid: {writer.txid}  seqid: {res.seq}"  # noqa
        )
        return res.seq


async def start_nidx_utility() -> NidxIndexer:
    if indexing_settings.index_nidx_subject is None:
        raise ValueError("nidx subject needed for nidx utility")
    nidx_utility = NidxIndexer(
        subject=indexing_settings.index_nidx_subject,
        nats_creds=indexing_settings.index_jetstream_auth,
        nats_servers=indexing_settings.index_jetstream_servers,
    )
    await nidx_utility.initialize()
    set_utility(Utility.NIDX, nidx_utility)
    return nidx_utility


async def stop_nidx_utility():
    nidx_utility = get_nidx()
    if nidx_utility:
        clean_utility(Utility.NIDX)
        await nidx_utility.finalize()


def get_nidx() -> NidxIndexer:
    return get_utility(Utility.NIDX)
