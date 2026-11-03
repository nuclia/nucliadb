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
from nidx_protos.noderesources_pb2 import Resource as IndexMessage

from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.index_message import IndexMessageBuilder
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_utils.storages.storage import Storage


async def build_index_message(
    driver: Driver,
    storage: Storage,
    message: BrokerMessage,
    *,
    resource_created: bool = False,
) -> IndexMessage:
    async with driver.transaction(read_only=True, kbid=message.kbid) as txn:
        resource = await KnowledgeBox(txn, storage, kbid=message.kbid).get(message.uuid)
        assert resource is not None
        builder = IndexMessageBuilder(resource)
        if message.source == BrokerMessage.MessageSource.PROCESSOR:
            return await builder.for_processor_bm(message)
        return await builder.for_writer_bm(message, resource_created=resource_created)
