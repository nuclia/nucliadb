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
