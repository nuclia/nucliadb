import nats
import pytest

from nucliadb_utils.indexing import IndexingUtility
from nucliadb_utils.settings import indexing_settings
from nucliadb_utils.utilities import Utility, clean_utility, get_utility, set_utility


# Needs to be session to be executed at the begging
@pytest.fixture(scope="function")
async def cleanup_indexing(natsd):
    nc = await nats.connect(servers=[natsd])
    js = nc.jetstream()

    try:
        await js.delete_consumer("node", "node-1")
    except nats.js.errors.NotFoundError:
        pass

    try:
        await js.delete_stream(name="node")
    except nats.js.errors.NotFoundError:
        pass

    indexing_settings.index_jetstream_target = "node.{node}"
    indexing_settings.index_jetstream_servers = [natsd]
    indexing_settings.index_jetstream_stream = "node"
    indexing_settings.index_jetstream_group = "node-{node}"
    await nc.drain()
    await nc.close()

    yield


@pytest.fixture(scope="function")
async def indexing_utility_registered():
    indexing_util = IndexingUtility(
        nats_creds=indexing_settings.index_jetstream_auth,
        nats_servers=indexing_settings.index_jetstream_servers,
        nats_target=indexing_settings.index_jetstream_target,
        nats_entities=indexing_settings.entities_jetstream_target,
    )
    await indexing_util.initialize()
    set_utility(Utility.INDEXING, indexing_util)
    yield
    await indexing_util.finalize()
    if get_utility(Utility.INDEXING):
        clean_utility(Utility.INDEXING)
