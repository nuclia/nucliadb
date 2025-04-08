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

import nats
import pytest
from nats.js.api import ConsumerConfig
from pytest_docker_fixtures import images  # type: ignore
from pytest_docker_fixtures.containers._base import BaseImage  # type: ignore

from nucliadb.common.cluster.settings import settings as cluster_settings
from nucliadb_telemetry.jetstream import (
    get_traced_jetstream,
    get_traced_nats_client,
)
from nucliadb_utils.tests.fixtures import get_testing_storage_backend
from tests.ndbfixtures import SERVICE_NAME

logger = logging.getLogger(__name__)

images.settings["nidx"] = {
    "image": "nidx",
    "version": "latest",
    "env": {},
    "options": {
        # A few indexers on purpose for faster indexing
        "command": [
            "nidx",
            "api",
            "searcher",
            "indexer",
            "indexer",
            "indexer",
            "indexer",
            "scheduler",
            "worker",
        ],
        "ports": {"10000": ("0.0.0.0", 0), "10001": ("0.0.0.0", 0)},
        "publish_all_ports": False,
        "platform": "linux/amd64",
    },
}


def get_container_host(container_obj):
    return container_obj.attrs["NetworkSettings"]["IPAddress"]


class NidxImage(BaseImage):
    name = "nidx"


@pytest.fixture(scope="session")
def gcs_nidx_storage(gcs):
    return {
        "INDEXER__OBJECT_STORE": "gcs",
        "INDEXER__BUCKET": "indexing",
        "INDEXER__ENDPOINT": gcs,
        "STORAGE__OBJECT_STORE": "gcs",
        "STORAGE__ENDPOINT": gcs,
        "STORAGE__BUCKET": "nidx",
    }


@pytest.fixture(scope="session")
def s3_nidx_storage(s3):
    return {
        "INDEXER__OBJECT_STORE": "s3",
        "INDEXER__BUCKET": "indexing",
        "INDEXER__ENDPOINT": s3,
        "STORAGE__OBJECT_STORE": "s3",
        "STORAGE__ENDPOINT": s3,
        "STORAGE__BUCKET": "nidx",
    }


@pytest.fixture(scope="session")
def nidx_storage(request):
    backend = get_testing_storage_backend()
    if backend == "gcs":
        return request.getfixturevalue("gcs_nidx_storage")
    elif backend == "s3":
        return request.getfixturevalue("s3_nidx_storage")


@pytest.fixture(scope="session")
async def nidx(natsd, nidx_storage, pg):
    # Create needed NATS stream/consumer
    nc = await nats.connect(servers=[natsd])
    nc = get_traced_nats_client(nc, SERVICE_NAME)
    js = get_traced_jetstream(nc, SERVICE_NAME)
    await js.add_stream(name="nidx", subjects=["nidx"])
    await js.add_consumer(stream="nidx", config=ConsumerConfig(name="nidx"))
    await nc.drain()
    await nc.close()

    # Run nidx
    images.settings["nidx"]["env"] = {
        "RUST_LOG": "info",
        "METADATA__DATABASE_URL": f"postgresql://postgres:postgres@172.17.0.1:{pg[1]}/postgres",
        "INDEXER__NATS_SERVER": natsd.replace("localhost", "172.17.0.1"),
        **nidx_storage,
    }
    image = NidxImage()
    image.run()

    api_port = image.get_port(10000)
    searcher_port = image.get_port(10001)

    # Configure settings
    from nucliadb_utils.settings import indexing_settings

    cluster_settings.nidx_api_address = f"localhost:{api_port}"
    cluster_settings.nidx_searcher_address = f"localhost:{searcher_port}"
    indexing_settings.index_nidx_subject = "nidx"

    yield

    image.stop()
