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
import dataclasses
import logging
import os
import platform
import sys
from typing import Iterator
from unittest.mock import AsyncMock, patch

import nats
import pytest
from nats.js.api import ConsumerConfig
from pytest_docker_fixtures import images  # type: ignore
from pytest_docker_fixtures.containers._base import BaseImage  # type: ignore

from nucliadb.common.cluster.settings import settings as cluster_settings
from nucliadb.common.nidx import NidxUtility
from nucliadb_utils.settings import indexing_settings
from nucliadb_utils.tests.fixtures import get_testing_storage_backend
from nucliadb_utils.utilities import Utility
from tests.ndbfixtures.utils import global_utility

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class Image:
    name: str
    version: str


def get_image() -> Image:
    """
    Returns the nidx image to be used in tests.
    By default, it expects to find a locally-build image named `nidx`, but it
    can be overridden to use the dockerhub image instead (nuclia/nidx:latest).
    """
    locally_built_image_tag = "nidx"
    image_tag = os.environ.get("NIDX_IMAGE", locally_built_image_tag)
    if ":" not in image_tag:
        image_tag += ":latest"
    name, version = image_tag.split(":", 1)
    return Image(name=name, version=version)


def get_platform():
    if sys.platform.startswith("darwin"):
        # macOS uses Darwin, but we want to run on Linux containers
        return f"linux/{platform.machine()}"
    elif sys.platform.startswith("linux"):
        # If we are on Linux, we can return the architecture directly
        return "linux/amd64"
    else:
        raise ValueError("Unsupported platform: {}".format(sys.platform))


images.settings["nidx"] = {
    "image": get_image().name,
    "version": get_image().version,
    "env": {
        "RUST_BACKTRACE": "1",
        "RUST_LOG": "debug",
    },
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
        "platform": get_platform(),
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
        "INDEXER__REGION_NAME": "nidx",
        "INDEXER__ENDPOINT": s3,
        "STORAGE__OBJECT_STORE": "s3",
        "STORAGE__ENDPOINT": s3,
        "STORAGE__BUCKET": "nidx",
        "STORAGE__REGION_NAME": "nidx",
    }


@pytest.fixture(scope="session")
def azure_nidx_storage(azurite):
    endpoint = f"http://172.17.0.1:{azurite.port}/devstoreaccount1"
    return {
        "INDEXER__OBJECT_STORE": "azure",
        "INDEXER__CONTAINER_URL": "https://devstoreaccount1.blob.core.windows.net/indexing",
        "INDEXER__ACCOUNT_KEY": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
        "INDEXER__ENDPOINT": endpoint,
        "STORAGE__OBJECT_STORE": "azure",
        "STORAGE__CONTAINER_URL": "https://devstoreaccount1.blob.core.windows.net/nidx",
        "STORAGE__ACCOUNT_KEY": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
        "STORAGE__ENDPOINT": endpoint,
    }


@pytest.fixture(scope="session")
def in_memory_nidx_storage():
    return {
        "INDEXER__OBJECT_STORE": "memory",
        "STORAGE__OBJECT_STORE": "memory",
    }


@pytest.fixture(scope="session")
def nidx_storage(request) -> dict[str, str]:
    backend = get_testing_storage_backend()
    if backend == "gcs":
        return request.getfixturevalue("gcs_nidx_storage")
    elif backend == "s3":
        return request.getfixturevalue("s3_nidx_storage")
    elif backend == "azure":
        return request.getfixturevalue("azure_nidx_storage")
    elif backend == "file":
        return {}
    else:
        return request.getfixturevalue("in_memory_nidx_storage")


@pytest.fixture(scope="session")
async def nidx(natsd: str, nidx_storage: dict[str, str], pg):
    # Create needed NATS stream/consumer
    nc = await nats.connect(servers=[natsd])
    js = nc.jetstream()
    await js.add_stream(name="nidx", subjects=["nidx"])
    await js.add_consumer(stream="nidx", config=ConsumerConfig(name="nidx"))
    await nc.drain()
    await nc.close()

    # Run nidx
    images.settings["nidx"].setdefault("env", {}).update(
        {
            "RUST_LOG": "info",
            "METADATA__DATABASE_URL": f"postgresql://postgres:postgres@172.17.0.1:{pg[1]}/postgres",
            "INDEXER__NATS_SERVER": natsd.replace("localhost", "172.17.0.1"),
            **nidx_storage,
        }
    )
    image = NidxImage()
    image.run()

    api_port = image.get_port(10000)
    searcher_port = image.get_port(10001)

    # Configure settings

    with (
        patch.object(cluster_settings, "nidx_api_address", f"localhost:{api_port}"),
        patch.object(cluster_settings, "nidx_searcher_address", f"localhost:{searcher_port}"),
        patch.object(indexing_settings, "index_nidx_subject", "nidx"),
        patch.object(indexing_settings, "index_jetstream_servers", [natsd]),
    ):
        yield

    image.stop()


@pytest.fixture(scope="function")
def dummy_nidx_utility() -> Iterator[NidxUtility]:
    class FakeNidx(NidxUtility):
        api_client = AsyncMock()
        searcher_client = AsyncMock()
        # methods
        initialize = AsyncMock()
        finalize = AsyncMock()
        index = AsyncMock()

    fake = FakeNidx()
    fake.api_client.NewShard.return_value.id = "00000"

    with global_utility(Utility.NIDX, fake):
        yield fake
