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

import nats
import pytest
from nats.js.api import ConsumerConfig
from pytest_docker_fixtures import images  # type: ignore
from pytest_docker_fixtures.containers._base import BaseImage  # type: ignore

from nucliadb.common.cluster.settings import settings as cluster_settings
from nucliadb_utils.tests.fixtures import get_testing_storage_backend

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class Image:
    name: str
    version: str


def get_image() -> Image:
    """
    Returns the nidx image to be used in tests.
    By default, it uses the image from dockerhub, but it can be overridden to use a local image.
    """
    dockerhub_image_tag = "nuclia/nidx"
    image_tag = os.environ.get("NIDX_IMAGE", dockerhub_image_tag)
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
    elif backend == "file":
        return {}
    else:
        return request.getfixturevalue("in_memory_nidx_storage")


@pytest.fixture(scope="session")
async def nidx(natsd, nidx_storage, pg):
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
    from nucliadb_utils.settings import indexing_settings

    cluster_settings.nidx_api_address = f"localhost:{api_port}"
    cluster_settings.nidx_searcher_address = f"localhost:{searcher_port}"
    indexing_settings.index_nidx_subject = "nidx"

    yield

    image.stop()
