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

import re
import tempfile
from io import BytesIO
from typing import Optional

import boto3
import docker  # type: ignore
import pytest
import requests
from google.auth.credentials import AnonymousCredentials  # type: ignore
from google.cloud import storage  # type: ignore
from pytest_docker_fixtures import images  # type: ignore
from pytest_docker_fixtures.containers._base import BaseImage  # type: ignore

from nucliadb_sdk.entities import Entity
from nucliadb_sdk.knowledgebox import KnowledgeBox
from nucliadb_sdk.labels import LabelType

DOCKER_ENV_GROUPS = re.search(r"//([^:]+)", docker.from_env().api.base_url)
DOCKER_HOST: Optional[str] = DOCKER_ENV_GROUPS.group(1) if DOCKER_ENV_GROUPS else None  # type: ignore


@pytest.fixture(scope="function")
def upload_data_field_classification(knowledgebox: KnowledgeBox):
    knowledgebox.set_labels("labelset1", ["A", "B"], LabelType.RESOURCES)
    knowledgebox.set_labels("labelset2", ["C"], LabelType.RESOURCES)
    knowledgebox.upload("doc1", text="This is my lovely text", labels=["labelset1/A"])
    knowledgebox.upload(
        "doc2",
        text="This is my lovely text2",
        labels=["labelset1/B", "labelset2/C"],
    )


@pytest.fixture(scope="function")
async def upload_data_paragraph_classification(knowledgebox: KnowledgeBox):
    knowledgebox.set_labels("labelset1", ["label1", "label2"], LabelType.PARAGRAPHS)
    knowledgebox.set_labels("labelset2", ["label1", "label2"], LabelType.PARAGRAPHS)
    knowledgebox.upload(
        "doc1", text="This is my lovely text", labels=["labelset1/label1"]
    )
    knowledgebox.upload(
        "doc2",
        text="This is my lovely text2",
        labels=["labelset1/label1", "labelset1/label2"],
    )
    knowledgebox.upload(
        "doc3",
        text="Yet another lovely text",
        labels=["labelset1/label2", "labelset2/label1"],
    )


@pytest.fixture(scope="function")
def upload_data_token_classification(knowledgebox: KnowledgeBox):
    knowledgebox.set_entities("PERSON", ["Ramon", "Carmen Iniesta", "Eudald Camprubi"])
    knowledgebox.set_entities("ANIMAL", ["lion", "tiger", "cheetah"])
    knowledgebox.upload(
        "doc1",
        text="Ramon This is my lovely text",
        entities=[Entity(type="PERSON", value="Ramon", positions=[(0, 5)])],
    )
    knowledgebox.upload(
        "doc2",
        text="Carmen Iniesta shows an amazing classifier to Eudald Camprubi",
        entities=[
            Entity(type="PERSON", value="Carmen Iniesta", positions=[(0, 14)]),
            Entity(type="PERSON", value="Eudald Camprubi", positions=[(46, 61)]),
        ],
    )
    knowledgebox.upload(
        "doc3",
        text="Which is the fastest animal, a lion, a tiger or a cheetah?",
        entities=[
            Entity(type="ANIMAL", value="lion", positions=[(31, 35)]),
            Entity(type="ANIMAL", value="tiger", positions=[(39, 44)]),
            Entity(type="ANIMAL", value="cheetah", positions=[(50, 57)]),
        ],
    )


@pytest.fixture(scope="function")
def temp_folder():
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield tmpdirname


images.settings["gcs"] = {
    "image": "fsouza/fake-gcs-server",
    "version": "1.44.1",
    "options": {
        "command": f"-scheme http -external-url http://{DOCKER_HOST}:4443 -port 4443",
        "ports": {"4443": "4443"},
    },
}

images.settings["s3"] = {
    "image": "localstack/localstack",
    "version": "0.12.18",
    "env": {"SERVICES": "s3"},
    "options": {
        "ports": {"4566": None, "4571": None},
    },
}


class GCS(BaseImage):
    name = "gcs"
    port = 4443

    def check(self):
        try:
            response = requests.get(
                f"http://{self.host}:{self.get_port()}/storage/v1/b"
            )
            return response.status_code == 200
        except:  # noqa
            return False


@pytest.fixture(scope="session")
def gcs():
    container = GCS()
    host, port = container.run()
    public_api_url = f"http://{host}:{port}"
    yield public_api_url
    container.stop()


@pytest.fixture(scope="session")
def gcs_with_partitions(gcs):
    def upload_random_file_to_bucket(
        client, bucket_name, destination_blob_name, file_size
    ):
        random_file = BytesIO()
        random_file.write(b"1" * file_size)
        random_file.seek(0)

        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_file(random_file)

    client = storage.Client(
        project="project",
        credentials=AnonymousCredentials(),
        client_options={"api_endpoint": gcs},
    )
    client.create_bucket("test_cloud_datasets")
    filesize = 10 * 1024 * 1024
    upload_random_file_to_bucket(
        client, "test_cloud_datasets", "datasets/123456/partition1.arrow", filesize
    )
    upload_random_file_to_bucket(
        client, "test_cloud_datasets", "datasets/123456/partition2.arrow", filesize
    )
    yield


class S3(BaseImage):
    name = "s3"
    port = 4566

    def check(self):
        try:
            response = requests.get(f"http://{self.host}:{self.get_port()}")
            return response.status_code == 404
        except Exception:  # pragma: no cover
            return False


@pytest.fixture(scope="session")
def s3():
    container = S3()
    host, port = container.run()
    public_api_url = f"http://{host}:{port}"
    yield public_api_url
    container.stop()


@pytest.fixture(scope="session")
def s3_with_partitions(s3):
    def upload_random_file_to_bucket(
        client, bucket_name, destination_blob_name, file_size
    ):
        random_file = BytesIO()
        random_file.write(b"1" * file_size)
        random_file.seek(0)

        client.put_object(
            Bucket=bucket_name, Key=destination_blob_name, Body=random_file
        )

    client = boto3.client(
        "s3",
        aws_access_key_id="",
        aws_secret_access_key="",
        use_ssl=False,
        verify=False,
        endpoint_url=s3,
        region_name=None,
    )

    client.create_bucket(Bucket="testclouddatasets")
    filesize = 10 * 1024 * 1024
    upload_random_file_to_bucket(
        client, "testclouddatasets", "datasets/123456/partition1.arrow", filesize
    )
    upload_random_file_to_bucket(
        client, "testclouddatasets", "datasets/123456/partition2.arrow", filesize
    )
    yield
