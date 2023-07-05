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

import base64
import json
import os
import re
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

import boto3
import pyarrow as pa  # type: ignore
from google.auth.credentials import AnonymousCredentials  # type: ignore
from google.cloud import storage  # type: ignore
from google.oauth2 import service_account  # type: ignore
from nucliadb_protos.dataset_pb2 import (
    FieldClassificationBatch,
    ImageClassificationBatch,
    ParagraphClassificationBatch,
    SentenceClassificationBatch,
    TaskType,
    TokenClassificationBatch,
    TrainSet,
)

from nucliadb_dataset.mapping import (
    batch_to_image_classification_arrow,
    batch_to_text_classification_arrow,
    batch_to_text_classification_normalized_arrow,
    batch_to_token_classification_arrow,
    bytes_to_batch,
)
from nucliadb_dataset.streamer import Streamer, StreamerAlreadyRunning
from nucliadb_models.entities import KnowledgeBoxEntities
from nucliadb_models.labels import KnowledgeBoxLabels
from nucliadb_sdk.client import NucliaDBClient
from nucliadb_sdk.knowledgebox import KnowledgeBox
from nucliadb_sdk.utils import get_kb

CHUNK_SIZE = 5 * 1024 * 1024

if TYPE_CHECKING:  # pragma: no cover
    TaskValue = TaskType.V
else:
    TaskValue = int

ACTUAL_PARTITION = "actual_partition"


class Task(str, Enum):
    PARAGRAPH_CLASSIFICATION = "PARAGRAPH_CLASSIFICATION"
    FIELD_CLASSIFICATION = "FIELD_CLASSIFICATION"
    SENTENCE_CLASSIFICATION = "SENTENCE_CLASSIFICATION"
    TOKEN_CLASSIFICATION = "TOKEN_CLASSIFICATION"
    IMAGE_CLASSIFICATION = "IMAGE_CLASSIFICATION"


class NucliaDataset(object):
    labels: Optional[KnowledgeBoxLabels]
    entities: Optional[KnowledgeBoxEntities]

    def __new__(cls, *args, **kwargs):
        if cls is NucliaDataset:
            raise TypeError(
                f"'{cls.__name__}' can't be instantiated, use its child classes"
            )
        return super().__new__(cls)

    def __init__(
        self,
        base_path: Optional[str] = None,
    ):
        if base_path is None:
            base_path = os.getcwd()
        self.base_path = base_path
        self.mappings: List[Callable] = []

        self.labels = None
        self.entities = None
        self.folder = None

    def iter_all_partitions(self, force=False) -> Iterator[Tuple[str, str]]:
        partitions = self.get_partitions()
        for index, partition in enumerate(partitions):
            print(f"Reading partition {partition} {index}/{len(partitions)}")
            filename = self.read_partition(partition, ACTUAL_PARTITION, force)
            print("done")
            yield partition, filename

    def read_all_partitions(self, force=False, path: Optional[str] = None) -> List[str]:
        partitions = self.get_partitions()
        result = []
        for index, partition in enumerate(partitions):
            print(f"Reading partition {partition} {index}/{len(partitions)}")
            filename = self.read_partition(partition, force=force, path=path)
            result.append(filename)
            print("done")
        return result

    def get_partitions(self):
        raise NotImplementedError()

    def read_partition(
        self,
        partition_id: str,
        filename: Optional[str] = None,
        force: bool = False,
        path: Optional[str] = None,
    ):
        raise NotImplementedError()


class NucliaDBDataset(NucliaDataset):
    def __init__(
        self,
        client: NucliaDBClient,
        task: Optional[Task] = None,
        labels: Optional[List[str]] = None,
        trainset: Optional[TrainSet] = None,
        base_path: Optional[str] = None,
    ):
        super().__init__(base_path)

        if labels is None:
            labels = []

        if trainset is None and task is not None:
            if Task.PARAGRAPH_CLASSIFICATION == task:
                trainset = TrainSet(type=TaskType.PARAGRAPH_CLASSIFICATION)
                trainset.filter.labels.extend(labels)
            elif Task.FIELD_CLASSIFICATION == task:
                trainset = TrainSet(type=TaskType.FIELD_CLASSIFICATION)
                trainset.filter.labels.extend(labels)
            elif Task.SENTENCE_CLASSIFICATION == task:
                trainset = TrainSet(type=TaskType.SENTENCE_CLASSIFICATION)
                trainset.filter.labels.extend(labels)
            elif Task.TOKEN_CLASSIFICATION == task:
                trainset = TrainSet(type=TaskType.TOKEN_CLASSIFICATION)
                trainset.filter.labels.extend(labels)
            elif Task.IMAGE_CLASSIFICATION == task:
                trainset = TrainSet(type=TaskType.IMAGE_CLASSIFICATION)
            else:
                raise KeyError("Not a valid task")
        elif trainset is None and task is None:
            raise AttributeError("Trainset or task needs to be defined")

        if trainset is None:
            raise AttributeError("Trainset could not be defined")

        self.trainset = trainset
        self.client = client
        self.knowledgebox = KnowledgeBox(self.client)
        self.streamer = Streamer(self.trainset, self.client)

        if self.trainset.type == TaskType.PARAGRAPH_CLASSIFICATION:
            self._configure_paragraph_classification()

        if self.trainset.type == TaskType.FIELD_CLASSIFICATION:
            self._configure_field_classification()

        if self.trainset.type == TaskType.TOKEN_CLASSIFICATION:
            self._configure_token_classification()

        if self.trainset.type == TaskType.SENTENCE_CLASSIFICATION:
            self._configure_sentence_classification()

        if self.trainset.type == TaskType.IMAGE_CLASSIFICATION:
            self._configure_image_classification()

    def _configure_sentence_classification(self):
        self.labels = self.client.get_labels()
        labelset = self.trainset.filter.labels[0]
        if labelset not in self.labels.labelsets:
            raise Exception("Labelset is not valid")
        self._set_mappings(
            [
                bytes_to_batch(SentenceClassificationBatch),
                batch_to_text_classification_normalized_arrow,
            ]
        )
        self._set_schema(
            pa.schema(
                [
                    pa.field("text", pa.string()),
                    pa.field("labels", pa.list_(pa.string())),
                ]
            )
        )

    def _configure_field_classification(self):
        if len(self.trainset.filter.labels) != 1:
            raise Exception("Needs to have only one labelset filter to train")
        self.labels = self.client.get_labels()
        labelset = self.trainset.filter.labels[0]
        computed_labelset = False

        if labelset not in self.labels.labelsets:
            if labelset in self.knowledgebox.get_uploaded_labels():
                computed_labelset = True
            else:
                raise Exception("Labelset is not valid")

        if (
            computed_labelset is False
            and "RESOURCES" not in self.labels.labelsets[labelset].kind
        ):
            raise Exception("Labelset not defined for Field Classification")

        self._set_mappings(
            [
                bytes_to_batch(FieldClassificationBatch),
                batch_to_text_classification_arrow,
            ]
        )
        self._set_schema(
            pa.schema(
                [
                    pa.field("text", pa.string()),
                    pa.field("labels", pa.list_(pa.string())),
                ]
            )
        )

    def _configure_token_classification(self):
        self.entities = self.client.get_entities()
        for family_group in self.trainset.filter.labels:
            if family_group not in self.entities.groups:
                raise Exception("Family group is not valid")

        schema = pa.schema(
            [
                pa.field("text", pa.list_(pa.string())),
                pa.field("labels", pa.list_(pa.string())),
            ]
        )
        self._set_mappings(
            [
                bytes_to_batch(TokenClassificationBatch),
                batch_to_token_classification_arrow(schema),
            ]
        )
        self._set_schema(schema)

    def _configure_paragraph_classification(self):
        if len(self.trainset.filter.labels) != 1:
            raise Exception("Needs to have only one labelset filter to train")
        self.labels = self.client.get_labels()
        labelset = self.trainset.filter.labels[0]

        if labelset not in self.labels.labelsets:
            raise Exception("Labelset is not valid")

        if "PARAGRAPHS" not in self.labels.labelsets[labelset].kind:
            raise Exception("Labelset not defined for Paragraphs Classification")

        self._set_mappings(
            [
                bytes_to_batch(ParagraphClassificationBatch),
                batch_to_text_classification_arrow,
            ]
        )
        self._set_schema(
            pa.schema(
                [
                    pa.field("text", pa.string()),
                    pa.field("labels", pa.list_(pa.string())),
                ]
            )
        )

    def _configure_image_classification(self):
        self._set_mappings(
            [
                bytes_to_batch(ImageClassificationBatch),
                batch_to_image_classification_arrow,
            ]
        )
        self._set_schema(
            pa.schema(
                [
                    pa.field("image", pa.string()),
                    pa.field("selection", pa.string()),
                ]
            )
        )

    def _map(self, batch: Any):
        for func in self.mappings:
            batch = func(batch)
        return batch

    def _set_mappings(self, funcs: List[Callable[[Any, Any], Tuple[Any, Any]]]):
        self.mappings = funcs

    def _set_schema(self, schema: pa.Schema):
        self.schema = schema

    def get_partitions(self):
        """
        Get expected number of partitions from a live NucliaDB
        """
        # XXX Bad pattern: using `client` attributes objects instead of methods
        partitions = self.client.train_session.get(f"/trainset").json()
        if len(partitions["partitions"]) == 0:
            raise KeyError("There is no partitions")
        return partitions["partitions"]

    def read_partition(
        self,
        partition_id: str,
        filename: Optional[str] = None,
        force: bool = False,
        path: Optional[str] = None,
    ):
        """
        Export an arrow partition from a live NucliaDB and store it locally
        """
        if self.streamer.initialized:
            raise StreamerAlreadyRunning()

        if filename is None:
            filename = partition_id

        if path is not None:
            filename = f"{path}/{filename}.arrow"
        else:
            filename = f"{self.base_path}/{filename}.arrow"

        if os.path.exists(filename) and force is False:
            return filename

        self.streamer.initialize(partition_id)
        filename_tmp = f"{filename}.tmp"
        print(
            f"Generating partition {partition_id} from {self.streamer.base_url} at {filename}"
        )
        with open(filename_tmp, "wb") as sink:
            with pa.ipc.new_stream(sink, self.schema) as writer:
                for batch in self.streamer:
                    batch = self._map(batch)
                    if batch is None:
                        break
                    writer.write_batch(batch)
        print("-" * 10)
        self.streamer.finalize()
        os.rename(filename_tmp, filename)
        return filename


class S3DatasetsClient:
    def __init__(self, settings):
        self.client = boto3.client(
            "s3",
            aws_access_key_id=settings["client_id"],
            aws_secret_access_key=settings["client_secret"],
            use_ssl=settings["ssl"],
            verify=settings["verify_ssl"],
            endpoint_url=settings["endpoint"],
            region_name=settings["region_name"],
        )

    def list_files(self, bucket_name: str, path: str) -> List[str]:
        objects = self.client.list_objects(Bucket=bucket_name, Prefix=path)
        files_list = [obj["Key"] for obj in objects.get("Contents", [])]
        return files_list

    def download(self, bucket_name: str, filename: str, file_obj) -> None:
        obj = self.client.get_object(Bucket=bucket_name, Key=filename)
        data = obj["Body"]
        file_obj.write(data.read())


class GCSDatasetsClient:
    def __init__(self, settings):
        if settings["base64_creds"] is not None:
            account_credentials = json.loads(base64.b64decode(settings["base64_creds"]))
            credentials = service_account.Credentials.from_service_account_info(
                account_credentials,
                scopes=["https://www.googleapis.com/auth/devstorage.read_write"],
            )
        else:
            credentials = AnonymousCredentials()
        self.client = storage.Client(
            project=settings["project"],
            credentials=credentials,
            client_options={"api_endpoint": settings["endpoint_url"]},
        )

    def list_files(self, bucket_name: str, path: str) -> List[str]:
        bucket = self.client.bucket(bucket_name)
        arrow_files = [blob.name for blob in bucket.list_blobs(prefix=path)]
        return arrow_files

    def download(self, bucket_name: str, filename: str, file_obj) -> None:
        bucket = self.client.bucket(bucket_name)
        blob = bucket.get_blob(filename)

        if blob is None:
            raise ValueError(f"File {filename} not found on {bucket_name}")

        blob.download_to_file(file_obj)


class NucliaCloudDataset(NucliaDataset):
    client: Union[GCSDatasetsClient, S3DatasetsClient]

    def __init__(self, base_path: str, remote_storage: Dict[str, Any]):
        super().__init__(base_path)
        self.bucket = remote_storage["bucket"]
        self.key = remote_storage["key"]
        if remote_storage["storage_type"] == "gcs":
            self.client = GCSDatasetsClient(remote_storage["settings"])
        elif remote_storage["storage_type"] == "s3":
            self.client = S3DatasetsClient(remote_storage["settings"])

    def get_partitions(self):
        """
        Count all *.arrow files on the bucket
        """
        arrow_files = self.client.list_files(self.bucket, self.key)
        partitions = []
        for file in arrow_files:
            match = re.match(r".*\/([^\/]+).arrow", file)
            if match:
                partitions.append(match.groups()[0])
        return partitions

    def read_partition(
        self,
        partition_id: str,
        filename: Optional[str] = None,
        force: bool = False,
        path: Optional[str] = None,
    ):
        """
        Download an pregenerated arrow partition from a bucket and store it locally
        """
        if filename is None:
            filename = partition_id

        if path is not None:
            filename = f"{path}/{filename}.arrow"
        else:
            filename = f"{self.base_path}/{filename}.arrow"

        if os.path.exists(filename) and force is False:
            return filename

        filename_tmp = f"{filename}.tmp"
        print(f"Downloading partition {partition_id} from {self.bucket}/{self.key}")
        with open(filename_tmp, "wb") as downloaded_file:
            self.client.download(
                self.bucket, f"{self.key}/{partition_id}.arrow", downloaded_file
            )
            downloaded_file.flush()

        print("-" * 10)
        os.rename(filename_tmp, filename)
        return filename


def download_all_partitions(
    task: str,  # type: ignore
    slug: Optional[str] = None,
    nucliadb_base_url: Optional[str] = "http://localhost:8080",
    path: Optional[str] = None,
    knowledgebox: Optional[KnowledgeBox] = None,
    labels: Optional[List[str]] = None,
):
    if knowledgebox is None and slug is not None:
        knowledgebox = get_kb(slug, nucliadb_base_url)

    if knowledgebox is None:
        raise KeyError("KnowledgeBox not found")

    task_obj = Task(task)
    fse = NucliaDBDataset(
        client=knowledgebox.client, task=task_obj, labels=labels, base_path=path
    )
    return fse.read_all_partitions(path=path)
