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

from typing import Any, Callable, Iterator, List, Optional, Tuple

import pyarrow as pa  # type: ignore
from nucliadb_protos.dataset_pb2 import (
    FieldClassificationBatch,
    ParagraphClassificationBatch,
    TaskType,
    TokenClassificationBatch,
    TrainSet,
)

from nucliadb_dataset.mapping import (
    batch_to_text_classification_arrow,
    batch_to_text_classification_normalized_arrow,
    batch_to_token_classification_arrow,
    bytes_to_batch,
)
from nucliadb_dataset.streamer import Streamer, StreamerAlreadyRunning
from nucliadb_models.entities import KnowledgeBoxEntities
from nucliadb_models.labels import KnowledgeBoxLabels
from nucliadb_sdk.client import NucliaDBClient

ACTUAL_PARTIITON = "actual_partition"


class NucliaDBDataset:
    labels: Optional[KnowledgeBoxLabels] = None
    entities: Optional[KnowledgeBoxEntities] = None

    def __init__(self, trainset: TrainSet, client: NucliaDBClient, base_path: str):
        self.client = client
        self.trainset = trainset
        self.base_url = self.client.url
        self.base_path = base_path
        self.mappings: List[Callable] = []
        self.streamer = Streamer(self.trainset, self.client)

        if self.trainset.type == TaskType.PARAGRAPH_CLASSIFICATION:
            self.configure_paragraph_classification()

        if self.trainset.type == TaskType.FIELD_CLASSIFICATION:
            self.configure_field_classification()

        if self.trainset.type == TaskType.TOKEN_CLASSIFICATION:
            self.configure_token_classification()

        if self.trainset.type == TaskType.SENTENCE_CLASSIFICATION:
            self.configure_sentence_classification()

    def configure_sentence_classification(self):
        if len(self.trainset.filter.labels) != 1:
            raise Exception("Needs to be only on filter labelset to train")
        self.labels = self.client.get_labels()
        labelset = self.trainset.filter.labels[0]
        if labelset not in self.labels.labelsets:
            raise Exception("Labelset is not valid")
        self.set_mappings(
            [
                bytes_to_batch(ParagraphClassificationBatch),
                batch_to_text_classification_normalized_arrow,
            ]
        )
        self.setschema(
            pa.schema(
                [
                    pa.field("text", pa.string()),
                    pa.field("labels", pa.list(pa.string())),
                ]
            )
        )

    def configure_field_classification(self):
        if len(self.trainset.filter.labels) != 1:
            raise Exception("Needs to have only one labelset filter to train")
        self.labels = self.client.get_labels()
        labelset = self.trainset.filter.labels[0]
        if labelset not in self.labels.labelsets:
            raise Exception("Labelset is not valid")

        if "RESOURCES" not in self.labels.labelsets[labelset].kind:
            raise Exception("Labelset not defined for Field Classification")

        self.set_mappings(
            [
                bytes_to_batch(FieldClassificationBatch),
                batch_to_text_classification_arrow,
            ]
        )
        self.set_schema(
            pa.schema(
                [
                    pa.field("text", pa.string()),
                    pa.field("labels", pa.list_(pa.string())),
                ]
            )
        )

    def configure_token_classification(self):
        if len(self.trainset.filter.labels) != 1:
            raise Exception("Needs to have only one labelset filter to train")
        self.entities = self.client.get_entities()
        for family_group in self.trainset.filter.labels:
            if family_group not in self.entities.groups:
                raise Exception("Family group is not valid")

        self.set_mappings(
            [
                bytes_to_batch(TokenClassificationBatch),
                batch_to_token_classification_arrow,
            ]
        )
        self.set_schema(
            pa.schema(
                [
                    pa.field("text", pa.list_(pa.string())),
                    pa.field("labels", pa.list_(pa.string())),
                ]
            )
        )

    def configure_paragraph_classification(self):
        if len(self.trainset.filter.labels) != 1:
            raise Exception("Needs to have only one labelset filter to train")
        self.labels = self.client.get_labels()
        labelset = self.trainset.filter.labels[0]

        if labelset not in self.labels.labelsets:
            raise Exception("Labelset is not valid")

        if "PARAGRAPHS" not in self.labels.labelsets[labelset].kind:
            raise Exception("Labelset not defined for Paragraphs Classification")

        self.set_mappings(
            [
                bytes_to_batch(ParagraphClassificationBatch),
                batch_to_text_classification_arrow,
            ]
        )
        self.set_schema(
            pa.schema(
                [
                    pa.field("text", pa.string()),
                    pa.field("labels", pa.list_(pa.string())),
                ]
            )
        )

    def get_partitions(self):
        partitions = self.client.reader_session.get(f"{self.base_url}/trainset").json()
        if len(partitions["partitions"]) == 0:
            raise KeyError("There is no partitions")
        return partitions["partitions"]

    def map(self, batch: Any):
        for func in self.mappings:
            batch = func(batch)
        return batch

    def generate_partition(self, partition_id: str, filename: Optional[str] = None):
        if self.streamer.initialized:
            raise StreamerAlreadyRunning()
        self.streamer.initialize(partition_id)

        if filename is None:
            filename = partition_id

        counter = 0
        filename = f"{self.base_path}/{filename}.arrow"
        print(f"Generating partition {partition_id} from {self.base_url} at {filename}")
        with open(filename, "wb") as sink:
            with pa.ipc.new_stream(sink, self.schema) as writer:
                for batch in self.streamer:
                    print(f"\r {counter}")
                    batch = self.map(batch)
                    if batch is None:
                        break
                    writer.write_batch(batch)
                    counter += 1
        print("-" * 10)
        self.streamer.finalize()
        return filename

    def iter_all_partitions(self) -> Iterator[Tuple[str, str]]:
        partitions = self.get_partitions()
        for index, partition in enumerate(partitions):
            print(f"Generating partition {partition} {index}/{len(partitions)}")
            filename = self.generate_partition(partition, ACTUAL_PARTIITON)
            print("done")
            yield partition, filename

    def generate_all_partitions(self) -> List[str]:
        partitions = self.get_partitions()
        for index, partition in enumerate(partitions):
            print(f"Generating partition {partition} {index}/{len(partitions)}")
            self.generate_partition(partition)
            print("done")
        return [f"{self.base_path}/{partition}" for partition in partitions]

    def set_mappings(self, funcs: List[Callable[[Any, Any], Tuple[Any, Any]]]):
        self.mappings = funcs

    def set_schema(self, schema: pa.Schema):
        self.schema = schema
