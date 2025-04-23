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

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

import pyarrow as pa  # type: ignore

from nucliadb_dataset.streamer import Streamer
from nucliadb_dataset.tasks import (
    ACTUAL_PARTITION,
    TASK_DEFINITIONS,
    TASK_DEFINITIONS_REVERSE,
    Task,
)
from nucliadb_models.entities import KnowledgeBoxEntities
from nucliadb_models.labels import KnowledgeBoxLabels
from nucliadb_models.resource import KnowledgeBoxObj
from nucliadb_models.trainset import TrainSetPartitions
from nucliadb_protos.dataset_pb2 import TrainSet
from nucliadb_sdk.v2.sdk import NucliaDB

logger = logging.getLogger("nucliadb_dataset")

CHUNK_SIZE = 5 * 1024 * 1024


@dataclass
class LabelSetCount:
    count: int
    labels: Dict[str, int] = field(default_factory=dict)


class NucliaDataset(object):
    labels: Optional[KnowledgeBoxLabels]
    entities: Optional[KnowledgeBoxEntities]

    def __new__(cls, *args, **kwargs):
        if cls is NucliaDataset:
            raise TypeError(f"'{cls.__name__}' can't be instantiated, use its child classes")
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
            logger.info(f"Reading partition {partition} {index}/{len(partitions)}")
            filename = self.read_partition(partition, ACTUAL_PARTITION, force)
            logger.info(f"Done reading partition {partition}")
            yield partition, filename

    def read_all_partitions(self, force=False, path: Optional[str] = None) -> List[str]:
        partitions = self.get_partitions()
        result = []
        for index, partition in enumerate(partitions):
            logger.info(f"Reading partition {partition} {index}/{len(partitions)}")
            filename = self.read_partition(partition, force=force, path=path)
            result.append(filename)
            logger.info(f"Done reading partition {partition}")
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
        sdk: NucliaDB,
        kbid: str,
        task: Optional[Task] = None,
        labels: Optional[List[str]] = None,
        trainset: Optional[TrainSet] = None,
        base_path: Optional[str] = None,
        search_sdk: Optional[NucliaDB] = None,
        reader_sdk: Optional[NucliaDB] = None,
    ):
        super().__init__(base_path)

        if labels is None:
            labels = []

        task_definition = None
        if trainset is None and task is not None:
            task_definition = TASK_DEFINITIONS.get(task)
            if task_definition is None:
                raise KeyError("Not a valid task")
            trainset = TrainSet(type=task_definition.proto)
            if task_definition.labels:
                trainset.filter.labels.extend(labels)
        elif trainset is not None:
            task_definition = TASK_DEFINITIONS_REVERSE.get(trainset.type)
        elif trainset is None and task is None:
            raise AttributeError("Trainset or task needs to be defined")

        if trainset is None or task_definition is None:
            raise AttributeError("Trainset could not be defined")

        self.kbid = kbid
        self.trainset = trainset
        self.task_definition = task_definition
        self.train_sdk = sdk
        if search_sdk is None:
            self.search_sdk = sdk
        else:
            self.search_sdk = search_sdk

        if reader_sdk is None:
            self.reader_sdk = sdk
        else:
            self.reader_sdk = reader_sdk
        self._set_schema(self.task_definition.schema)
        self._set_mappings(self.task_definition.mapping)

    def _map(self, batch: Any):
        for func in self.mappings:
            batch = func(batch, self.schema)
        return batch

    def get_streamer_for_partition(
        self,
        partition_id: str,
    ) -> Streamer:
        streamer = Streamer(
            self.trainset,
            reader_headers=self.train_sdk.headers,
            base_url=self.train_sdk.base_url,
            kbid=self.kbid,
        )
        streamer.initialize(partition_id)
        return streamer

    def _set_mappings(self, funcs: List[Callable[[Any, Any], Tuple[Any, Any]]]):
        self.mappings = funcs

    def _set_schema(self, schema: pa.Schema):
        self.schema = schema

    def get_partitions(self) -> List[str]:
        """
        Get expected number of partitions from a live NucliaDB
        """
        partitions: TrainSetPartitions = self.train_sdk.trainset(kbid=self.kbid)
        if len(partitions.partitions) == 0:
            raise KeyError("There is no partitions")
        return partitions.partitions

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
        streamer = self.get_streamer_for_partition(partition_id)

        if filename is None:
            filename = partition_id

        if path is not None:
            filename = f"{path}/{filename}.arrow"
        else:
            filename = f"{self.base_path}/{filename}.arrow"

        if os.path.exists(filename) and force is False:
            return filename

        filename_tmp = f"{filename}.tmp"
        logger.info(f"Generating partition {partition_id} from {streamer.base_url} at {filename}")
        with open(filename_tmp, "wb") as sink:
            with pa.ipc.new_stream(sink, self.schema) as writer:
                for batch in streamer:
                    batch = self._map(batch)
                    if batch is None:
                        break
                    writer.write_batch(batch)
        logger.info("Finalizing partition")
        streamer.finalize()
        os.rename(filename_tmp, filename)
        return filename


def download_all_partitions(
    task: str,
    slug: Optional[str] = None,
    kbid: Optional[str] = None,
    nucliadb_base_url: Optional[str] = "http://localhost:8080",
    path: Optional[str] = None,
    sdk: Optional[NucliaDB] = None,
    labels: Optional[List[str]] = None,
):
    if sdk is None:
        sdk = NucliaDB(region="on-prem", url=nucliadb_base_url)

    if kbid is None and slug is not None:
        kb: KnowledgeBoxObj = sdk.get_knowledge_box_by_slug(slug=slug)
        kbid = kb.uuid

    if kbid is None:
        raise KeyError("Not a valid KB")

    task_obj = Task(task)
    fse = NucliaDBDataset(sdk=sdk, task=task_obj, labels=labels, base_path=path, kbid=kbid)
    return fse.read_all_partitions(path=path)
