from datasets import IterableDataset
from nucliadb_dataset.dataset import NucliaDBDataset
from nucliadb_dataset.export import FileSystemExport, NucliaDatasetsExport
from nucliadb_protos.train_pb2 import TrainSet, Type

from nucliadb_dataset.streamer import Streamer
from nucliadb_sdk.client import Environment
from nucliadb_sdk.knowledgebox import KnowledgeBox
import tempfile


def test_filesystem(knowledgebox: KnowledgeBox, upload_data_field_classification):
    trainset = TrainSet()
    trainset.type = Type.FIELD_CLASSIFICATION
    trainset.filter.labels.append("labelset1")
    trainset.batch_size = 2

    with tempfile.TemporaryDirectory() as tmpdirname:
        fse = FileSystemExport(
            nucliadb_kb_url=knowledgebox.client.url,
            trainset=trainset,
            store_path=tmpdirname,
            environment=knowledgebox.client.environment,
        )
        fse.export()


def test_nucliadb(knowledgebox: KnowledgeBox, upload_data_field_classification):
    trainset = TrainSet()
    trainset.type = Type.FIELD_CLASSIFICATION
    trainset.filter.labels.append("labelset1")
    trainset.batch_size = 2

    with tempfile.TemporaryDirectory() as tmpdirname:
        fse = NucliaDatasetsExport(
            nucliadb_kb_url=knowledgebox.client.url,
            trainset=trainset,
            store_path=tmpdirname,
            environment=knowledgebox.client.environment,
            apikey="XXX",
        )
        fse.export()


def test_live(knowledgebox: KnowledgeBox, upload_data_field_classification):
    trainset = TrainSet()
    trainset.type = Type.FIELD_CLASSIFICATION
    trainset.filter.labels.append("labelset1")
    trainset.batch_size = 2

    with tempfile.TemporaryDirectory() as tmpdirname:
        fse = NucliaDBDataset(
            client=knowledgebox.client,
            trainset=trainset,
            base_path=tmpdirname,
        )
        partitions = fse.get_partitions()
        assert len(partitions) == 1
        filename = fse.generate_partition(partitions[0])
        import pdb

        pdb.set_trace()
