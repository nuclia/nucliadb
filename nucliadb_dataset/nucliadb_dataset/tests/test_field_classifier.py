import os
import re
import tempfile
from uuid import uuid4

import pyarrow as pa
from nucliadb_protos.train_pb2 import TrainSet, Type

from nucliadb_dataset.dataset import NucliaDBDataset
from nucliadb_dataset.export import FileSystemExport, NucliaDatasetsExport
from nucliadb_sdk.knowledgebox import KnowledgeBox


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
        files = os.listdir(tmpdirname)
        for filename in files:
            with open(f"{tmpdirname}/{filename}", "rb") as source:
                loaded_array = pa.ipc.open_file(source).read_all()
                assert len(loaded_array) == 2


def run_dataset_export(requests_mock, knowledgebox, trainset):
    mock_dataset_id = str(uuid4())
    requests_mock.real_http = True
    requests_mock.register_uri(
        "POST",
        "http://datasets.service/datasets",
        status_code=201,
        json={"id": mock_dataset_id},
    )
    requests_mock.register_uri(
        "PUT",
        re.compile(rf"^http://datasets.service/dataset/{mock_dataset_id}/partition/.*"),
        status_code=204,
        content=b"",
    )

    with tempfile.TemporaryDirectory() as tmpdirname:
        fse = NucliaDatasetsExport(
            apikey="XXX",
            nucliadb_kb_url=knowledgebox.client.url,
            datasets_url="http://datasets.service",
            trainset=trainset,
            cache_path=tmpdirname,
            environment=knowledgebox.client.environment,
        )
        fse.export()


def test_nucliadb_export_fields(
    knowledgebox: KnowledgeBox, upload_data_field_classification, requests_mock
):
    trainset = TrainSet()
    trainset.type = Type.FIELD_CLASSIFICATION
    trainset.filter.labels.append("labelset1")
    trainset.batch_size = 2

    run_dataset_export(requests_mock, knowledgebox, trainset)


# def test_nucliadb_export_tokens(knowledgebox: KnowledgeBox, upload_data_token_classification, requests_mock):
#     trainset = TrainSet()
#     trainset.type = Type.TOKEN_CLASSIFICATION
#     trainset.filter.labels.append("labelset1")
#     trainset.batch_size = 2

#     run_dataset_export(requests_mock, knowledgebox, trainset)


# def test_nucliadb_export_paragraphs(knowledgebox: KnowledgeBox, upload_data_paragraph_classification, requests_mock):
#     trainset = TrainSet()
#     trainset.type = Type.PARAGRAPH_CLASSIFICATION
#     trainset.filter.labels.append("labelset1")
#     trainset.batch_size = 2

#     run_dataset_export(requests_mock, knowledgebox, trainset)


# def test_nucliadb_export_sentences(knowledgebox: KnowledgeBox, upload_data_sentence_classification, requests_mock):
#     trainset = TrainSet()
#     trainset.type = Type.SENTENCE_CLASSIFICATION
#     trainset.filter.labels.append("labelset1")
#     trainset.batch_size = 2

#     run_dataset_export(requests_mock, knowledgebox, trainset)


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

        with open(filename, "rb") as source:
            loaded_array = pa.ipc.open_file(source).read_all()
            assert len(loaded_array) == 2
