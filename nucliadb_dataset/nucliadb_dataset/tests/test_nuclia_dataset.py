from datasets import IterableDataset
from nucliadb_dataset.mapping import text_label_to_list
from nucliadb_protos.train_pb2 import TrainSet, Type

from nucliadb_dataset.streamer import Streamer
from nucliadb_sdk.knowledgebox import KnowledgeBox


def test_hf_dataset(knowledgebox: KnowledgeBox, upload_data_resource_classification):
    # First create a dataset
    # NER / LABELER
    # create a URL

    trainset = TrainSet()
    trainset.type = Type.RESOURCE_CLASSIFICATION
    trainset.filter.labels.append("l/labelset1")
    trainset.batch_size = 2
    trainset.seed = 1234

    streamer = Streamer(trainset=trainset, client=knowledgebox.client)
    streamer.set_mappings([text_label_to_list()])

    for X, Y in streamer:
        import pdb

        pdb.set_trace()

    for X, Y in streamer:
        import pdb

        pdb.set_trace()
