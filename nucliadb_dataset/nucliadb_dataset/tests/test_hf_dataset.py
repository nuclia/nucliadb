from datasets import IterableDataset
from nucliadb_protos.train_pb2 import TrainSet, Type

from nucliadb_dataset.streamer import Streamer
from nucliadb_sdk.knowledgebox import KnowledgeBox


def test_hf_dataset(knowledgebox: KnowledgeBox):
    # First create a dataset
    # NER / LABELER
    # create a URL

    trainset = TrainSet()
    trainset.type = Type.PARAGRAPH_CLASSIFICATION
    trainset.filter.labels.append("l/labelset1")
    trainset.batch_size = 2
    trainset.seed = 1234

    streamer = Streamer(trainset=trainset, client=knowledgebox.client)

    _ = IterableDataset.from_generator(streamer.get_data)
