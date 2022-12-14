from nucliadb_dataset.streamer import Streamer
from pyarrow import fs

from datasets import IterableDataset

from nucliadb_dataset import DSTYPE
from nucliadb_protos.train_pb2 import TrainSet
import torch


def test_pt_dataset(knowledgebox: KnowledgeBox):
    # First create a dataset
    # NER / LABELER
    # create a URL

    torch.utils.data.DataLoader(ds, num_workers=2)
