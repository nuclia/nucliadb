from nucliadb_dataset.pt import NucliaTorchDataset
from nucliadb_protos.train_pb2 import TrainSet, Type

from nucliadb_dataset.streamer import Streamer
from nucliadb_sdk.knowledgebox import KnowledgeBox

import torch.utils.data


def test_pt_dataset(knowledgebox: KnowledgeBox, upload_demo):
    # First create a dataset
    # NER / LABELER
    # create a URL

    trainset = TrainSet()
    trainset.type = Type.PARAGRAPH_CLASSIFICATION

    streamer = Streamer(trainset, knowledgebox.client)

    ds = NucliaTorchDataset(streamer=streamer)

    data = list(torch.utils.data.DataLoader(ds, num_workers=0))
    import pdb

    pdb.set_trace()
