import torch

from nucliadb_dataset.streamer import Streamer


class NucliaTorchDataset(torch.utils.data.IterableDataset):
    def __init__(self, streamer: Streamer):
        super(NucliaTorchDataset, self).__init__()
        self.streamer = streamer

    def __iter__(self):
        return self.streamer.next()
