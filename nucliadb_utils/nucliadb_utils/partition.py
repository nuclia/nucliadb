import mmh3  # type: ignore


class PartitionUtility:
    def __init__(self, partitions: int, seed: int):
        self.partitions = partitions
        self.seed = seed

    def generate_partition(self, kbid: str, uuid: str):
        ident = f"{kbid}/{uuid}"
        return (mmh3.hash(ident, self.seed, signed=False) % self.partitions) + 1
