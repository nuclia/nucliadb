from typing import List
from pydantic import BaseModel


class TrainSetPartitions(BaseModel):
    partitions: List[str]
