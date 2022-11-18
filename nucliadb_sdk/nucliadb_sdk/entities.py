from dataclasses import dataclass
from typing import List, Tuple

EntityPosition = Tuple[int, int]


@dataclass
class Entity:
    type: str
    value: str
    positions: List[EntityPosition]


Entities = List[Entity]
