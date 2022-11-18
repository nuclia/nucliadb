from dataclasses import dataclass
from typing import List, Optional, Tuple


@dataclass
class Vector:
    value: List[float]
    vectorset: str
    key: Optional[str] = None
    positions: Optional[Tuple[int, int]] = None


Vectors = List[Vector]
