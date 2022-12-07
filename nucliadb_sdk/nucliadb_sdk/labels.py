from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union


@dataclass
class Label:
    label: str
    labelset: Optional[str] = None


Labels = List[Union[Label, str]]


@dataclass
class LabelSet:
    count: int
    labels: Dict[str, int] = field(default_factory=dict)
