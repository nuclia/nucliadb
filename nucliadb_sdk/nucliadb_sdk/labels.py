from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Union


class LabelType(str, Enum):
    PARAGRAPHS = "PARAGRAPHS"
    RESOURCES = "RESOURCES"
    SENTENCES = "SENTENCES"


@dataclass
class Label:
    label: str
    labelset: Optional[str] = None


Labels = List[Union[Label, str]]


@dataclass
class LabelSet:
    count: int
    labels: Dict[str, int] = field(default_factory=dict)
