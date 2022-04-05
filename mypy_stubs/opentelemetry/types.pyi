from typing import Mapping, Optional, Sequence, Tuple, Union

AttributeValue = Union[str, bool, int, float, Sequence[str], Sequence[bool], Sequence[int], Sequence[float]]
Attributes = Optional[Mapping[str, AttributeValue]]
AttributesAsKey = Tuple[Tuple[str, Union[str, bool, int, float, Tuple[Optional[str], ...], Tuple[Optional[bool], ...], Tuple[Optional[int], ...], Tuple[Optional[float], ...]]], ...]
