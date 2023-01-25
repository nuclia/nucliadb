# Copyright (C) 2021 Bosutech XXI S.L.
#
# nucliadb is offered under the AGPL v3.0 and as commercial software.
# For commercial licensing, contact us at info@nuclia.com.
#
# AGPL:
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

from dataclasses import dataclass
from typing import Any, List, Optional, Tuple


@dataclass
class Vector:
    value: List[float]
    vectorset: str
    key: Optional[str] = None
    positions: Optional[Tuple[int, int]] = None


Vectors = List[Vector]


def convert_vector(vector: Any) -> List[float]:
    if (
        vector.__class__.__module__ == "numpy"
        and vector.__class__.__name__ == "ndarray"
    ):
        vector = vector.tolist()

    if (
        vector.__class__.__module__ == "tensorflow.python.framework.ops"
        and vector.__class__.__name__ == "EagerTensor"
    ):
        vector = vector.numpy().tolist()

    if vector.__class__.__module__ == "torch" and vector.__class__.__name__ == "Tensor":
        vector = vector.tolist()

    return vector
