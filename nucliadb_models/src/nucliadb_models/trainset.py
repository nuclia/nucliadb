# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, model_validator

from nucliadb_models.filters import FilterExpression


class TrainSetPartitions(BaseModel):
    partitions: list[str]


class TrainSetType(int, Enum):
    # NOTE: matches the TaskType in nucliadb_protos.dataset.proto
    FIELD_CLASSIFICATION = 0
    PARAGRAPH_CLASSIFICATION = 1
    SENTENCE_CLASSIFICATION = 2
    TOKEN_CLASSIFICATION = 3
    IMAGE_CLASSIFICATION = 4
    PARAGRAPH_STREAMING = 5
    QUESTION_ANSWER_STREAMING = 6
    FIELD_STREAMING = 7


class TrainSet(BaseModel):
    type: TrainSetType = Field(..., description="Streaming type")
    filter_expression: Optional[FilterExpression] = Field(
        default=None,
        title="Filter resource by an expression",
        description=(
            "Returns only documents that match this filter expression. "
            "Filtering examples can be found here: https://docs.nuclia.dev/docs/rag/advanced/search-filters. "
            "It is only supported on FIELD_STREAMING types."
        ),
    )
    batch_size: int = Field(
        default=5,
        description="Batch size of the resulting arrow file. This affects how many rows are read simultaneously while parsing the resulting arrow file.",
    )
    exclude_text: bool = Field(
        default=False,
        description="Set to True if the extracted text is not needed for the stream and it will not be added. This is useful to reduce the amount of data streamed.",
    )

    @model_validator(mode="after")
    def validate_filter_expressions_supported_on_stream(self):
        if self.filter_expression is not None and self.type != TrainSetType.FIELD_STREAMING:
            raise ValueError(f"{self.type.name} does not support `filter_expression` parameter yet.")
        return self
