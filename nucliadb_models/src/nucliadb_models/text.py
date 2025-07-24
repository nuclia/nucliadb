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
from typing_extensions import Self

from nucliadb_models.utils import validate_json

MB = 1024 * 1024

# Shared classes


class TextFormat(Enum):
    PLAIN = "PLAIN"
    HTML = "HTML"
    RST = "RST"
    MARKDOWN = "MARKDOWN"
    JSON = "JSON"
    KEEP_MARKDOWN = "KEEP_MARKDOWN"
    JSONL = "JSONL"
    PLAIN_BLANKLINE_SPLIT = "PLAIN_BLANKLINE_SPLIT"


TEXT_FORMAT_TO_MIMETYPE = {
    TextFormat.PLAIN: "text/plain",
    TextFormat.HTML: "text/html",
    TextFormat.RST: "text/x-rst",
    TextFormat.MARKDOWN: "text/markdown",
    TextFormat.JSON: "application/json",
    TextFormat.KEEP_MARKDOWN: "text/markdown",
    TextFormat.JSONL: "application/x-ndjson",
    TextFormat.PLAIN_BLANKLINE_SPLIT: "text/plain+blankline",
}


# Visualization classes (Those used on reader endpoints)


class FieldText(BaseModel):
    body: Optional[str] = None
    format: Optional[TextFormat] = None
    md5: Optional[str] = None
    extract_strategy: Optional[str] = Field(
        default=None,
        description="Id of the Nuclia extract strategy used at processing time. If not set, the default strategy was used. Extract strategies are defined at the learning configuration api.",
    )
    split_strategy: Optional[str] = Field(
        default=None,
        description="Id of the Nuclia split strategy used at processing time. If not set, the default strategy was used. Split strategies are defined at the learning configuration api.",
    )


# Creation and update classes (Those used on writer endpoints)


class TextField(BaseModel):
    body: str = Field(
        ...,
        description="""The text body. The format of the text should be specified in the format field.
The sum of all text fields in the request may not exceed 2MB.
If you need to store more text, consider using a file field instead or splitting into multiple requests for each text field.""",
        max_length=2 * MB,
    )
    format: TextFormat = Field(
        default=TextFormat.PLAIN,
        description="The format of the text.",
    )
    extract_strategy: Optional[str] = Field(
        default=None,
        description="Id of the Nuclia extract strategy to use at processing time. If not set, the default strategy will be used. Extract strategies are defined at the learning configuration api.",
    )
    split_strategy: Optional[str] = Field(
        default=None,
        description="Id of the Nuclia split strategy used at processing time. If not set, the default strategy was used. Split strategies are defined at the learning configuration api.",
    )

    @model_validator(mode="after")
    def check_text_format(self) -> Self:
        if self.format == TextFormat.JSON:
            validate_json(self.body or "")
        return self
