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

import json
from datetime import datetime
from typing import Union

import pydantic
from pydantic import BeforeValidator
from typing_extensions import Annotated


def validate_field_id(value, handler, info):
    try:
        return handler(value)
    except pydantic.ValidationError as e:
        if any(x["type"] == "string_pattern_mismatch" for x in e.errors()):
            raise ValueError(
                f"Invalid field_id: '{value}'. Field ID must be a string with only "
                "letters, numbers, underscores, colons and dashes."
            )
        else:
            raise e


FieldIdPattern = r"^[a-zA-Z0-9:_-]+$"


FieldIdString = Annotated[
    str,
    pydantic.StringConstraints(pattern=FieldIdPattern),
    pydantic.WrapValidator(validate_field_id),
]


def validate_slug(value, handler, info):
    try:
        return handler(value)
    except pydantic.ValidationError as e:
        if any(x["type"] == "string_pattern_mismatch" for x in e.errors()):
            raise ValueError(
                f"Invalid slug: '{value}'. Slug must be a string with only "
                "letters, numbers, underscores, colons and dashes."
            )
        else:
            raise e


SlugString = Annotated[
    str,
    pydantic.StringConstraints(
        pattern=r"^[a-zA-Z0-9:_-]+$",
        min_length=1,
        max_length=250,
    ),
    pydantic.WrapValidator(validate_slug),
]


def validate_json(value: str):
    try:
        json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError("Invalid JSON") from exc


def check_valid_datetime(v: Union[str, datetime]) -> datetime:
    if isinstance(v, datetime):
        return v
    try:
        return datetime.fromisoformat(v)
    except ValueError as e:
        raise ValueError(f"Invalid datetime: {e}")


DateTime = Annotated[datetime, BeforeValidator(check_valid_datetime)]
