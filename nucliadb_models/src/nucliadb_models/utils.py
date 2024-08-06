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
    pydantic.StringConstraints(pattern=r"^[a-zA-Z0-9:_-]+$"),
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
