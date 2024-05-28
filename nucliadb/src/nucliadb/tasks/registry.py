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
#
import functools
from typing import Optional

import pydantic

from nucliadb.tasks.models import RegisteredTask
from nucliadb_utils import const

REGISTRY: dict[str, RegisteredTask] = dict()


def register_task(
    name: str,
    stream: const.Streams,
    msg_type: pydantic.BaseModel,
    max_concurrent_messages: Optional[int] = None,
):
    """
    Decorator to register a task in the registry.
    """

    def decorator_register(func):
        REGISTRY[name] = RegisteredTask(
            callback=func,
            stream=stream,
            msg_type=msg_type,
            max_concurrent_messages=max_concurrent_messages,
        )

        @functools.wraps(func)
        async def wrapper_register(*args, **kwargs):
            return await func(*args, **kwargs)

        return wrapper_register

    return decorator_register


def get_registered_task(name: str) -> RegisteredTask:
    return REGISTRY[name]
